# This source code is Copyright (c) 2023 Georgia Tech Research Corporation. All
# Rights Reserved. Permission to copy, modify, and distribute this software and
# its documentation for academic research and education purposes, without fee,
# and without a written agreement is hereby granted, provided that the above
# copyright notice, this paragraph and the following three paragraphs appear in
# all copies. Permission to make use of this software for other than academic
# research and education purposes may be obtained by contacting:
#
#  Office of Technology Licensing
#  Georgia Institute of Technology
#  926 Dalney Street, NW
#  Atlanta, GA 30318
#  404.385.8066
#  techlicensing@gtrc.gatech.edu
#
# This software program and documentation are copyrighted by Georgia Tech
# Research Corporation (GTRC). The software program and documentation are 
# supplied "as is", without any accompanying services from GTRC. GTRC does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL GEORGIA TECH RESEARCH CORPORATION BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF GEORGIA TECH RESEARCH CORPORATION HAS BEEN ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE. GEORGIA TECH RESEARCH CORPORATION SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN "AS IS" BASIS, AND  GEORGIA TECH RESEARCH CORPORATION HAS
# NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.
#
# This source code is part of the GRIP software. The original GRIP software is
# Copyright (c) 2015 The Regents of the University of California. All rights
# reserved. Permission to copy, modify, and distribute this software for
# academic research and education purposes is subject to the conditions and
# copyright notices in the source code files and in the included LICENSE file.

import logging, json, argparse, time
import pprint

from grip.common import ES_CONFIG_LOCATION, RPKI_DATA_DIR, IRR_DATA_DIR, \
        ASRANK_DATA_DIR, HEGEMONY_DATA_DIR
from grip.tagger.common import REDIS_AVAIL_SECONDS

from grip.utils.kafka import KafkaHelper
from grip.utils.messages import EventOnElasticMsg
from grip.events.event import Event
from grip.events.pfxevent import PfxEvent
from grip.utils.data.elastic import ElasticConn
from grip.utils.data.elastic_queries import query_in_range

from grip.tagger.tagger_defcon import DefconTagger
from grip.tagger.tagger_edges import EdgesTagger
from grip.tagger.tagger_moas import MoasTagger
from grip.tagger.tagger_submoas import SubMoasTagger

from grip.inference.inference_collector import InferenceCollector

TEST_INDEX_NAME_PATTERN = 'retagged-v4-events-{}-{}-{}'

def month_index(value):
    if value not in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
        raise argparse.ArgumentTypeError(f"{value} is not a valid month index")
    return value

TAGGERS = {
    "defcon": DefconTagger,
    "edges": EdgesTagger,
    "moas": MoasTagger,
    "submoas": SubMoasTagger,
}


class GripRetagger(object):
    def __init__(self, options):
        self.elastic_conf_loc = options.elastic_conf
        self.write_prod = options.write_prod
        self.write_stdout = options.write_stdout
        self.write_test = options.write_testindex
        self.event_type = options.type
        self.esconn = ElasticConn(conffile=self.elastic_conf_loc,
                debug=False)
        self.query = None

        if len([x for x in [options.start_ts, options.end_ts] if x]) == 1:
                raise ValueError('You have to provide both start date and end date.')
        
        if options.start_ts:
                self.start_ts, self.end_ts = options.start_ts, options.end_ts
                self.query = query_in_range(self.start_ts, self.end_ts, size=10)
                self.index_name_pattern = self.esconn.get_index_name(self.event_type)
        elif  options.month:
                try:
                        reqdate = options.month
                        self.month, self.year = reqdate.split("-")
                        if len(self.month) != 2 or \
                                not self.month.isdigit() or \
                                not (1 <= int(self.month) <= 12) or \
                                len(self.year) != 4 or \
                                not self.year.isdigit():
                                raise ValueError(f"Invalid format for month: {reqdate}")   

                        self.index_name_pattern = self.esconn.get_index_name(self.event_type, self.year, self.month)                            
                except ValueError as e:
                        raise
        else:
                raise ValueError('You have to give either specific month or ts range')
        
        if not self.write_test and not self.write_stdout and \
                not self.write_prod:
            logging.warn("No output mode has been defined, so we're not going to generate output!")


        tagger_opts = vars(options)
        tagger_opts["redis_cluster"] = True
        tagger_opts["no_view_metrics"] = True
        tagger_opts["historic_mode"] = True

        self.tagger = TAGGERS[self.event_type](tagger_opts)
        self.inferencer = InferenceCollector(event_type = self.event_type,
                debug=False, esconf=self.elastic_conf_loc)

    def _get_output_index_name(self, event_id):
        index_name = self.esconn.infer_index_name_by_id(event_id)
        if self.write_prod:
            return index_name
        return index_name.replace('observatory', 'retagged')


    def run(self):
        last_ts = 0
        warned = False
        for ev in self.esconn.search_generator(index=self.index_name_pattern, query=self.query):
            ts = ev.view_ts
            if ts < last_ts:
               if not warned:
                   logging.warn("events are not being received from ES in time order?")
                   warned = True
               continue
            if ts != last_ts:
                if ts < time.time() - REDIS_AVAIL_SECONDS:
                    self.tagger.in_memory=True
                self.tagger.update_datasets(ts)
                self.tagger.methodology.prepare_for_view(ts)

                last_ts = ts

            self.tagger.retag_event(ev)
            ev.summary.clear_inference()
            self.inferencer.infer_event(event=ev)

            if self.write_prod or self.write_test:
                outindex = self._get_output_index_name(ev.event_id)

                self.esconn.index_event(ev, outindex, debug=False, update=True)

            if self.write_stdout:
                pprint.pprint(ev.as_dict())

def main():
    logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                        # filename=LOG_FILENAME,
                        level=logging.INFO)


    parser = argparse.ArgumentParser(
            description="Re-tag all GRIP events for a particular month")

    parser.add_argument("-t", "--type", required=True,
            choices=["defcon", "edges", "moas", "submoas"],
            help="Event type to re-tag")

    parser.add_argument("-s", "--start-ts", type=int, required=False,
                            help="First ts to check for events.")
    parser.add_argument("-e", "--end-ts", type=int, required=False, 
                            help="Last ts to check for events.")

    parser.add_argument("-m", "--month", required=False, type=str,
            help="The month to re-tag (must be in the format MM-YYYY)")
    parser.add_argument("--elastic-conf", required=False, type=str,
            help="Location of the elastic search configuration file",
            default=ES_CONFIG_LOCATION)

    parser.add_argument("-p", "--write-prod", action="store_true",
            default=False, help="Write re-tagged events to production indexes")
    parser.add_argument("-T", "--write-stdout", action="store_true",
            default=False, help="Write re-tagged events to standard output")
    parser.add_argument("-D", "--write-testindex", action="store_true",
            default=False, help="Write re-tagged events to test indexes")
    parser.add_argument('--asrank-api', action='store_true', default=False,
            required=False,
            help="Query ASRank API instead of CAIDA local datasets")
    parser.add_argument('--rpki-data-dir', default=RPKI_DATA_DIR,
            required=False,
            help="Location of local RPKI data files")
    parser.add_argument('--irr-data-dir', default=IRR_DATA_DIR, required=False,
            help="Location of local IRR data files")
    parser.add_argument('--asrank-data-dir', default=ASRANK_DATA_DIR,
            required=False,
            help="Location of local ASrank data files")
    parser.add_argument('--hegemony-data-dir', default=HEGEMONY_DATA_DIR,
            required=False,
            help="Location of local hegemony data files")
    parser.add_argument("--pfx-origins-data-dir", default=None, required=False,
            help="Location of pfx origins data files")
    parser.add_argument('--redis-user', default="default", type=str,
            help="The username to use for accessing redis")
    parser.add_argument('--redis-password', default="", type=str,
            help="The password to use for authenticating with redis")
    parser.add_argument('--redis-host', default="gaiola.cc.gatech.edu",
            type=str, help="The redis host to connect to")
    parser.add_argument('--redis-port', default="6379", type=str,
            help="The port on the redis host to connect to")

    parser.add_argument("-P", "--predetermined-tags", action="append", default=[], help="Predetermined tags to add to pfx events")

    opts, _ = parser.parse_known_args()


    retagger = GripRetagger(opts)
    retagger.run()
