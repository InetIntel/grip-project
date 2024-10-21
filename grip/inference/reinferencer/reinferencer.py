# This source code is Copyright (c) 2022 Georgia Tech Research Corporation. All
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

import argparse
import logging
from grip.utils.data.elastic import ElasticConn
from grip.common import ES_CONFIG_LOCATION
from grip.utils.data.elastic_queries import query_in_range
from grip.inference.inference_collector import InferenceCollector


class GripReinferencer:
    def __init__(self, options):
        if options is None:
            options = {}

        self.type = options['type']
        self.debug = options.get("debug", False)
        esconf = options.get("elastic_conf", ES_CONFIG_LOCATION)
        self.esconn = ElasticConn(conffile=esconf)
        
        esindex_read_prefix = options.get('es_index_in', None)
        self.esindex_read = self.esconn.get_index_name(self.type, debug=self.debug, prefix=esindex_read_prefix)

        self.esindex_write_prefix = options.get('es_index_out', None)
        self.output = options.get('output_stdio', None)

        self.inferencer = InferenceCollector(event_type = self.type,
                debug=False, esconf=esconf)
    
    def reindex_event(self, event):
        if self.output:
            pprint.pprint(event.as_dict())
        else:
            logging.info(f'Indexing event {event.event_id}.')
            self.esconn.index_event(event, debug=self.debug, prefix=self.esindex_write_prefix , update=True)

    def run(self, start_ts, end_ts):
        query = query_in_range(start_ts, end_ts)
        for ev in self.esconn.search_generator(index=self.esindex_read, query=query, timeout='30m'):
            ev.summary.clear_inference()
            self.inferencer.infer_event(event=ev)
            self.reindex_event(ev)

def main():
    parser = argparse.ArgumentParser(description="""
    BGP Hijacks Re-Inferencer runner
    """)
    parser.add_argument("-c", "--type", required=True,
                        choices=["moas", "submoas", "defcon", "edges"],
                        help="Inferencer type to run")

    parser.add_argument("-s", "--start_ts", required=True,
                        help="Unix timestamp of view to start processing from.",
                        type=int)
    parser.add_argument("-e", "--end_ts", required=True,
                        help="Unix timestamp of view to start processing from.",
                        type=int)

    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Whether to enable debug mode")

    parser.add_argument('-v', '--verbose', action="store_true",
                        required=False, help='Verbose logging')

    parser.add_argument("--elastic-conf", required=False, type=str,
                        help="Location of the elastic search configuration file",
                        default=ES_CONFIG_LOCATION)

    parser.add_argument("-i", "--es-index-in", type=str, default=None,
                        help="Index to read data from.")
    parser.add_argument("-o", "--es-index-out", type=str, default=None,
                        help="Index to write data to.")
    
    
    parser.add_argument("-f", "--output-stdio", action="store_true", default=False, 
                        help="Write re-inferenced events to standard output")
    
    opts, _ = parser.parse_known_args()

    logging.basicConfig(level="DEBUG" if opts.verbose else "INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    if opts.debug:
        logging.getLogger('elasticsearch').setLevel(logging.INFO)
    else:
        logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    reinferencer = GripReinferencer(options={
        "type": opts.type,
        "debug": opts.debug,
        "elastic_conf": opts.elastic_conf,
        "es_index_in": opts.es_index_in,
        "es_index_out": opts.es_index_out,
        "output_stdio": opts.output_stdio,
    })

    if opts.start_ts:
        reinferencer.run(start_ts=opts.start_ts, end_ts=opts.end_ts)

if __name__ == "__main__":
    main()
