#  This software is Copyright (c) 2015 The Regents of the University of
#  California. All Rights Reserved. Permission to copy, modify, and distribute this
#  software and its documentation for academic research and education purposes,
#  without fee, and without a written agreement is hereby granted, provided that
#  the above copyright notice, this paragraph and the following three paragraphs
#  appear in all copies. Permission to make use of this software for other than
#  academic research and education purposes may be obtained by contacting:
#
#  Office of Innovation and Commercialization
#  9500 Gilman Drive, Mail Code 0910
#  University of California
#  La Jolla, CA 92093-0910
#  (858) 534-5815
#  invent@ucsd.edu
#
#  This software program and documentation are copyrighted by The Regents of the
#  University of California. The software program and documentation are supplied
#  "as is", without any accompanying services from The Regents. The Regents does
#  not warrant that the operation of the program will be uninterrupted or
#  error-free. The end-user understands that the program was developed for research
#  purposes and is advised not to rely exclusively on the program for any reason.
#
#  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
#  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
#  PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
#  THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
#  DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
#  IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
#  MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
import json
import logging
import os
import re
import time
import elasticsearch

import wandio
from urllib3.exceptions import ProtocolError

import grip.common
import grip.coodinator.announce
from grip.events.event import Event
from grip.events.event_summary import EventSummary
from grip.events.pfxevent_parser import PfxEventParser
from grip.metrics.view_metrics import ViewMetrics
from grip.redis import Pfx2AsNewcomer, Adjacencies, Pfx2AsHistorical, Pfx2AsNewcomerLocal
from grip.tagger.cache_window import CacheWindow
from grip.tagger.finisher import Finisher
from grip.tagger.tags import tagshelper
from grip.utils.data.asrank import AsRankUtils
from grip.utils.data.siblings import Siblings
from grip.utils.data.asrank_local import AsRankLocal
from grip.utils.data.elastic import ElasticConn
from grip.utils.data.hegemony import HegemonyUtils
from grip.utils.data.ixpinfo import IXPInfo
from grip.utils.data.reserved_prefixes import ReservedPrefixes
from grip.utils.data.spamhaus import AsnDrop
from grip.utils.data.trusted_asns import TrustedAsns
from grip.utils.kafka import KafkaHelper
from grip.utils.messages import EventOnElasticMsg
from .methods import TaggingMethodology
from .tags.friends import OrgFriends
from ..utils.data.rpki import RpkiUtils
from grip.utils.data.irr import IRRUtils
from ..utils.fs import fs_get_timestamp_from_file_path, fs_generate_file_list

MAX_PFX_EVENTS_PER_EVENT_TO_TAG = {
    "edges": 1,  # there is little point checking the same new-edge for different prefixes
    "moas": 100,
    "submoas": 100,
    "defcon": 100,
}

def save_unrepeatable_tags(evtype, tags, details):

    unrepeatable = []
    if evtype == "moas":
        # transition requires keeping state across events
        unrepeatable += [
            "moas-potential-convergence",
            "moas-potential-transfer",
            "moas-transition"
        ]
        
    if evtype == "moas" and len(details.get_aspaths()) == 0:
        unrepeatable += [
            # require aspaths which are not saved in ES
            "all-newcomers-next-to-an-oldcomer",
            "oldcomers-always-on-newcomer-originated-paths",
            "newcomers-always-on-oldcomer-originated-paths",
            "oldcomer-path-prepending",
            "hegemony-valley-paths"
        ]

    if evtype == "edges":
        # hopefully we'll be able to retag most of these in the future,
        # but for now we don't really have the data in an accessible form
        unrepeatable += [
            "new-bidirectional",
            "adj-previously-observed-opposite",
            "adj-previously-observed-exact",
            "ixp-colocated",
            "new-edge-connected-to-Tier-1"
        ]

    instances = {}
    for x in unrepeatable:
        instances[x] = tagshelper.get_tag(x)

    saved = set()
    for t in tags:
        if t.name in instances:
            saved.add(instances[t.name])

    return saved


class Tagger(object):
    DEBUG = False

    def __init__(self, name, file_regex, options):
        """
        initialization of the tagging class object
        """

        # process options
        if options is None:
            options = {}

        self.DEBUG = options.get("debug", False)

        self.redis_host=options.get("redis_host", "procida.cc.gatech.edu")
        self.redis_port=str(options.get("redis_port", 6379))
        self.redis_user=options.get("redis_user", "default")
        self.redis_password=options.get("redis_password", None)
        self.redis_cluster=options.get("redis_cluster", True)
        self.elastic_conf_loc = options.get("elastic_conf",
                grip.common.ES_CONFIG_LOCATION)

        self.force_process_view = options.get("force_process_view", False)
        self.produce_kafka_message = options.get("produce_kafka_message", True)
        self.offsite_mode = options.get("offsite_mode", False)
        self.in_memory = options.get("in_memory_data", self.offsite_mode)  # if offsite mode then must in memory

        pfx2as_datafile = options.get("pfx2as_file", None)
        pfx2as_path = options.get("pfx_origins_path", None)
        self.output_file = options.get("output_file", None)
        self.rpki_data_dir = options.get("rpki_data_dir", grip.common.RPKI_DATA_DIR)
        self.irr_data_dir = options.get("irr_data_dir", grip.common.IRR_DATA_DIR)
        self.asrank_data_dir = options.get("asrank_data_dir", grip.common.ASRANK_DATA_DIR)
        self.siblings_data_dir = options.get("siblings_data_fir", grip.common.SIBLINGS_DATA_DIR)
        self.hegemony_data_dir = options.get("hegemony_data_dir", grip.common.HEGEMONY_DATA_DIR)
        self.tags = options.get("predetermined_tags", [])
        self.no_view_metrics = options.get("no_view_metrics", False)
        self.historic_mode = options.get("historic_mode", False)

        self.name = name  # type of tagger: moas, submoas, defcon, edges
        self.consumer_filename_regex = file_regex  # regex to parse consumer files

        if self.historic_mode:
            finisher_pfxs = "/data/bgp/historical/pfx-origins"
        else:
            finisher_pfxs = "/data/bgp/live/pfx-origins/production"
        self.finisher = Finisher(event_type=name, load_unfinished=options.get("load_unfinished", True), debug=self.DEBUG,\
                                pfx_datadir=finisher_pfxs,
                                esconf=self.elastic_conf_loc) if options.get("enable_finisher", False) else None
        # datasets and methodology
        self.datasets = {
            # production site datasets
            # "ixp_info": IXPInfo() if not self.offsite_mode else None,
            "ixp_info": None,
            "adjacencies": Adjacencies() if not self.offsite_mode else None,
            "pfx2asn_newcomer": Pfx2AsNewcomer(host=self.redis_host, port=self.redis_port, db=1, password=self.redis_password, cluster_mode=self.redis_cluster, user=self.redis_user) if not self.offsite_mode else None,
            "pfx2asn_historical": Pfx2AsHistorical(host=self.redis_host, port=self.redis_port, db=0, password=self.redis_password, cluster_mode=self.redis_cluster, user=self.redis_user) if not self.offsite_mode else None,
            "asndrop": AsnDrop(esconf=self.elastic_conf_loc) if not self.offsite_mode else None,
            # globally available datasets
            "pfx2asn_newcomer_local": Pfx2AsNewcomerLocal(live_datapath=pfx2as_path, datafile=pfx2as_datafile, never_update_files=self.historic_mode),
            "rpki": RpkiUtils(self.rpki_data_dir, never_update_files=self.historic_mode),
            "irr": IRRUtils(self.irr_data_dir, never_update_files=self.historic_mode),
            "as_rank": AsRankLocal(self.asrank_data_dir, never_update_files=self.historic_mode) if not options.get('asrank_api', False) else AsRankUtils(),
            "siblings": Siblings(self.siblings_data_dir, never_update_files=self.historic_mode),            
            "hegemony": HegemonyUtils(self.hegemony_data_dir, never_update_files=self.historic_mode),
            "trust_asns": TrustedAsns(),
            "friend_asns": OrgFriends(),
            "reserved_pfxs": ReservedPrefixes(),
        }

        self.methodology = TaggingMethodology(datasets=self.datasets)
        self.window = CacheWindow()

        # data utilities
        if not self.offsite_mode:
            # elasticsearch
            self.es_conn = ElasticConn(conffile=self.elastic_conf_loc,
                    debug=self.DEBUG)
            # kafka
            kafka_template = grip.common.KAFKA_DEBUG_TOPIC_TEMPLATE if self.DEBUG \
                else grip.common.KAFKA_TOPIC_TEMPLATE
            self.kafka_producer_topic = kafka_template % ("tagger", name)
            self.kafka = KafkaHelper()
            self.kafka.init_producer(topic=self.kafka_producer_topic)

        # time tracking
        self.start_time = None
        self.current_ts = None

    def update_datasets(self, ts, consumer_filename=None):
        """
        Update datasets used by taggers
        """

        if self.current_ts and self.current_ts == ts:
            return

        logging.info("updating datasets...")
        
        common_update_functions = {"asndrop", "ixp_info", "rpki", "irr", "as_rank", "siblings", "hegemony"}
        for dsname in self.datasets:
            if not self.datasets[dsname]:
                continue
            elif dsname in common_update_functions:
                logging.info(f'updating dataset: {dsname}')
                self.datasets[dsname].update_ts(ts)
            elif dsname == "pfx2asn_newcomer_local" and self.in_memory:
                logging.info(f'updating dataset: pfx2asn_newcomer_local')
                self.datasets["pfx2asn_newcomer_local"].check_and_load_data_from_timestamp(ts)
        logging.info("updating datasets complete")

        self.current_ts = ts

    def parse_timestamp(self, consumer_filename):
        """
        parse incoming kafka message from the consumer and extract filename and time stamp from the message.

        :param consumer_filename: the filename passed in from consumer kafka
        """
        match = re.match(self.consumer_filename_regex, os.path.basename(consumer_filename))
        if match is None:
            raise ValueError("Invalid %s file: %s" % (self.name, consumer_filename))
        ts = int(match.group(1))
        return ts

    def tag_pfxevent(self, pfxevent):
        raise NotImplementedError

    def _parse_consumer_file_for_pfx_events(self, event_type, consumer_filename, view_metrics=None, is_caching=False,
                                            check_recurring=True):
        log_prefix = ""
        if is_caching:
            log_prefix = "caching: "
        logging.info("{}parsing consumer file to extract prefix events: {}".format(log_prefix, consumer_filename))
        parser = PfxEventParser(event_type, is_caching)
        pfx_events = []
        all_cnt, new_cnt, fin_cnt, skip_cnt, recur_cnt = 0, 0, 0, 0, 0
        try:
            for line in wandio.open(consumer_filename):
                # ignore commented lines
                if line.startswith("#"):
                    continue

                try:
                    pfx_event = parser.parse_line(line)
                except ValueError as e:
                    # handling event parsing error here
                    logging.error("parse pfx_event failed: %s" % e)
                    skip_cnt += 1
                    continue

                all_cnt += 1

                if pfx_event is None:
                    skip_cnt += 1
                    continue

                if pfx_event.position == "NEW":
                    if check_recurring and self.window.is_old_event_and_update(pfx_event):
                        # skipping recurring events
                        recur_cnt += 1
                        continue
                    pfx_events.append(pfx_event)
                    new_cnt += 1
                    if self.DEBUG:
                        logging.debug("NEW: %s %s", pfx_event.details.get_prefix_of_interest(),
                                      [tag.name for tag in pfx_event.tags])
                elif pfx_event.position == "FINISHED":
                    pfx_events.append(pfx_event)
                    fin_cnt += 1
                    if self.DEBUG:
                        logging.debug("FIN: %s", pfx_event.details.get_prefix_of_interest())
        except ProtocolError as e:
            # handle connection broken more gracefully
            # TODO: find out what causes the connection broken error
            logging.error('Connection broken: %r' % e)

        if view_metrics:
            view_metrics.consumer_events_cnt = all_cnt
            view_metrics.consumer_new_events_cnt = new_cnt
            view_metrics.consumer_fin_events_cnt = fin_cnt
            view_metrics.consumer_skip_events_cnt = skip_cnt
            view_metrics.consumer_recur_events_cnt = recur_cnt
        return pfx_events

    def retag_event(self, event:Event):

        event.summary = EventSummary(event)
        if len(event.pfx_events) > MAX_PFX_EVENTS_PER_EVENT_TO_TAG[self.name]:
            logging.info("too many prefix events to tag for {}: total - {}, limit - {}".format(
                event.event_id, len(event.pfx_events), MAX_PFX_EVENTS_PER_EVENT_TO_TAG[self.name]
            ))

        TagSkippedPfxEvent = tagshelper.get_tag('skipped-pfx-event')
        pfx_event_cnt = 0
        for pfx_event in event.pfx_events:
            pfx_event.tags = save_unrepeatable_tags(self.name, pfx_event.tags,
                    pfx_event.details)
            pfx_event_cnt += 1
            pfx_event.add_tags(self.tags)
            # check if reaching the tagging count limit for this event for this event
            if pfx_event_cnt > MAX_PFX_EVENTS_PER_EVENT_TO_TAG[self.name]:
                pfx_event.add_tags([TagSkippedPfxEvent])
                continue

            # main (per prefix-event) tagging function
            self.tag_pfxevent(pfx_event)

        event.summary.update()

    def tag_event(self, event: Event, check_recurring=True):
        """
        Given an Event object with a number of prefix events associated, tag prefix events within. The number of prefix
        events are limited by MAX_PFX_EVENTS_PER_EVENT_TO_TAG.

        :param check_recurring:
        :param event: Event object
        :return a boolean whether the event in question is a recurring event.
        """
        assert event.position == "NEW"
        if len(event.pfx_events) > MAX_PFX_EVENTS_PER_EVENT_TO_TAG[self.name]:
            logging.info("too many prefix events to tag for {}: total - {}, limit - {}".format(
                event.event_id, len(event.pfx_events), MAX_PFX_EVENTS_PER_EVENT_TO_TAG[self.name]
            ))

        TagSkippedPfxEvent = tagshelper.get_tag('skipped-pfx-event')
        is_recurring = False
        pfx_event_cnt = 0
        for pfx_event in event.pfx_events:
            pfx_event_cnt += 1
            pfx_event.add_tags(self.tags)
            # check if reaching the tagging count limit for this event for this event
            if pfx_event_cnt > MAX_PFX_EVENTS_PER_EVENT_TO_TAG[self.name]:
                pfx_event.add_tags([TagSkippedPfxEvent])
                continue

            # main (per prefix-event) tagging function
            self.tag_pfxevent(pfx_event)

        event.summary.update()
        return is_recurring
    
    def _add_external_data(self, event, to_query_asrank=True, to_query_hegemony=True):
        """
        Query data sources and put the data into the event object
        :param event:
        :param to_query_asrank:
        :param to_query_hegemony:
        :return:
        """
        asns = event.summary.ases
        if to_query_asrank:
            # filter only asns that need query:
            # 1. we don't have any info for the asn in event.asinfo
            # 2. we have some info for the asn, but not AS rank
            to_query_asns = [asn for asn in asns if asn not in event.asinfo or "asrank" not in event.asinfo[asn]]
            if to_query_asns:
                asrank_res = self.datasets['as_rank'].get_asrank_for_asns(to_query_asns)
                for asn in asrank_res:
                    event.add_to_asinfo(asn, "asrank", asrank_res[asn])
        if to_query_hegemony:
            # see comments for asrank section above.
            to_query_asns = [asn for asn in asns if asn not in event.asinfo or "hegemony" not in event.asinfo[asn]]
            if to_query_asns:
                hegemony_res = self.datasets['hegemony'].query_hegemony(
                    subgraph_asn_lst=["0"], asn_lst=to_query_asns)["0"]
                for asn in hegemony_res:
                    event.add_to_asinfo(asn, "hegemony", hegemony_res[asn])           

    def _produce_event(self, event, output_fh=None):
        """
        Output single event to file and produce the event to kafka for the components in the pipeline to consumer

        :param event: the event object to produce
        """

        if event.position == "FINISHED":
            if self.finisher is not None:
                self.finisher.process_finished_event(event=event)
            # for finished events, we do not propagate further to the pipeline, nor do we commit it to elasticsearch
            return

        # processing of new event
        index_name = self.es_conn.infer_index_name_by_id(event_id=event.event_id, debug=self.DEBUG)
        if self.finisher is not None:
            event_finisher_start_time = time.time()
            self.finisher.process_new_event(event, index_name)
            event.event_metrics.proc_time_tagger += time.time() - event_finisher_start_time
        
        # if the event has the predetermined tag 'missed-low-duration' it means that it lasted less than
        # 5 min, so we set the finished ts for the event and the included pfx_events to the next timebin
        if event.summary.has_tag('missed-low-duration'):
            for pfx_event in event.pfx_events:
                pfx_event.finished_ts = pfx_event.view_ts + 300
            event.finished_ts = event.view_ts + 300

        try:
            succeeded = self.es_conn.index_event(index=index_name, event=event)
        except elasticsearch.exceptions.RequestError as e:
            logging.error("ES request error: %s" % (str(e.info['error'])))
            raise

        if not succeeded:
            return

        if not self.produce_kafka_message:
            return

        kafka_msg = EventOnElasticMsg(
            sender="tagger",
            es_index=index_name,
            es_id=event.event_id,
            tr_worthy=event.summary.tr_worthy,
        )
        self.kafka.produce(kafka_msg.to_str(), topic=self.kafka_producer_topic, flush=True)
        if output_fh is not None:
            output_fh.write((event.as_json() + "\n").encode())

    def _dump_events(self, new_events, finished_event):
        """
        Output all events extracted in one consumer file to disk and kafka
        """
        assert isinstance(new_events, list)

        # update event summaries
        # process all new events
        for event in new_events:
            event.summary.update()
            self._produce_event(event)

        # process the only one finished event
        if finished_event is not None:
            self._produce_event(finished_event)

        if self.produce_kafka_message:
            self.kafka.flush()

    def cache_consumer_file(self, consumer_filename):
        # set is_caching=True to avoid saving aspaths for submoas and defcon
        pfx_events = self._parse_consumer_file_for_pfx_events(self.name, consumer_filename, None, is_caching=True)
        for pfx_event in pfx_events:
            if not pfx_event.position == "NEW":
                continue
            # cache the pfx_event
            self.window.is_old_event_and_update(pfx_event, show_warning=False)

    def cache_period(self, start_ts=None):
        if not start_ts:
            start_ts = int(time.time())
        cache_files = []
        logging.info("looking for consumer files to cache...")
        for file_name in fs_generate_file_list("/data/bgp/live/{}".format(self.name)):
            ts = fs_get_timestamp_from_file_path(file_name)

            if ts < start_ts and ts >= start_ts - self.window.window_size:
                cache_files.append(file_name)
                
        logging.info("caching total of %d consumer files" % len(cache_files))
        for fn in cache_files:
            self.cache_consumer_file(fn)

    def process_consumer_file(self, consumer_filename, cache_files=False):
        """
        Entry point function for the tagging process. it takes a consumer output file from disk, extracts events,
        and writes the events to Elasticsearch and propagates events down the pipeline using Kafka

        :param consumer_filename: the consumer file from disk
        :param cache_files: whether to cache consumer files, default is False. Set to True if run manually
        :return:
        """
        ts = self.parse_timestamp(consumer_filename)

        if cache_files:
            self.cache_period(ts)

        ####
        # skip views if already processed
        ####

        if not self.offsite_mode and not self.force_process_view and \
                self.es_conn.view_metrics_exist(view_ts=ts, event_type=self.name, debug=self.DEBUG):
            logging.info("view already processed, skipping {}".format(consumer_filename))
            return

        ####
        # initialize tagging
        ####

        self.start_time = time.time()
        view_metrics = ViewMetrics(view_ts=ts, event_type=self.name, consumer_file_path=consumer_filename)

        ####
        # Read prefix events from consumer output file
        #### 

        # The consumer output file contains prefix events, untagged
        # TODO: discard pfx events here?
        pfx_events = self._parse_consumer_file_for_pfx_events(self.name, consumer_filename, view_metrics)

        ####
        # Build events from prefix events
        #### 

        # define temporary storage of new events and finished event
        new_events = {}
        finished_event = None
        count_32 = 0

        for pfx_event in pfx_events:
            # if it is a FINISHED event, add to the finished_event object
            if pfx_event.position == "FINISHED":
                if finished_event is None:
                    finished_event = Event.from_pfxevent(pfx_event)
                finished_event.add_pfx_event(pfx_event)
                continue

            # add this prefix event to the appropriate high-level event
            event_id = pfx_event.get_event_id()
            if event_id not in new_events:
                if self.name == "submoas" and \
                        pfx_event.details.get_prefix_of_interest().endswith('/32') and\
                        len(pfx_event.details.get_sub_aspaths()) == 1 and\
                        pfx_event.details.get_sub_aspaths()[0][0] == '211398':
                            count_32 += 1
                            if count_32 > 50:
                                continue

                new_events[event_id] = Event.from_pfxevent(pfx_event)
            event = new_events[event_id]
            event.add_pfx_event(pfx_event)

        total_pfx_events_to_tag = sum(
            [len(event.pfx_events[:MAX_PFX_EVENTS_PER_EVENT_TO_TAG[self.name]]) for event in new_events.values()])
        unfinished_pfx_events = [e for e in pfx_events if e.position != "FINISHED"]
        logging.info("view {} has {} new prefix events for {} events, tagging {} pfx events"
                     .format(ts, len(unfinished_pfx_events), len(new_events), total_pfx_events_to_tag))

        ####
        # Tagging
        ####

        # Init
        self.update_datasets(ts, consumer_filename)  # NOTE: only edges run special function to update dataset
        self.methodology.prepare_for_view(ts)
        # Actual tagging loop

        non_recurring_events = []
        for event in new_events.values(): 
            # update tagger time per event
            event_start_time = time.time()
            is_recurring = self.tag_event(event)
            event.event_metrics.proc_time_tagger = time.time() - event_start_time
            self._add_external_data(event)

            if not is_recurring:
                non_recurring_events.append(event)

        logging.info("tagging finished")

        ####
        # output events to ElasticSearch and send Kafka messages to the downstream receivers (active driver, inference)
        ####

        # output events to ElasticSearch and Kafka
        if not self.offsite_mode:
            self._dump_events(non_recurring_events, finished_event)  # output events
            # check transitions if any left over exist, due to missing pfx-origins data
            if self.finisher:
                self.finisher.recheck_transition_events()
            # update metrics for this view
            if not self.no_view_metrics:
                view_metrics.update_proc_time(self.start_time, time.time())
                self.es_conn.index_view_metrics(view_metrics, debug=self.DEBUG)
        elif self.output_file:
            logging.info("writing tagged events to file: {}".format(self.output_file))
            with wandio.open(self.output_file, "w") as of:
                json.dump([e.as_dict() for e in new_events.values()], of, indent=4)

        logging.info("Done processing %s data", self.name)

    def listen(self, group, offset, cache_files=False):
        """
        The listener function that listens to the kafka message from consumer for new incoming available consumer files.

        :param cache_files: whether to first cache consumer data for the past 24 hours in order to capture duplicate events
        :param group: kafka group
        :param offset: kafka offset
        """
        listener = grip.coodinator.announce.Listener(
            offset=offset,
            group=group,
            sender_type="consumer",
            sender_name=self.name,
            auto_commit=True,  # manually commit offset after callback is finished
        )

        for in_ann in listener.listen():
            self.process_consumer_file(in_ann.path, cache_files=cache_files)
            if not listener.auto_commit:
                # if the listener is not set to autocommit when retrieving data from kakfa, we should commit the offset
                # manually here as shown below. if the autocommit is set to be True, then no need to call commit_offset()
                listener.commit_offset()
            cache_files = False  # only cache consumer files once
