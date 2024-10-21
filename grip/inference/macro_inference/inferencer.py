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

import grip.events.event
import grip.events.pfxevent
from grip.inference.inference import Inference
from grip.utils.data.elastic import ElasticConn
from grip.utils.data.elastic_queries import query_in_range, query_attackers, query_inferences, query_victims
from grip.common import ES_CONFIG_LOCATION
from bisect import bisect_right
from itertools import groupby
import pprint
from grip.tagger.methods import asn_should_keep
import logging

# TIME_WINDOW = 21600 # 6 hours
# MAX_EVENTS = 50

TIME_WINDOW = 900 # 15 minutes
MAX_EVENTS = 2

LABEL_ABNORMAL = "abnormal"
LABEL_TRACEROUTE = "traceroute"
LABEL_MISCONFIG = "misconfig"

# to remove them
DEFAULT_WORTHY_INFERENCE = Inference(
    inference_id="default-tr-worthy",
    explanation="no other inferences found, event is traceroute worthy",
    suspicion_level=80,
    confidence=50,
    labels=[LABEL_TRACEROUTE]
)

DEFAULT_NOT_WORTHY_INFERENCE = Inference(
    inference_id="default-not-tr-worthy",
    explanation="no other inferences found, event is not traceroute worthy",
    suspicion_level=20,
    confidence=50,
    labels=[LABEL_TRACEROUTE]
)

ABNORMAL_ATTACKER = Inference(
    inference_id="abnormal-attacker",
    explanation="this is part of a macro-event containing many events \
                caused by the same attacker",
    suspicion_level=60,
    confidence=98,
    labels=[LABEL_ABNORMAL]
)

ABNORMAL_MISCONFIG = Inference(
    inference_id="abnormal-victim",
    explanation="this is part of a macro-event containing many events \
                caused by the same victim",
    suspicion_level=20,
    confidence=98,
    labels=[LABEL_ABNORMAL]
)

ABNORMAL_PEER = Inference(
    inference_id="abnormal-peer",
    explanation="this is part of a macro-event containing many events \
                where attacker paths are visible by only one peer",
    suspicion_level=60,
    confidence=98,
    labels=[LABEL_ABNORMAL]
)

class MacroInferencer:
    """
    Retrieve events of a specific view and check for abnormal behavior
    """

    def __init__(self, type, options):
        if options is None:
            options = {}

        self.type = type
        self.debug = options.get("debug", False)
        esconf = options.get("elastic_conf", ES_CONFIG_LOCATION)
        self.esconn = ElasticConn(conffile=esconf)
        
        esindex_read_prefix = options.get('es_index_in', None)
        self.esindex_read = self.esconn.get_index_name(self.type, debug=self.debug, prefix=esindex_read_prefix)

        self.esindex_write_prefix = options.get('es_index_out', None)
        self.output = options.get('output_stdio', None)

        # cache
        self.cache = dict()
        self.cache_ts = []

        self.check_functions = {
            'attackers': self.check_attackers,
            'peers': self.check_peers,
            'misconfigurers': self.check_misconfig
        }

    def get_interesting_paths(self, pfxevent):
        raise NotImplementedError

    def get_peer_on_attackers_path(self, attackers: set, paths: list):
        peer = None
        for path in paths:
            if path[-1] in attackers:
                peer = path[0]
                break
        return peer



    """
    # Alternative: in case we want to treat attackers individually
    def update_cache(self, query):
        cur_ts = None
        added_ts = set()
        for ev in self.esconn.search_generator(index=self.esindex_read, query=query, 
                                               timeout='30m'):
            if ev.view_ts != cur_ts:
                cur_ts = ev.view_ts
                added_ts.add(cur_ts)
                self.cache[cur_ts] = {
                    'attackers': dict(),
                    'peers': dict(),
                }
                self.cache_ts.append(cur_ts)


            # attacker logic
            attackers = ev.summary.attackers
            # attackers = [attacker for attacker in attackers if asn_should_keep(attacker)]
            for attacker in attackers:
                if attacker not in self.cache[cur_ts]['attackers']:
                    self.cache[cur_ts]['attackers'][attacker] = 0
                self.cache[cur_ts]['attackers'][attacker] += 1
            

            added_peers = set()
            for pfx_event .pfx_events:
                # peer logic
                # check all pfx_events b/c maybe 2 pfx_events have different single peers
                # check if it has the single-peer-on-attacker-path tag
                if pfx_event.has_tag('single-peer-on-attacker-path'):
                    paths = self.get_interesting_paths(pfx_event)
                    peer = self.get_peer_on_attackers_path(set(attackers), paths)
                    if peer and peer not in added_peers: # count only once per event
                        added_peers.add(peer) 
                        if peer not in self.cache[cur_ts]['peers']:
                            self.cache[cur_ts]['peers'][peer] = 0
                        self.cache[cur_ts]['peers'][peer] += 1

        # update total
        if 'total' not in self.cache:
            self.cache['total'] = {
                'attackers': dict(),
                'peers': dict(),
            }

        for view_ts in added_ts:
            view_dict = self.cache[view_ts]
            for type_name, type_dict in view_dict.items():
                for asn, counter in type_dict.items():
                    if asn not in self.cache['total'][type_name]:
                        self.cache['total'][type_name][asn] = [0, None]
                    self.cache['total'][type_name][asn][0] += counter
    """


    def update_cache(self, query):
        added_ts = set()
        cur_ts = None
        for ev in self.esconn.search_generator(index=self.esindex_read, query=query, 
                                               timeout='30m'):
            if ev.view_ts != cur_ts:
                cur_ts = ev.view_ts
                added_ts.add(cur_ts)
                logging.info(f'Adding view {cur_ts} in cache.')
                self.cache[cur_ts] = {
                    'attackers': dict(),
                    'peers': dict(),
                    'misconfigurers': dict()
                }
                self.cache_ts.append(cur_ts)

            added_attackers = set()
            added_peers = set()
            added_misconfigs = set()
            
            for pfx_event in ev.pfx_events:
                # victims for misconfigs; attackers otherwise
                if any([LABEL_MISCONFIG in inf.labels for inf in pfx_event.inferences]):
                    victims = pfx_event.details.extract_attackers_victims()[1]
                    if not victims:
                        continue
                    victims_str = '_'.join((sorted(victims)))

                    if victims_str not in added_misconfigs: # count only once per event
                        added_misconfigs.add(victims_str)
                        if victims_str not in self.cache[cur_ts]['misconfigurers']:
                            self.cache[cur_ts]['misconfigurers'][victims_str] = 0
                        self.cache[cur_ts]['misconfigurers'][victims_str] += 1
                else:
                    attackers = pfx_event.details.extract_attackers_victims()[0]
                    # attackers = [attacker for attacker in attackers if asn_should_keep(attacker)]
                    if not attackers:
                        continue
                    # attacker logic
                    # treat attackers as groups so that malicious actors don't hide
                    # in abnormal incidents
                    attackers_str = '_'.join((sorted(attackers)))
                    if attackers_str not in added_attackers: # count only once per event
                        added_attackers.add(attackers_str)
                        if attackers_str not in self.cache[cur_ts]['attackers']:
                            self.cache[cur_ts]['attackers'][attackers_str] = 0
                        self.cache[cur_ts]['attackers'][attackers_str] += 1
                    
                # peer logic
                # check all pfx_events b/c maybe 2 pfx_events have different single peers
                # check if it has the single-peer-on-attacker-path tag
                if pfx_event.has_tag('single-peer-on-attacker-path'):
                    paths = self.get_interesting_paths(pfx_event)
                    peer = self.get_peer_on_attackers_path(set(attackers), paths)
                    if peer and peer not in added_peers: # count only once per event
                        added_peers.add(peer) 
                        if peer not in self.cache[cur_ts]['peers']:
                            self.cache[cur_ts]['peers'][peer] = 0
                        self.cache[cur_ts]['peers'][peer] += 1

        # update total
        if 'total' not in self.cache:
            self.cache['total'] = {
                'attackers': dict(),
                'peers': dict(),
                'misconfigurers': dict()
            }

        for view_ts in added_ts:
            view_dict = self.cache[view_ts]
            for type_name, type_dict in view_dict.items():
                for asn, counter in type_dict.items():
                    if asn not in self.cache['total'][type_name]:
                        self.cache['total'][type_name][asn] = [0, None]
                    self.cache['total'][type_name][asn][0] += counter


    def load_cache(self, until_ts):
        logging.info(f'Loading cache until view {until_ts}.')
        query = query_in_range(until_ts - TIME_WINDOW + 300, until_ts)
        self.update_cache(query)

    def remove_view(self, view_ts):
        # assume cache_ts is sorted
        to_remove_index = bisect_right(self.cache_ts, view_ts)
        for ts in self.cache_ts[:to_remove_index]:
            for type_name, type_dict in self.cache[ts].items():
                for asn, counter in type_dict.items():
                    self.cache['total'][type_name][asn][0] -= counter
                    if self.cache['total'][type_name][asn][0] == 0:
                        del self.cache['total'][type_name][asn]
        
        self.cache_ts = self.cache_ts[to_remove_index:]


    def add_view(self, view_ts):
        query = query_in_range(view_ts, view_ts)
        self.update_cache(query)
    
    def optimize_query(self, ases):
        ases = sorted(list(ases), key=lambda x: x[1])
        ases = [({pair[0] for pair in x[1]}, x[0]) for x in groupby(ases, lambda x: x[1])]

        return ases

    def update_pfx_event(self, pfx_event, inferences):
        pfx_event.add_inferences(inferences)
        if len(pfx_event.inferences) > 1:
            pfx_event.remove_inferences({DEFAULT_WORTHY_INFERENCE, DEFAULT_NOT_WORTHY_INFERENCE})

    def reindex_event(self, event):
        event.summary.update()
        if self.output:
            pprint.pprint(event.as_dict())
        else:
            logging.info(f'Indexing event {event.event_id}.')
            self.esconn.index_event(event, debug=self.debug, prefix=self.esindex_write_prefix , update=True)

    def check_peers(self, ases_to_update, view_ts):
        logging.info(f'Checking for peers in view {view_ts}.')
        min_ts = min(ases_to_update, key=lambda x: x[1])[1]
        ases_dict = dict(ases_to_update)
        tag = 'single-peer-on-attacker-path'
        query = query_in_range(min_ts, view_ts, must_tags=[tag])
        for ev in self.esconn.search_generator(self.esindex_read, query, timeout="30m"):
            ev_ts = ev.view_ts
            changed = False
            for pfx_event in ev.pfx_events:
                if pfx_event.has_tag(tag):
                    paths = self.get_interesting_paths(pfx_event)
                    if paths:
                        attackers = pfx_event.details.extract_attackers_victims()[0]
                        peer = self.get_peer_on_attackers_path(attackers, paths)
                        if peer is not None and peer in ases_dict and ev_ts >= ases_dict[peer]:
                            self.update_pfx_event(pfx_event, [ABNORMAL_PEER])
                            if not changed:
                                changed = True
            if changed:
                self.reindex_event(ev)

    """
    # Alternative if we treat attackers individually
    def check_attackers(self, ases_to_update, view_ts):
        # optimization: minimize how many events we query
        ases_to_update = self.optimize_query(ases_to_update)
        for ases, ts in ases_to_update:
            query_ases = ases
            query  = query_attackers(ts, view_ts, query_ases)
            for ev in self.esconn.search_generator(self.esindex_read, query, timeout="30m"):
                changed = False
                for pfx_event in ev.pfx_events:
                    attackers = pfx_event.details.extract_attackers_victims()[0]
                    if any({attacker in ases for attacker in attackers}):
                        self.update_pfx_event(pfx_event, [ABNORMAL_ATTACKER])
                        if not changed:
                            changed = True
                if changed: 
                    self.reindex_event(ev)
    """ 

    def check_attackers(self, ases_to_update, view_ts):
        # optimization: minimize how many events we query
        logging.info(f'Checking for attackers in view {view_ts}.')
        ases_to_update = self.optimize_query(ases_to_update)
        for ases, ts in ases_to_update:
            query_ases = [asn.split('_') if '_' in asn else asn for asn in ases]
            query  = query_attackers(ts, view_ts, query_ases)
            for ev in self.esconn.search_generator(self.esindex_read, query, timeout="30m"):
                changed = False
                for pfx_event in ev.pfx_events:
                    attackers_str = '_'.join(sorted(pfx_event.details.extract_attackers_victims()[0]))
                    if attackers_str in ases:
                        self.update_pfx_event(pfx_event, [ABNORMAL_ATTACKER])
                        if not changed:
                            changed = True
                if changed:
                    self.reindex_event(ev)

    def check_misconfig(self, ases_to_update, view_ts):
        # optimization: minimize how many events we query
        logging.info(f'Checking for misconfigurers in view {view_ts}.')
        ases_to_update = self.optimize_query(ases_to_update)
        for ases, ts in ases_to_update:
            query_ases = [asn.split('_') if '_' in asn else asn for asn in ases]
            query  = query_victims(ts, view_ts, query_ases)
            for ev in self.esconn.search_generator(self.esindex_read, query, timeout="30m"):
                changed = False
                for pfx_event in ev.pfx_events:
                    victims_str = '_'.join(sorted(pfx_event.details.extract_attackers_victims()[1]))
                    if victims_str in ases:
                        self.update_pfx_event(pfx_event, [ABNORMAL_MISCONFIG])
                        if not changed:
                            changed = True
                if changed:
                    self.reindex_event(ev)
        

    def process_view(self, view_ts):
        logging.info(f'Processing view {view_ts}.')
        self.remove_view(view_ts - TIME_WINDOW)
        self.add_view(view_ts)

    def check_view(self, view_ts):
        if view_ts in self.cache:
            for type_name, type_dict in self.cache['total'].items():
                ases_to_update = set()
                for asn, (counter, last_ts) in type_dict.items():
                    if counter > MAX_EVENTS:
                        if last_ts is None or last_ts < view_ts - TIME_WINDOW:
                            last_ts = view_ts - TIME_WINDOW
                        if last_ts != view_ts - 300 or asn in self.cache[view_ts][type_name]:
                            ases_to_update.add((asn, last_ts + 300))
                       
                        # update total table
                        type_dict[asn][1] = view_ts
                if ases_to_update:
                    self.check_functions[type_name](ases_to_update, view_ts)

    def reset_event_inferences(self, event):
        for pfx_event in event.pfx_events:
            pfx_event.remove_inferences({ABNORMAL_ATTACKER, ABNORMAL_PEER, ABNORMAL_MISCONFIG})
            if not pfx_event.inferences:
                pfx_event.add_inferences([self._get_default_inference(pfx_event.traceroutes["worthy"])])
        
    def reset_inferences(self, start_ts, end_ts):
        start_ts = start_ts - TIME_WINDOW + 300
        end_ts = end_ts + TIME_WINDOW - 300
        logging.info(f'Resetting inferences for views: {start_ts} to {end_ts}')
        query = query_inferences(start_ts, end_ts, {ABNORMAL_ATTACKER, ABNORMAL_PEER, ABNORMAL_MISCONFIG})
        for ev in self.esconn.search_generator(index=self.esindex_read, query=query, 
                                               timeout='30m'):
            self.reset_event_inferences(ev)
            self.reindex_event(ev)

    def _get_default_inference(self, tr_worthy):
        if tr_worthy:
            return DEFAULT_WORTHY_INFERENCE
        else:
            return DEFAULT_NOT_WORTHY_INFERENCE

    def process_events(self, start_ts, end_ts):
        """
        Process Views within timestamp range
        """
        
        # first load previous views to cache
        self.load_cache(start_ts)
        
        
        # check first view
        self.check_view(start_ts)

        # then process all views until end_ts + time_window
        for ts in range(start_ts + 300, end_ts + TIME_WINDOW - 300, 300):
            self.process_view(ts)
            self.check_view(ts)
