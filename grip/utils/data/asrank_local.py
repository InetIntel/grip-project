# This source code is Copyright (c) 2021 Georgia Tech Research Corporation. All
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

import json, gzip
import logging
from pathlib import Path
from datetime import datetime, timezone
from bisect import bisect_right

REL = {
    'provider': 'customer',
    'customer': 'provider',
    'peer': 'peer'
}

class AsRankLocal:
    """
    Use local ASRank datasets instead of the ASRank API
    """

    def __init__(self, datadir, max_ts=None, never_update_files=False):
        self.current_ts = {
            'asns': None,
            'orgs': None,
            'links': None,
            'cones': None
        }
        self.datadir = datadir
        self.data = {
            'asns': dict(),
            'orgs': dict(),
            'links': dict(),
            'cones': dict()
        }
        self.ts_paths_map = {
            'asns': dict(),
            'orgs': dict(),
            'links': dict(),
            'cones': dict()
        }
        self.sorted_file_ts = {
            'asns': [],
            'orgs': [],
            'links': [],
            'cones': []
        }

        self.never_update_files = never_update_files

        if not self.never_update_files:
            if max_ts is None:
                max_ts = int(datetime.now(timezone.utc).timestamp())
            
            self.update_ts(max_ts)

    def _load_paths(self):
        ts_paths = {
            'asns': dict(),
            'orgs': dict(),
            'links': dict(),
            'cones': dict()
        }
        logging.info(f'Loading ASRank data from {self.datadir}')
        for type, ts_paths_type in ts_paths.items():
            for path in Path(f'{self.datadir}/{type}/').rglob("*.json.gz"):
                path = str(path)
                ts = int(path.split('/')[-1].split('.')[-3])
                ts_paths_type[ts] = path
        return ts_paths


    def update_ts(self, ts):
        """
        Load ASRank data by unix timestamp.

        :param ts:
        :return:
        """

        # don't reload paths if we do historical processing and they are already loaded
        if not (self.never_update_files and self.sorted_file_ts['asns']):
                self.ts_paths_map = self._load_paths()
                for record_type, ts_paths in self.ts_paths_map.items():
                    self.sorted_file_ts[record_type] = sorted(ts_paths.keys())


        for type, tses in self.sorted_file_ts.items():

            closest_ts_index = bisect_right(tses, ts) - 1
            if closest_ts_index < 0:
                logging.warning(f'No {type} ASRank data are available for timestamp {ts}.')
                return False
            else:
                closest_ts = tses[closest_ts_index]

                if closest_ts == self.current_ts[type]:
                    logging.info(f'{type} ASRank data are already loaded for {ts}, skipping.')

                self.data[type] = json.load(gzip.open(self.ts_paths_map[type][closest_ts], 'rt', encoding='UTF-8'))

                self.current_ts[type] = closest_ts

        return True

    def are_siblings(self, asn1, asn2):
        """
        Check if two ASes are sibling ASes, i.e., they belong to the same organization
        :param asn1: first asn
        :param asn2: second asn
        :return: True if asn1 and asn2 belong to the same organization
        """

        if any([asn not in self.data['asns'] for asn in [asn1, asn2]]):
            return False
        return self.data['asns'][asn1]['organization']['orgId'] == self.data['asns'][asn2]['organization']['orgId']

    def get_organization(self, asn):
        # Example return value: {'orgId': 'LPL-141-ARIN', 'country': {'iso': 'US'}}
        
        if asn not in self.data['asns']: 
            return None
        return self.data['asns'][asn]['organization']

    def get_registered_country(self, asn):
        """
        Get AS's registered country in ISO format (e.g., United States: US).
        """

        if asn not in self.data['asns']: 
            return None
        return self.data['asns'][asn]['organization']['country']['iso']

    def get_degree(self, asn):
        """
        Get relationship summary for the AS (#customers, #providers, #peers).
        :param asn:
        :return:
        """
        # Example return value: 'asnDegree': {'provider': 0, 'peer': 74, 'customer': 6377}

        if asn not in self.data['asns']: 
            return None

        return self.data['asns'][asn]['asnDegree']

    def is_sole_provider(self, asn_provider, asn_customer):
        """
        Checks if asn_provider and asn_customer are in a provider-customer relationship
        and asn_provider is the unique upstream of asn_customer (i.e., asn_customer has
        no other providers or peers).

        :param asn_provider: provider AS
        :param asn_customer: customer AS
        :return: True or False
        """

        if asn_customer not in self.data['asns']:
            return False
        
        degreeInfo = self.data['asns'][asn_customer]['asnDegree']
        if degreeInfo["provider"] == 1 and degreeInfo["peer"] == 0 and \
        self.get_relationship(asn_provider, asn_customer) == "p-c":
            return True
        return False

    def get_relationship(self, asn0, asn1):
        """
        Get the AS relationship between asn0 and asn1.

        asn0 is asn1's:
        - provider: "p-c"
        - customer: "c-p"
        - peer: "p-p"
        - no info: None

        :param asn0:
        :param asn1:
        :return:
        """

        if asn0 not in self.data['links'] or asn1 not in self.data['links'][asn0]:
            return None

        rel = self.data['links'][asn0][asn1]
        if rel == 'provider':
            return 'p-c'
        elif rel == 'customer':
            return 'c-p'
        elif rel == 'peer':
            return 'p-p'

    def in_customer_cone(self, asn0, asn1):
        """
        Check if asn0 is in the customer cone of asn1
        :param asn0:
        :param asn1:
        :return:
        """
        
        if asn1 not in self.data['cones']:
            return False
        return asn0 in self.data['cones'][asn1]

    def get_all_siblings(self, asn):
        """
        Get all the siblings of an ASN
        :param asn:
        :return: tuple(totalCount, ASNs)
        """
        
        if asn not in self.data['asns']:
            return 0, []
        
        orgId = self.data['asns'][asn]['organization']['orgId']

        if orgId not in self.data['orgs']:
            return 0, []
        
        org_record = self.data['orgs'][orgId]
        totalCount, siblings = org_record['members']['totalCount'], set(org_record['members']['asns'])
        totalCount -= 1
        siblings.remove(int(asn))
    
        return totalCount, list(siblings)

    def get_neighbor_ases(self, asn):
        res = {"providers": [], "customers": [], "peers": []}

        if asn not in self.data['links']:
            return res
        
        for neigh, rel in self.data['links'][asn].items():
            res[f'{REL[rel]}s'].append(neigh)

        return res        

    def get_asrank_for_asns(self, asns):
        """
        Get ASRank data for ASNs.
        :param asns:
        :return:
        """

        res = {}
        for asn in asns:
            res[asn] = self.data['asns'].get(asn, None)
        return res

    def get_rank_for_asns(self, asns):
        """
        Retrieve ranks for ASNs.
        :param asn_lst: list of asns
        :return:
        """
        res = {}
        for asn in asns:
            if asn not in self.data['asns']:
                res[asn] = None
            else:
                res[asn] = self.data['asns'][asn]['rank']
        return res
