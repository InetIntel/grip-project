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

import gzip
import json
import logging
from pathlib import Path
from itertools import chain
from radix import Radix
from bisect import bisect_right

SupportedIRRs = { 'ARIN', 'RADB', 'BELL', 'BBOI', 'LACNIC',
                  'LEVEL3', 'NTTCOM', 'TC', 'WCGDB', 'AFRINIC',
                  'ALTDB', 'APNIC', 'ARIN', 'CANARIE', 'IDNIC', 
                  'PANIX', 'REACH', 'RIPE', 'RIPE-NONAUTH', 'OPENFACE',
                  'JPIRR', 'NESTEGG' }

class IRRUtils:
    def __init__(self, datadir, never_update_files):
        self.datadir = datadir
        self.radix = dict()
        self.current_ts = dict()
        self.ts_paths_map = dict()
        self.sorted_file_ts = dict()
        self.never_update_files = never_update_files

    def _load_irr_records(self, file):
        with gzip.open(file, 'rt', encoding='UTF-8') as irr_file:
            irr_data = json.load(irr_file)
            irr = next(iter(irr_data.keys()))
            irr_records = irr_data[irr]

            self.radix[irr] = Radix()

            for record in irr_records:
                pfx = record["prefix"]
                if ":" in pfx:
                    # skip ipv6 prefixes for now
                    continue
                try:
                    node = self.radix[irr].add(pfx)
                except ValueError:
                    # temporary but safe: have to fix some broken records in IRR files
                    continue

                if "irr_records" not in node.data:
                    node.data["irr_records"] = []
                node.data["irr_records"].append(record)

    def _load_paths(self):
        """
        Scan all files in data dir and load existing file paths into memory for quick searching.
        :return:
        """
        ts_paths_map = {}
        for path in Path(self.datadir).rglob("irr.*.json.gz"):
            path = str(path)
            ts = int(path.split('/')[-1].split('.')[-3])
            irr = path.split('/')[-1].split('.')[1]
            if irr not in ts_paths_map:
                ts_paths_map[irr] = {}
            ts_paths_map[irr][ts] = path
        return ts_paths_map

    def update_ts(self, ts: int, load_data=True):
        """
        Load IRR data by unix timestamp.

        :param ts:
        :return:
        """

        # don't reload paths if we do historical processing and they are already loaded
        if not (self.never_update_files and self.ts_paths_map):
            self.ts_paths_map = self._load_paths()
            for irr, ts_paths in self.ts_paths_map.items():
                self.sorted_file_ts[irr] = sorted(ts_paths.keys())
        
        corr_irr_data = {}
        for irr, tses in self.sorted_file_ts.items():

            closest_ts_index = bisect_right(tses, ts) - 1
            if closest_ts_index < 0:
                logging.error(f'No available IRR data for {irr} found before time {ts}.')
            else:
                closest_ts = tses[closest_ts_index] 
                corr_irr_data[irr] = closest_ts

        # no IRR data before this timestamp at all
        if not len(corr_irr_data):
            logging.error(f'No available IRR data found before time {ts}.')
            return False

        # check if we've already loaded these files
        if corr_irr_data == self.current_ts:
            logging.info(f'Data already loaded for {ts}, skipping.')
        else:
            # load updated files
            for irr, irr_ts in corr_irr_data.items():
                if irr not in self.current_ts or irr_ts != self.current_ts[irr]:
                    path = self.ts_paths_map[irr][irr_ts]
                    self._load_irr_records(path)
                    self.current_ts[irr] = irr_ts
        
        return True

    def validated_origins(self, pfx, ts):
        """
        Get all validated origins for the given prefix.
        :param pfx:
        :return:
        """
        origins = dict()
        for irr, radix in self.radix.items():
            if self.current_ts[irr] > ts:
                logging.error(f'No available IRR data for {irr} before {ts}.')
                continue
            nodes = radix.search_covering(pfx)
            irr_origins = set()

            for node in nodes:
                for record in node.data["irr_records"]:
                    asn = int(record["origin"].lstrip("AS"))
                    irr_origins.add(asn)

            if len(irr_origins):
                origins[irr] = irr_origins
        return origins

    def validate_prefix_origin(self, pfx, origin, ts):
        """
        Validate a given prefix-origin pair.
        We follow the methodology from existing router configuration tools based on IRR data
        such as IRRPT, BGPQ3, etc. They are way more complicated from what we are currently checking,
        but the basic idea is that they accept prefixes (even more specific, BGPQ3: <= 32, IRRPT: <= 24) 
        from the ASes that have registered them in the IRRs. 

        :param pfx:
        :param origin:
        :return:
        """
        origin = int(origin)
        pfx_length = int(pfx.split("/")[1])
        
        res = {
            "exact": [],
            "more_specific": [],
            "no_data": []
        }

        # find related IRR records
        for irr, radix in self.radix.items():
            if self.current_ts[irr] > ts:
                continue
            records = list(chain.from_iterable([n.data["irr_records"] for n in radix.search_covering(pfx)]))

            # check if there is a matching record for the prefix or its superprefix
            matched = False
            for record in records:
                if record["origin"].startswith("AS") or \
                        record["origin"].startswith("as"):
                    asn = int(record["origin"][2:])
                else:
                    continue
                if asn == origin:
                    record_pfx_len = int(record["prefix"].split('/')[-1])
                    if record_pfx_len == pfx_length:
                        res['exact'].append(irr)
                    else:
                        res['more_specific'].append(irr)
                    matched = True
                    break # https://github.com/mjschultz/py-radix/blob/52d70b050a1dcbf3a7849aa51b2420883e31539e/radix/radix.py#L493

            # no records from this IRR about this pair
            if not matched:
                res["no_data"].append(irr)               
        
        return res
