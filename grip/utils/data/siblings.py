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
from bisect import bisect_right

class Siblings:
    """
    Use AS2Org mapping from https://github.com/InetIntel/Dataset-AS-to-Organization-Mapping/tree/master
    """

    def __init__(self, datadir, never_update_files=False):
        self.current_ts = None
        self.datadir = datadir
        self.data = dict()
        self.ts_paths_map = dict()
        self.sorted_file_ts = []

        self.never_update_files = never_update_files


    def _load_paths(self):
        ts_paths = dict()
        logging.info(f'Loading Siblings data from {self.datadir}')
        for type, ts_paths_type in ts_paths.items():
            for path in Path(f'{self.datadir}/{type}/').rglob("*.json.gz"):
                path = str(path)
                ts = int(path.split('/')[-1].split('.')[-3])
                ts_paths_type[ts] = path
        return ts_paths


    def update_ts(self, ts):
        """
        Load AS2Org data by unix timestamp.

        :param ts:
        :return:
        """

        # don't reload paths if we do historical processing and they are already loaded
        if not (self.never_update_files and self.sorted_file_ts):
                self.ts_paths_map = self._load_paths()
                for record_type, ts_paths in self.ts_paths_map.items():
                    self.sorted_file_ts[record_type] = sorted(ts_paths.keys())

        closest_ts_index = bisect_right(self.sorted_file_ts, ts) - 1
        if closest_ts_index < 0:
            logging.warning(f'No Siblings data are available for timestamp {ts}.')
            self.data = dict()
            return False
        else:
            closest_ts = self.sorted_file_ts[closest_ts_index]

            if closest_ts == self.current_ts:
                logging.info(f'Siblings data are already loaded for {ts}, skipping.')

            self.data = json.load(gzip.open(self.ts_paths_map[closest_ts], 'rt', encoding='UTF-8'))

            # convert lists to sets to enable faster lookup
            self.data = {asn: set(siblings) for asn, siblings in self.data.items()}
            
            self.current_ts[type] = closest_ts

        return True

    def are_siblings(self, asn1, asn2):
        """
        Check if two ASes are sibling ASes
        :param asn1: first asn
        :param asn2: second asn
        :return: True if asn1 and asn2 are siblings
        """

        return asn2 in self.data[asn1] if asn1 in self.data else False
