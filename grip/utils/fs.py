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

import os
from datetime import datetime
from glob import glob
from itertools import chain

def _iterate_files(path):
    if os.path.exists(path):
        with os.scandir(path) as it:
            flist = list(it)
        for f in sorted(flist, key=lambda x: x.name):
            fp = os.path.join(path, f.name)
            if os.path.isfile(fp) and fp[-3:] == ".gz":
                yield fp

def _iterator_subdirs(path):
    if os.path.exists(path):
        with os.scandir(path) as it:
            dirs = list(it)
        for f in sorted(dirs, key=lambda x: x.name):
            fp = os.path.join(path, f.name)
            if not os.path.isfile(fp):
               yield fp

def fs_generate_file_list(basepath):
    for subdir in _iterator_subdirs(basepath):
        yield from fs_generate_file_list(subdir)
    yield from _iterate_files(basepath)


def fs_get_consumer_filename_from_ts(basedir, event_type, ts):
    ts = int(ts)
    datestr = ""

    # "live" consumer output is now using a date-based directory hierarchy
    datestr = datetime.utcfromtimestamp(ts).strftime("year=%Y/month=%m/day=%d/hour=%H")

    file_prefix = event_type

    if event_type == "submoas" or event_type == "defcon":
        file_prefix = "subpfx-" + file_prefix

    filename = "%s/%s/production/%s/%s.%s.events.gz" % \
            (basedir, event_type, datestr, file_prefix, ts)
    return filename

def fs_get_timestamp_from_file_path(fpath):
    return int(fpath.split("/")[-1].split(".")[1])
