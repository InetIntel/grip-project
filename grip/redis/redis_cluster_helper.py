#!/usr/bin/env python

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

import calendar
import datetime
import logging
import socket
import struct
import sys

from redis.cluster import RedisCluster as Redis

class RedisHelper:

    #DEFAULT_HOST = "giglio.cc.gatech.edu"
    # change the default host to local host to avoid accidentally modify data on other machines
    # DEFAULT_HOST = "localhost"
    #DEFAULT_HOST = "redis-test"
    DEFAULT_HOST="procida.cc.gatech.edu"

    def __init__(self, host=DEFAULT_HOST, port="6379", db=0,
            username="default", password="",
            log_level="INFO"):
        self.host = host
        self.port = port
        self.db = db
        self.red = None
        self.pipes = {}
        self.pipe = None
        self.password = password
        self.username = username

        self.nodes = []

        self._init_logging(log_level)
        self.connect()

    @staticmethod
    def _init_logging(loglevel):
        logging.basicConfig(level=loglevel,
                            format="%(asctime)s|%(levelname)s: %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    @staticmethod
    def version():
        print("1.1")

    def connect(self):
        self.red = Redis(host=self.host, port=self.port, 
                password=self.password, username=self.username,
                decode_responses=True)

        self.nodes = self.red.get_nodes()
        for n in self.nodes:
            self.pipes[n.name] = self.red.pipeline(transaction=False)

    def pipe_zrem_all_below(self, pattern, below):
        for n in self.nodes:
            if n.server_type != "primary":
                continue
            p = self.pipes[n.name]
            for k in self.red.scan_iter(pattern, target_nodes=n):
                p.pipeline_execute_command("ZREMRANGEBYSCORE", k, "-inf",
                        below, target_nodes=n)


    def pipe_zincrby(self, key, amount, member):
        slot = self.red.keyslot(key)
        n = self.red.nodes_manager.get_node_from_slot(slot)
        if n.name in self.pipes:
            p = self.pipes[n.name]
        else:
            print("cannot find suitable pipe for node %s?" % (n.name))

        p.pipeline_execute_command("ZINCRBY", key, amount, member,
                target_nodes=n)

    def pipe_zadd(self, key, amount, member, n=None):
        if n is None:
            slot = self.red.keyslot(key)
            n = self.red.nodes_manager.get_node_from_slot(slot)

        if n.name in self.pipes:
            p = self.pipes[n.name]
        else:
            print("cannot find suitable pipe for node %s?" % (n.name))

        p.pipeline_execute_command("ZADD", key, amount, member,
                target_nodes=n)

    def pipe_delete(self, key, n = None):
        if n is None:
            slot = self.red.keyslot(key)
            n = self.red.nodes_manager.get_node_from_slot(slot)

        if n.name in self.pipes:
            p = self.pipes[n.name]
        else:
            print("cannot find suitable pipe for node %s?" % (n.name))

        p.pipeline_execute_command("DEL", key, target_nodes=n)

    def pipe_delete_all_keys(self, keyregex):
        for n in self.nodes:
            if n.server_type != "primary":
                continue
            p = self.pipes[n.name]
            for k in self.red.scan_iter(keyregex, target_nodes=n):
                p.pipeline_execute_command("DEL", k, target_nodes=n)

    def foreach_zrange_with_minscore(self, keyregex, minscore,
            maxscore="+inf", withscores=False):
        for n in self.nodes:
            if n.server_type != "primary":
                continue
            for k in self.red.scan_iter(keyregex, target_nodes=n):
                results = self.red.execute_command("ZRANGEBYSCORE", k,
                        minscore, maxscore, target_nodes=n)
                for found in results:
                    yield(k, n, found)

    def get_existing_zrange(self, key, withscores=False, target_nodes=None):
        if target_nodes is None:
            slot = self.red.keyslot(key)
            target_nodes = self.red.nodes_manager.get_node_from_slot(slot)

        return self.red.execute_command("ZRANGEBYSCORE", key, "-inf", "+inf",
                "WITHSCORES", target_nodes=target_nodes)

    def execute_pipelines(self):
        tot = 0
        for n in self.nodes:
            p = self.pipes[n.name]
            tot += len(p.execute())
        return tot


    def _set_pipeline(self):
        if self.pipe is None:
            self.pipe = self.red.pipeline(transaction=False)

    def get_pipeline(self):
        self._set_pipeline()
        return self.pipe

    def scan_keys(self, key):
        return self.red.scan_iter(key, target_nodes=Redis.ALL_NODES)

    def __getattr__(self, attr):
        return getattr(self.red, attr)

    @staticmethod
    def datestr_to_epoch(date):
        # return the midnight time
        return calendar.timegm(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())

    @staticmethod
    def get_bin_pfx(pfx):
        ip, mask = pfx.split("/")
        try:
            bin_ip = socket.inet_aton(ip)
        except socket.error:
            logging.warning("malformatted IP address: {}".format(ip))
            return None
        (int_ip,) = struct.unpack("!L", bin_ip)
        return format(int_ip, "032b")[:int(mask)]

    @staticmethod
    def get_str_pfx(bin_pfx):
        mask = len(bin_pfx)
        # pad the IP to 32 bit
        bin_ip = bin_pfx.ljust(32, '0')
        # find the octets, convert to int, join them, append the mask
        str_ip = ".".join([str(int(bin_ip[off:off+8], 2))
                           for off in [0, 8, 16, 24]])
        return "%s/%d" % (str_ip, mask)
