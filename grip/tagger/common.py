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

# Redis utility functions
import time

REDIS_AVAIL_SECONDS = 86400


def get_recent_prefix_origins(prefix, view_ts, dataset):
    """
    Lookup dataset to get the most recent origins of a given prefix before view_ts.

    :param prefix: prefix in question
    :param view_ts: the maximum timestamp to lookup
    :param dataset: redis dataset or local-in-memory dataset that provides `lookup`
                    and `get_most_recent_timestamp` functions
    :return: - origins that previously announce the prefix
             - the timestamp of announcement
             - the most recent time in dataset
    """
    # search for the most recent available data about the prefix before view_ts
    prefix_info, asn_info = dataset.lookup(prefix, max_ts=view_ts - 1, exact_match=True)
    # search for the most recent time that redis was updated with data before view_ts 
    redis_ts = dataset.get_most_recent_timestamp(view_ts - 1)

    if len(asn_info) == 0:
        # information about this prefix from redis at all
        # return all origins as newcomer, empty set of old_view_origins
        return set(), None, redis_ts

    (asn, data_ts) = asn_info[0]

    old_origins_set = set()
    if asn != "":
        # the asns could be empty string if the lookup failed to find any announcements
        for asn in asn.split():
            if "{" not in asn:
                old_origins_set.add(asn)

    return old_origins_set, data_ts, redis_ts


def get_previous_origins(
        view_ts,
        prefix,
        datasets,
        in_memory
):
    """
    get newcomers for a prefix event

    :param view_ts:
    :param prefix:
    :param datasets:
    :param in_memory:
    :return: old_origins_set, outdated
    """
    assert (isinstance(view_ts, int))

    OUTDATED = False
    if in_memory:
        dataset = datasets["pfx2asn_newcomer_local"]
    else:
        dataset = datasets["pfx2asn_newcomer"]

    (old_origins_set, data_ts, redis_recent_ts) = get_recent_prefix_origins(prefix, view_ts, dataset)

    """
    We want to find the previous origins of a prefix with respect to view_ts , 
    i.e., the origins of the prefix during the timebin [view_ts - 600, view_ts - 300].
    If redis was updated at view_ts - 300, it is up to date with respect to the 
    previous timebin. However, if it wasn't updated at this timestamp, it means we
    are missing info about this specific timebin and redis is not up to date. 
    """
    if (redis_recent_ts is None) or (redis_recent_ts < view_ts - 300):
        OUTDATED = True
    if (data_ts is None) or (not OUTDATED and data_ts < view_ts - 300):
        """
        i) There is no information about this prefix in redis, so we consider all origins as newcomers
        and return an empty set of old_origins.
        ii) If redis is up to date and the data about the prefix were updated before view_ts - 300,
        it means that during the previous timebin this prefix wasn't originated by any AS. 
        Therefore, the prefix has no previous origins. Note: this will never occur in the case of
        the dataset in memory since redis_recent_ts and data_ts (if not None) will be the same and equal
        to the file_ts.
        """
        return set(), OUTDATED

    return old_origins_set, OUTDATED
