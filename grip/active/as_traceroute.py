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
import itertools
from grip.utils.data.reserved_prefixes import ReservedPrefixes


class AsTracerouteDriver:
    """
    AS Tracereoute Driver, converting traceroute AS hops to AS paths
    """

    def __init__(self):
        self.ixp_dataset = None

    @staticmethod
    def __preprocess_trace(trace):
        hops = trace["hops"]
        return [hops[key]["asn"] for key in sorted(hops.keys())]

    def fill_as_traceroute_results(self, traceroute_results):
        """do as traceroute and fill the results structure"""

        for result_dict in traceroute_results:
            hops = self.__preprocess_trace(result_dict)
            result_dict["as_traceroute"] = self.as_traceroute(hops)

    def as_traceroute(self, hops):
        """
        Convert traceroute IP hops to AS hops.

        hops: list of ASNs
        """

        aspath = []
        prev_origins = ""
        for asn in hops:
            if prev_origins == "":
                # first time, cannot be "*"
                if asn != "*":
                    aspath.append(asn)
            elif prev_origins == asn:
                # remove continuous duplicate ASNs
                pass
            elif len(aspath) >= 2 and aspath[-1] == "*" and aspath[-2] == asn and len(asn.split()) == 1:
                # if we are at A,*,A case, does not add the new "A" to the path
                aspath.pop()
            elif asn in aspath and asn != '*':
                # loop avoidance, ignore later hops
                # TODO: may need to go back and discuss the caida_method with the team
                # IL: I would add it again. btw ignore means break not pass...
                aspath.append(asn)
            else:
                # good case
                aspath.append(asn)
            prev_origins = asn

        return aspath
