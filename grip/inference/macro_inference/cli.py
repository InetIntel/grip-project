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

from grip.tagger.common import REDIS_AVAIL_SECONDS
from grip.inference.macro_inference.inference_moas import MoasMacroInferencer
from grip.inference.macro_inference.inference_submoas import SubMoasMacroInferencer
from grip.common import ES_CONFIG_LOCATION

CLASSIFIERS = {
    "moas": MoasMacroInferencer,
    "submoas": SubMoasMacroInferencer,
}

def main():
    parser = argparse.ArgumentParser(description="""
    BGP Hijacks Macro Inferencer runner
    """)
    parser.add_argument("-c", "--type", required=True,
                        choices=CLASSIFIERS.keys(),
                        help="MacroInferencer type to run")

    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("-r", "--run", action="store_true",
                     help="Run MacroInferencer")
    grp.add_argument("--clear", action="store_true",
                     help="Reset inferences")
    
    parser.add_argument("-l", "--listen", action="store_true",
                        help="Listen for new data to process.")
    
    parser.add_argument("-s", "--start_ts",
                        help="Unix timestamp of view to start processing from.",
                        type=int)
    parser.add_argument("-e", "--end_ts",
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

    if len([x for x in [opts.start_ts, opts.end_ts] if x]) == 1:
            raise ValueError('You have to provide both start date and end date.')
    elif len([x for x in [opts.start_ts, opts.listen] if x]) != 1:
            raise ValueError('You have to provide either listen or start date.')

    logging.basicConfig(level="DEBUG" if opts.verbose else "INFO",
                        format="%(asctime)s|%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    if opts.debug:
        logging.getLogger('elasticsearch').setLevel(logging.INFO)
    else:
        logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    macro_inferencer = CLASSIFIERS[opts.type](options={
        "debug": opts.debug,
        "elastic_conf": opts.elastic_conf,
        "es_index_in": opts.es_index_in,
        "es_index_out": opts.es_index_out,
        "output_stdio": opts.output_stdio,
    })

    if opts.clear:
         macro_inferencer.reset_inferences(start_ts=opts.start_ts, end_ts=opts.end_ts)
    else:
        if opts.start_ts:
            macro_inferencer.process_events(start_ts=opts.start_ts, end_ts=opts.end_ts)


if __name__ == "__main__":
    main()
