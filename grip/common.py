"""
Common utilities and settings for the various hijacks modules.
"""

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

# Kafka broker servers
import os

from dotenv import load_dotenv, find_dotenv

KAFKA_BROKERS = "gaiola.cc.gatech.edu:9192,procida.cc.gatech.edu:9192,cirella.cc.gatech.edu:9192"

# Kafka topic and group template
# observatory-%COMPONENT%-%EVENT_TYPE%
KAFKA_TOPIC_TEMPLATE = "grip-%s-%s"
KAFKA_DEBUG_TOPIC_TEMPLATE = "grip-%s-%s-DEBUG"

# ElasticSearch defaults
ES_VIEW_METRICS_INDEX = "grip-operations-view-metrics"
ES_OPS_EVENTS_INDEX = "grip-operations-ops-events"

ES_CONFIG_LOCATION="/home/grip/esconfig.yaml"

RPKI_DATA_DIR = "/data/bgp/rpki/rpki/roas"
IRR_DATA_DIR = "/data/bgp/irr/IRR/"
ASRANK_DATA_DIR = "/data/bgp/asrank/"
HEGEMONY_DATA_DIR = "/data/bgp/hegemony/"
SIBLINGS_DATA_DIR = "/data/bgp/siblings/"

# Active probing
ACTIVE_MAX_PFX_EVENTS = 2  # max num prefixes to trace per event
ACTIVE_MAX_EVENT_ASES = 3  # max num ASes (involved in the event) from whose proximity (or from themselves) we select VPs
ACTIVE_MAX_PROBES_PER_TARGET = 10  # max num of VPs per event_AS
ACTIVE_MAX_EVENTS_PER_BIN = 10  # how many events do we do traceroutes for in every 5 minutes bin
ACTIVE_MAX_TIME_DELTA = 7200  # maximum seconds time (2 hour) difference between now and the event time

load_dotenv(find_dotenv(".limbo-cred"), override=True)
SWIFT_AUTH_OPTIONS = {
    "auth_version": '3',
    "os_username": os.environ.get('OS_USERNAME', None),
    "os_password": os.environ.get('OS_PASSWORD', None),
    "os_project_name": os.environ.get('OS_PROJECT_NAME', None),
    "os_auth_url": os.environ.get('OS_AUTH_URL', None),
}


def get_kafka_topic(component, event_type, debug=False):
    assert component in ["tagger", "driver", "collector", "inference"]
    assert event_type in ["moas", "submoas", "defcon", "edges"]

    if debug:
        return KAFKA_DEBUG_TOPIC_TEMPLATE % (component, event_type)
    else:
        return KAFKA_TOPIC_TEMPLATE % (component, event_type)
