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

import datetime
import yaml
import logging
from unittest import TestCase

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.exceptions import RequestError, ConnectionTimeout

import grip.events.event
import grip.metrics.view_metrics
from grip.common import ES_VIEW_METRICS_INDEX, ES_OPS_EVENTS_INDEX, \
    ES_CONFIG_LOCATION


def convert_data_int_to_str(date, is_year):
    if isinstance(date, str):
        return date
    if is_year:
        return "{:04d}".format(date)
    else:
        return "{:02d}".format(date)

# XXX do these need to be renamed?
MAIN_INDEX_NAME_PATTERN = 'observatory-v4-events-{}-{}-{}'
TEST_INDEX_NAME_PATTERN = 'observatory-v4-test-events-{}-{}-{}'
CUSTOM_INDEX_NAME_PATTERN = '{}-{}-{}-{}'

class ElasticConn:
    """maintain elasticsearch connection and provide utilities"""

    def __init__(self, conffile=ES_CONFIG_LOCATION, debug=False):
        self.DEBUG = debug

        with open(conffile) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        self.es = Elasticsearch(config['nodes'],
            timeout=config['conn_timeout'],
            max_retries=config['conn_retries'],
            retry_on_timeout=True,
            use_ssl=True, verify_certs=False, ssl_show_warn=False,
            api_key=(config['api_key_id'], config['api_key_secret']))

        if not self.es.ping():
            raise ValueError("Connection failed")

    def index_ops_event(self, ops_event, index=None):
        """
        Index a OperationalEvent object to ElasticSearch
        :param ops_event: OperationalEvent object
        :param index: (optional) index name
        :return:
        """
        if index is None:
            index = ES_OPS_EVENTS_INDEX
        try:
            self.es.index(index=index, id=ops_event.get_id(), body=ops_event.as_json_str())
        except RequestError as e:
            logging.error(e)
            logging.error(ops_event.as_json_str())
            raise e
        except ConnectionTimeout as e:
            logging.error(e)

    def view_metrics_exist(self, view_ts, event_type, index=None, debug=False):
        assert (isinstance(view_ts, int))
        if index is None:
            index = ES_VIEW_METRICS_INDEX
        if debug:
            index = "{}-debug".format(index)
        try:
            data = self.es.count(index=index, body={
                "query":
                    {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "view_ts": view_ts
                                    }
                                },
                                {
                                    "match": {
                                        "event_type": event_type
                                    }
                                }
                            ]
                        }
                    }
            })
            return data['count'] == 1
        except NotFoundError:
            # index does not exist, treat this as metrics does not exist
            return False

    def index_view_metrics(self, view_metrics, index=None, debug=False):
        """
        Index a view metrics object to ElasticSearch
        :param view_metrics: view_metric object
        :param index: (optional) index name
        :param debug: if it's a debug view
        :return:
        """
        assert (isinstance(view_metrics, grip.metrics.view_metrics.ViewMetrics))
        if index is None:
            index = ES_VIEW_METRICS_INDEX
        if debug:
            index = "{}-debug".format(index)
        try:
            self.es.index(index=index, id=view_metrics.get_view_metrics_id(), body=view_metrics.as_json_str())
        except RequestError as e:
            logging.error(e)
            logging.error(view_metrics.as_json_str())
            raise e
        except ConnectionTimeout as e:
            logging.error(e)

    def delete_event_by_id(self, event_id, index=None, debug=False, prefix=None, ignore_not_found=False):

        succeeded = False
        if index is None:
            index = self.infer_index_name_by_id(event_id, debug, prefix)

        try:
            # update insert time and last modified time before inserting it to elasticsearch
            self.es.delete(index=index, id=event_id)
            succeeded = True
        except RequestError as e:
            logging.error(e)
            logging.error(event_id)
        except NotFoundError as e:
            if not ignore_not_found:
                raise e
        except ConnectionTimeout as e:
            logging.error(e)

        return succeeded

    @staticmethod
    def validate_index(index):
        """
        Make sure the index is correct
        :param index:
        :return:
        """
        fields = index.split("-")
        assert fields[-1].isnumeric() and fields[-2].isnumeric()
        assert len(fields[-1]) == 2
        assert len(fields[-2]) == 4

    def index_event(self, event, index=None, debug=False, prefix=None, update=False, upsert=True):
        """
        Index an Event object into ElasticSearch
        :param event: Event object
        :param index: (optional) index name
        :param debug: whether to commit the event to debug index
        :param update: whether to update the object instead of replace it
        :param upsert: whether to insert the document when updating an non-existing document
        :return: True if index operation succeeded
        """
        assert (isinstance(event, grip.events.event.Event))

        succeeded = False
        if index is None:
            index = self.infer_index_name_by_id(event.event_id, debug, prefix)
        self.validate_index(index)

        try:
            # update insert time and last modified time before inserting it to elasticsearch
            if event.insert_ts is None:
                # only update insert_ts if insert_ts is not available
                event.insert_ts = int(datetime.datetime.now().strftime("%s"))
            event.last_modified_ts = int(datetime.datetime.now().strftime("%s"))

            if update:
                self.es.update(index=index, id=event.event_id, body=
                {
                    "doc": event.as_dict(),
                    "doc_as_upsert": upsert,
                },
                retry_on_conflict=10,
                               )
            else:
                self.es.index(index=index, id=event.event_id, body=event.as_json())
                # Adds a JSON document to the specified data stream or index and makes it searchable. 
                # If the target is an index and the document already exists, the request updates the document and increments its version.
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
            succeeded = True
        except RequestError as e:
            logging.error(e)
            logging.error(event.event_id)
            raise e
        except ConnectionTimeout as e:
            logging.error(e)

        return succeeded

    def count_indices(self, pattern):
        indices = self.es.cat.indices(index=pattern, h='index', format='json')
        return len(indices)

    def record_exists(self, index, record_id):
        """
        Check if record exist in elasticsearch
        :param index: index name
        :param record_id: event id
        :return:
        """
        try:
            data = self.es.count(index=index, body={
                "query":
                    {
                        "match": {
                            "_id": record_id
                        }
                    }
            })
        except NotFoundError:
            return False

        return data['count'] >= 1

    def get_event_by_id(self, event_id, index=None, debug=False, prefix=None):
        """
        retrieve event object of givent event_id from ElasticSearch

        :param event_id: event id to look for event
        :param index: (optional) ElasticSearch index name
        :param debug: whether the event is in debug index
        :return: Event object, or none if retrieval failed
        """
        if index is None:
            index = self.infer_index_name_by_id(event_id, debug, prefix)
        try:
            event_json = self.es.get(index=index, id=event_id)["_source"]
        except NotFoundError:
            return None
        return grip.events.event.Event.from_dict(event_json)

    def id_generator(self, index, query, timeout="10m"):
        # get point in time
        pit = self.es.open_point_in_time(index=index, keep_alive=timeout)
        pit['keep_alive'] = timeout
        query['pit'] = pit
        
        # extract events into objects
        query["_source"] = ["_id"]
        res = self.es.search(body=query)
        count = 0

        while len(res['hits']['hits']):
            pit['id'] = res["pit_id"] # get updated pit
            events = res["hits"]["hits"]
            for e in events:
                try:
                    info_imp = e["sort"]
                    count += 1
                    yield e["_id"]
                except TypeError as err:
                    logging.error("%s", err)
                    logging.error("%s", e)
            query["search_after"] = info_imp
            query["track_total_hits"] = False # speed up pagination
            res = self.es.search(body=query)
        
        # close point in time
        self.es.close_point_in_time({'id': pit['id']})

    def search_generator(self, index, query=None, limit=-1, timeout="10m", raw_json=False):
        """
        search for events based on match conditions, yields Event object
        :param index: ES index to search
        :param query: query
        :param limit: limit of total number of objects to return
        :param timeout: timeout string, e.g. "10m" means 10 minutes
        :param raw_json: true if to return raw json string, otherwise return Event object
        :return:
        """
        # https://www.elastic.co/guide/en/elasticsearch/reference/7.16/paginate-search-results.html#scroll-search-results
        if not query:
            query = {
                'size': 100,
                "query": {
                    "match_all": {}
                },
                "sort": {
                    "view_ts": {
                        "order": "asc"
                    }
                }
            }

        # get point in time
        pit = self.es.open_point_in_time(index=index, keep_alive=timeout)
        pit['keep_alive'] = timeout
        query['pit'] = pit
        
        # extract events into objects
        res = self.es.search(body=query)
        count = 0

        while len(res['hits']['hits']):
            pit['id'] = res["pit_id"] # get updated pit
            events = res["hits"]["hits"]
            for e in events:
                try:
                    info_imp = e["sort"]
                    count += 1
                    if raw_json:
                        event = e["_source"]
                    else:
                        event = grip.events.event.Event.from_dict(e["_source"])
                    yield event
                except TypeError as err:
                    logging.error("%s", err)
                    logging.error("%s", e)
                if limit and limit > 0 and count >= limit:
                    # close point in time
                    self.es.close_point_in_time({'id': pit['id']})
                    return
            query["search_after"] = info_imp
            query["track_total_hits"] = False # speed up pagination
            res = self.es.search(body=query)
        
        # close point in time
        self.es.close_point_in_time({'id': pit['id']})

    @staticmethod
    def get_index_name(event_type, year="*", month="*", debug=False, prefix=None):
        assert (event_type in ["*", "moas", "submoas", "defcon", "edges"])
        year = convert_data_int_to_str(year, is_year=True)
        month = convert_data_int_to_str(month, is_year=False)
        if prefix is not None:
            return CUSTOM_INDEX_NAME_PATTERN.format(prefix, event_type, year, month)
        elif debug:
            return TEST_INDEX_NAME_PATTERN.format(event_type, year, month)
        else:
            return MAIN_INDEX_NAME_PATTERN.format(event_type, year, month)

    def infer_index_name_by_id(self, event_id, debug=False, prefix=None):
        """
        Infer index name by event ID.
        :param event_id: event ID
        :param debug: whether the event is logged in debug mode
        :return:
        """
        fields = event_id.split("-")
        event_type = fields[0]
        view_ts = fields[1]

        # convert non-datetime view_ts to datetime object
        view_ts = float(view_ts)
        view_ts = datetime.datetime.utcfromtimestamp(view_ts)

        index_name = self.get_index_name(event_type, view_ts.year, view_ts.month, debug, prefix)

        self.validate_index(index_name)

        return index_name


class Test(TestCase):

    def setUp(self) -> None:
        logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                            # filename=LOG_FILENAME,
                            level=logging.DEBUG)
        self.esconn = ElasticConn()

    def test_retrieving_event(self):
        event = self.esconn.get_event_by_id("moas-1590379200-12252_6147")
        self.assertEqual(
            ["not-previously-announced-by-any-newcomer", "hegemony-valley-paths", "prefix-small-edit-distance"],
            event.pfx_events[0].traceroutes["worthy_tags"])

    def test_update(self):
        """
        Test ES event object update function.
        Procesure:
        1. retrieve an known existing object from production database
        2. commit it to debug index
        3. add additional field and update it on the debug index
        4. remove the added field and add another field, update the document
        4. assert whether both added fields exist
        """
        test_event_id = "moas-1590379200-12252_6147"

        event = self.esconn.get_event_by_id(test_event_id, debug=False)

        self.esconn.delete_event_by_id(test_event_id, debug=True, ignore_not_found=True)

        # upsert the item first
        self.esconn.index_event(event=event, debug=True, update=True, upsert=True)

        event.debug["test_update"] = True
        self.esconn.index_event(event=event, update=True, debug=True)

        event.debug.pop("test_update")
        event.debug["test_update_2"] = True
        self.esconn.index_event(event=event, update=True, debug=True)

        event2 = self.esconn.get_event_by_id(test_event_id, debug=True)
        self.assertTrue(event2.debug["test_update"])
        self.assertTrue(event2.debug["test_update_2"])

    def test_infer_update(self):
        from grip.inference.inference_collector import InferenceCollector
        collector = InferenceCollector()
        logging.basicConfig(format="%(levelname)s %(asctime)s: %(message)s",
                            level=logging.INFO)
        logging.getLogger('elasticsearch').setLevel(logging.INFO)

        test_event_id = "defcon-1614603000-270625"

        event = self.esconn.get_event_by_id(test_event_id, debug=False)
        collector.infer_event(event)
        self.esconn.index_event(event, index="observatory-v4-events-defcon-2021-03-reindex", update=True,
                                debug=True)  # commit updated event back to ES

    def test_infer_index(self):
        test_event_id = "moas-1609940100-29259_37385"
        index = self.esconn.infer_index_name_by_id(test_event_id)
        self.assertEqual(index, "observatory-v4-events-moas-2021-01")
