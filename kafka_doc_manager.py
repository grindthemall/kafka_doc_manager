# Copyright 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Kafka implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
Kafka.
"""
import base64
import logging
import warnings

from threading import Timer

import bson.json_util

import json

from kafka import KafkaProducer
from kafka.common import KafkaError
from kafka.client import KafkaClient

from bson import Binary, Code
from bson.json_util import dumps

from mongo_connector import errors
from mongo_connector.compat import u
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter


LOG = logging.getLogger(__name__)

class DocManager(DocManagerBase):
    """Kafka implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions
    to send to Kafka
    """

    def __init__(self, url, unique_id='_id', **kwargs):
        self.kafkaprod = KafkaProducer(client_id='mongotokafka-producer-mconnect', bootstrap_servers=[url])
        print("__init__ ran")
        print(str(self.kafkaprod.config))
        self.unique_key = unique_id
        self._formatter = DefaultDocumentFormatter()

    def _topic_and_mapping(self, namespace):
        """Helper method for getting the topic from a namespace."""
        topic_prefix, topic = namespace.split('.', 1)
        return topic_prefix + "_" + topic

    def stop(self):
        #logging.log(info,"Closing Kafka Broker")
        logging.info("Closing Kafka Broker")
        self.kafkaprod.close()

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]
        if doc.get('dropDatabase'):
            raise errors.OperationFailed("kafka_doc_manager does not currently support deleting a topic")

        if doc.get('renameCollection'):
            raise errors.OperationFailed(
                "kafka_doc_manager does not support renaming topics.")

        if doc.get('create'):
            db, coll = self.command_helper.map_collection(db, doc['create'])
            if db and coll:
                # if a MongoDB dbs is created, create Kafka topic for it
                # TODO
                pass

        if doc.get('drop'):
            db, coll = self.command_helper.map_collection(db, doc['drop'])
            if db and coll:
                # if a MongoDB collection is deleted, delete Kafka topic
                # TODO
                pass

    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
        """
        #
        self.commit()
        self.upsert(updated, namespace, timestamp)
        return updated

    def upsert(self, doc, namespace, timestamp):
        """Insert a document into a Kafka topic."""
        #Use Kafka Synchronous method to insert individual record.
        topic = self._topic_and_mapping(namespace)
        # Send document to Kafka with appropriate topic setting
        d_fixed = self._formatter.format_document(doc)
        doc_fixed = dumps(d_fixed)
        futureprod = self.kafkaprod.send(topic, str(doc_fixed))
        # commit right away making sure kafka buffer is empty
        self.commit()
        #try:
        #    record_metadata = futureprod.get(timeout=10)
        #    logging.log(info,record_metadata.offset)
        #except KafkaError:
        #    logging.exception("Kafka single upsert failed")
        #    pass

    def bulk_upsert(self, docs, namespace, timestamp):
        #Insert multiple documents into Kafka topics.
        # Make calls to Kafka.send async non-blocking
        # create loop to read through "docs" and send each one to the buffer
        for doc in docs:
                topic = self._topic_and_mapping(namespace)
                d_fixed = self._formatter.format_document(doc)
                doc_fixed = dumps(d_fixed)
                futureprod = self.kafkaprod.send(topic, str(doc_fixed))

        self.commit()

    def insert_file(self, f, namespace, timestamp):
        # Not implemented for Kafka
        pass
    def remove(self, document_id, namespace, timestamp):
        #Kafka does not allow deletion of random messages/offsets in a topic
        pass

    def _stream_search(self, *args, **kwargs):
        # Kafka does not allow searching for specific values in the topic
        pass

    def search(self, start_ts, end_ts):
        # Kafka does not allow searching of topics
        pass

    def commit(self):
        # Kafka does not normally require commits, but the flush command can temporarily block to empty a buffer
        self.kafkaprod.flush()

    def run_auto_commit(self):
        # Kafka does not normally require commits, but the flush command can temporarily block to empty a buffer
        self.kafkaprod.flush()

    def get_last_doc(self):
        # While we could pull the last doc from Kafka, rollbacks in Mongo
        # will replay docs into the given Kafka topic, the _id should
        # allow syncing for any consumers
        pass
