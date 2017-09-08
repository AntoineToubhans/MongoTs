import unittest
import pymongo
from unittest.mock import patch
from unittest.mock import MagicMock

import lib.mongots as mongots

class MongoTSClientTest(unittest.TestCase):
    def setUp(self):
        mongots.MongoClient = MagicMock(pymongo.MongoClient)

    def test_mongots_client_init_succeeds_when_mongo_client_is_provided(self):
        mocked_mongo_client = MagicMock(pymongo.MongoClient)

        mongots_client = mongots.MongoTSClient(mongo_client=mocked_mongo_client)

        self.assertEqual(mongots_client._client, mocked_mongo_client)
        mongots.MongoClient.assert_not_called()

    def test_mongots_client_init_succeeds_when_mongo_config_is_provided(self):
        mongots_client = mongots.MongoTSClient(host='toto.fr', port=66666)

        mongots.MongoClient.assert_called_with(host='toto.fr', port=66666)
