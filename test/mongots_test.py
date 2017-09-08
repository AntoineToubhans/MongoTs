import unittest
import mongomock
from unittest.mock import MagicMock

import lib.mongots as mongots

class MongoTSClientTest(unittest.TestCase):

    def test_mongots_client_init_succeeds_when_mongo_client_is_provided(self):
        mongots.MongoClient = MagicMock()
        mocked_mongo_client = mongomock.MongoClient()

        mongots_client = mongots.MongoTSClient(mongo_client=mocked_mongo_client)

        self.assertIsInstance(mongots_client._client, mongomock.MongoClient)
        self.assertEqual(mongots_client._client, mocked_mongo_client)
        mongots.MongoClient.assert_not_called()

    def test_mongots_client_init_succeeds_when_mongo_config_is_provided(self):
        mongots.MongoClient = mongomock.MongoClient

        mongots_client = mongots.MongoTSClient(host='toto.fr', port=66666)

        self.assertIsInstance(mongots_client._client, mongomock.MongoClient)
        self.assertEqual(mongots_client._client.address, ('toto.fr', 66666))

    def test_get_database_returns_a_database(self):
        mongots.MongoClient = mongomock.MongoClient

        mongots_client = mongots.MongoTSClient(host='toto.fr', port=66666)
        mongots_database = mongots_client.get_database('TestDb')

        self.assertIsNotNone(mongots_database)
        self.assertIsInstance(mongots_database._database, mongomock.Database)
        self.assertEqual(mongots_database._database.name, 'TestDb')

    def test_magic_get_database_returns_a_database(self):
        mongots.MongoClient = mongomock.MongoClient

        mongots_client = mongots.MongoTSClient(host='toto.fr', port=66666)
        mongots_database = mongots_client.TestDb

        self.assertIsNotNone(mongots_database)
        self.assertIsInstance(mongots_database._database, mongomock.Database)
        self.assertEqual(mongots_database._database.name, 'TestDb')
