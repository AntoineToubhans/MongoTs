import unittest
import mongomock
from unittest.mock import MagicMock

import mongots


class MongoTSClientTest(unittest.TestCase):

    def test_mongots_client_init_succeeds_when_mongo_client_is_provided(self):
        mongots.client.MongoClient = MagicMock()
        mocked_mongo_client = mongomock.MongoClient()

        mongots_client = mongots.MongoTSClient(
            mongo_client=mocked_mongo_client,
        )

        self.assertIsInstance(mongots_client._client, mongomock.MongoClient)
        self.assertEqual(mongots_client._client, mocked_mongo_client)
        mongots.client.MongoClient.assert_not_called()

    def test_mongots_client_init_succeeds_when_kwargs_are_provided(self):
        mongots.client.MongoClient = mongomock.MongoClient

        mongots_client = mongots.MongoTSClient(host='toto.fr', port=66666)

        self.assertIsInstance(mongots_client._client, mongomock.MongoClient)
        self.assertEqual(mongots_client._client.address, ('toto.fr', 66666))

    def test_mongots_client_init_succeeds_when_args_are_provided(self):
        mongots.client.MongoClient = mongomock.MongoClient

        mongots_client = mongots.MongoTSClient('toto.fr', 66666)

        self.assertIsInstance(mongots_client._client, mongomock.MongoClient)
        self.assertEqual(mongots_client._client.address, ('toto.fr', 66666))

    def test_get_database_returns_a_database(self):
        mongots_client = mongots.MongoTSClient(
            mongo_client=mongomock.MongoClient(),
        )
        mongots_database = mongots_client.get_database('TestDb')

        self.assertIsNotNone(mongots_database)
        self.assertIsInstance(mongots_database._database, mongomock.Database)
        self.assertEqual(mongots_database._database.name, 'TestDb')

    def test_magic_get_database_returns_a_database(self):
        mongots_client = mongots.MongoTSClient(
            mongo_client=mongomock.MongoClient(),
        )
        mongots_database = mongots_client.TestDb

        self.assertIsNotNone(mongots_database)
        self.assertIsInstance(mongots_database._database, mongomock.Database)
        self.assertEqual(mongots_database._database.name, 'TestDb')

    def test_get_collection_returns_a_collection(self):
        mongots_client = mongots.MongoTSClient(
            mongo_client=mongomock.MongoClient()
        )
        mongots_database = mongots_client.get_database('TestDb')
        mongots_collection = mongots_database.get_collection('temperature')

        self.assertIsNotNone(mongots_collection)
        self.assertIsInstance(
            mongots_collection._collection,
            mongomock.Collection,
        )
        self.assertEqual(mongots_collection._collection.name, 'temperature')

    def test_magic_get_collection_returns_a_collection(self):
        mongots_collection = mongots.MongoTSClient(
            mongo_client=mongomock.MongoClient(),
        ).TestDb.temperature

        self.assertIsNotNone(mongots_collection)
        self.assertIsInstance(
            mongots_collection._collection,
            mongomock.Collection,
        )
        self.assertEqual(mongots_collection._collection.name, 'temperature')

    def test_magic_get_database_fails_for_invalid_name(self):
        with self.assertRaises(AttributeError):
            mongots.MongoTSClient(
                mongo_client=mongomock.MongoClient(),
            )._test_db.temperature

    def test_magic_get_collection_fails_for_invalid_name(self):
        with self.assertRaises(AttributeError):
            mongots.MongoTSClient(
                mongo_client=mongomock.MongoClient(),
            ).TestDb._temperature

    def test_mongots_collection_has_a_metadata_collection(self):
        mongots_collection = mongots.MongoTSClient(
            mongo_client=mongomock.MongoClient(),
        ).TestDb.temperature

        self.assertIsNotNone(mongots_collection)
        self.assertIsInstance(
            mongots_collection._metadata,
            mongots.metadata.MongoTSMetadata,
        )
        self.assertEqual(
            mongots_collection._metadata._metadata_collection.name,
            'mongots__metadata',
        )
