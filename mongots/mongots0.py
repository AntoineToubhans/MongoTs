from pymongo import MongoClient
from lib.utils import is_name_valid

class MongoTSClient():
    def __init__(self, mongo_client=None, *args, **kwargs):
        if mongo_client:
            self._client = mongo_client
        else:
            self._client = MongoClient(*args, **kwargs)

    def get_database(self, database_name):
        mongo_database = self._client.get_database(database_name)

        return MongoTSDatabase(mongo_database)

    def __getattr__(self, key):
        if is_name_valid(key):
            return self.get_database(key)
        else:
            raise AttributeError


class MongoTSDatabase():
    def __init__(self, mongo_database):
        self._database = mongo_database

    def get_collection(self, collection_name):
        mongo_collection = self._database.get_collection(collection_name)

        return MongoTSCollection(mongo_collection)

    def __getattr__(self, key):
        if is_name_valid(key):
            return self.get_collection(key)
        else:
            raise AttributeError


class MongoTSCollection():
    def __init__(self, mongo_collection):
        self._collection = mongo_collection
