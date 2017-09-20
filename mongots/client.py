from pymongo import MongoClient

from mongots.utils import is_name_valid
from mongots.database import MongoTSDatabase


class MongoTSClient():
    def __init__(self, *args, mongo_client=None, **kwargs):
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
