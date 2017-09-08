from pymongo import MongoClient


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
        return self.get_database(key)

class MongoTSDatabase():
    def __init__(self, mongo_database):
        self._database = mongo_database
