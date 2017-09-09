from mongots.utils import is_name_valid
from mongots import mongots_collection


class MongoTSDatabase():
    def __init__(self, mongo_database):
        self._database = mongo_database

    def get_collection(self, collection_name):
        mongo_collection = self._database.get_collection(collection_name)

        return mongots_collection.MongoTSCollection(mongo_collection)

    def __getattr__(self, key):
        if is_name_valid(key):
            return self.get_collection(key)
        else:
            raise AttributeError


class MongoTSCollection():
    def __init__(self, mongo_collection):
        self._collection = mongo_collection
