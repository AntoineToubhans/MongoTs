from mongots.utils import is_name_valid
from mongots.collection import MongoTSCollection
from mongots.constants import METADATA_COLLECTION_SUFFIX


class MongoTSDatabase():
    def __init__(self, mongo_database):
        self._database = mongo_database

    def get_collection(self, collection_name):
        mongo_collection = self._database.get_collection(collection_name)

        metadata_mongo_collection = self._database.get_collection(
            collection_name + METADATA_COLLECTION_SUFFIX
        )

        return MongoTSCollection(
            mongo_collection,
            metadata_collection=metadata_mongo_collection,
        )

    def __getattr__(self, key):
        if is_name_valid(key):
            return self.get_collection(key)
        else:
            raise AttributeError
