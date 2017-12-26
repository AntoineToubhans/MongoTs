from mongots.constants import METADATA_COLLECTION_NAME

from mongots.utils import is_name_valid
from mongots.collection import MongoTSCollection
from mongots.metadata import MongoTSMetadata


class MongoTSDatabase():
    def __init__(self, mongo_database):
        self._database = mongo_database
        self._metadata = MongoTSMetadata(
            mongo_database.get_collection(METADATA_COLLECTION_NAME),
        )

    def get_collection(self, collection_name):
        mongo_collection = self._database.get_collection(collection_name)

        return MongoTSCollection(
            mongo_collection,
            metadata=self._metadata,
        )

    def get_collections(self):
        """
        Get all collections in the database

        Return (array):
            A list of collection descriptor (dict)
            containing the following keys:
            - collection_name (str): the name of the collection
            - tags (dict): tag description
                           see MongoTSCollection.get_tags function
            - timerange (dict): the min/max timestamp in the collection
                                see MongoTSCollection.get_timerange function
        """
        return self._metadata.get_collections()

    def __getattr__(self, key):
        if is_name_valid(key):
            return self.get_collection(key)
        else:
            raise AttributeError
