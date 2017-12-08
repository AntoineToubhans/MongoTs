from datetime import datetime
from pymongo.collection import Collection

from mongots.types import MetadataTags
from mongots.types import Tags


class MongoTSMetadata():
    def __init__(
        self,
        metadata_collection: Collection,
    ) -> None:
        self._metadata_collection = metadata_collection


    def update_metadata(
        self,
        collection_name: str,
        timestamp: datetime,
        tags: Tags = None,
    ) -> bool:

        return True


    def get_tags(
        self,
        collection_name: str,
    ) -> MetadataTags:

        return {}
