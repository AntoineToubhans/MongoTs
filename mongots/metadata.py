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

    def update(
        self,
        collection_name: str,
        timestamp: datetime,
        tags: Tags = None,
    ) -> bool:

        tags_updated = self.update_tags(collection_name, tags)

        return tags_updated

    def update_tags(
        self,
        collection_name: str,
        tags: Tags = None,
    ) -> bool:
        if tags is None:
            return True

        result = self._metadata_collection.update_one({
            'collection_name': collection_name,
        }, {
            '$addToSet': tags,
        }, upsert=True)

        return result.acknowledged \
            and (1 == result.matched_count or result.upserted_id is not None)

    def get_tags(
        self,
        collection_name: str,
    ) -> MetadataTags:

        return self._metadata_collection.find_one({
            'collection_name': collection_name,
        }, {
            '_id': 0,
            'collection_name': 0,
        }) or {}
