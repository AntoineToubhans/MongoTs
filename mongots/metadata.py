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

        if tags is None:
            return True

        result = self._metadata_collection.update_one({
            'collection_name': collection_name,
        }, {
            '$addToSet': {
                'tags.{}'.format(tag): tags[tag]
                for tag in tags
            },
        }, upsert=True)

        return result.acknowledged \
            and (1 == result.matched_count or result.upserted_id is not None)

    def get_tags(
        self,
        collection_name: str,
    ) -> MetadataTags:

        mongo_tags = self._metadata_collection.find_one({
            'collection_name': collection_name,
        }, {
            'tags': 1,
        }) or {}

        return mongo_tags.get('tags', {})
