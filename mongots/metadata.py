from datetime import datetime
from pymongo.collection import Collection

from mongots.types import MetadataTags
from mongots.types import MetadataTimeRange
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

        result = self._metadata_collection.update_one({
            'collection_name': collection_name,
        }, {
            '$addToSet': {
                'tags.{}'.format(tag): tags[tag]
                for tag in (tags or {})
            },
            '$min': {'timerange.min': timestamp},
            '$max': {'timerange.max': timestamp},
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

    def get_timerange(
        self,
        collection_name: str,
    ) -> MetadataTimeRange:

        mongo_timerange = self._metadata_collection.find_one({
            'collection_name': collection_name,
        }, {
            'timerange': 1,
        })

        if mongo_timerange is None:
            return None

        return (
            mongo_timerange['timerange']['min'],
            mongo_timerange['timerange']['max'],
        )
