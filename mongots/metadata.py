from datetime import datetime
from pymongo.collection import Collection

from mongots.types import MetadataTags
from mongots.types import Tags


def update_metadata(
    metadata_collection: Collection,
    timestamp: datetime,
    tags: Tags = None,
) -> bool:

    return True


def get_tags(
    collection_name: str,
) -> MetadataTags:

    return {}
