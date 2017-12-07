from datetime import datetime
from pymongo.collection import Collection

from mongots.types import Tags


def update_metadata(
    metadata_collection: Collection,
    timestamp: datetime,
    tags: Tags = None,
) -> bool:

    return 1
