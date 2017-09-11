from mongots.query import build_empty_document
from mongots.query import build_filter_query
from mongots.query import build_update_query


class MongoTSCollection():
    def __init__(self, mongo_collection):
        self._collection = mongo_collection

    def insert_one(self, value, timestamp, tags=None):
        filters = build_filter_query(timestamp, tags)
        update = build_update_query(value, timestamp)

        result = self._collection.update_one(filters, update, upsert=False)

        if result.modified_count == 0:
            empty_document = build_empty_document(timestamp)
            empty_document.update(filters)

            self._collection.insert_one(empty_document)

            result = self._collection.update_one(filters, update, upsert=False)

        return result.modified_count
