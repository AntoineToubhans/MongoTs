from mongots.query import build_filter_query


class MongoTSCollection():
    def __init__(self, mongo_collection):
        self._collection = mongo_collection

    def insert_one(self, value, timestamp, tags=None):
        filters = build_filter_query(timestamp, tags)

        return 1
