from mongots.query import build_filter_query


class MongoTSCollection():
    def __init__(self, mongo_collection):
        self._collection = mongo_collection

    def insert_one(self, value, datetime, tags=None):
        filters = build_filter_query(datetime, tags)

        return 1
