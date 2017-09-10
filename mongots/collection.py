from mongots.query import MongoTSQueryBuilder


class MongoTSCollection(MongoTSQueryBuilder):
    def __init__(self, mongo_collection):
        self._collection = mongo_collection

    def insert_one(self, value, datetime, tags=None):
        filters = self.build_filters(datetime, tags)

        return 0
