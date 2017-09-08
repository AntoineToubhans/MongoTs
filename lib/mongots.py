from pymongo import MongoClient


class MongoTSClient():
    def __init__(self, mongo_client=None, *args, **kwargs):
        if mongo_client:
            self._client = mongo_client
        else:
            self._client = MongoClient(*args, **kwargs)
