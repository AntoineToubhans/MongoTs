from pymongo import MongoClient


class MongoTSClient():
    def __init__(self, mongoClient=None, *args, **kwargs):
        if mongoClient:
            self._client = mongoClient
        else:
            self._client = MongoClient(*args, **kwargs)
