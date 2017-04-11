import pymongo

class BaseConnector():
    def __init__(self, mongoURI, dbName, collectionName):
        self._db = pymongo.MongoClient(mongoURI + '/' + dbName)[dbName]
        self._collection = self._db[collectionName]
