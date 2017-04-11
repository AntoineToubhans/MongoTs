import pymongo

class Insert():
    def __init__(self, mongoURI, dbName, collectionName):
        self._db = pymongo.MongoClient(mongoURI + '/' + dbName)[dbName]
        self._collection = self._db[collectionName]

    def insert(self, document):
        return self._collection.insert_one(document)
