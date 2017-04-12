from lib.connector_base import BaseConnector

class RawConnector(BaseConnector):
    def push(self, document):
        collection = self.getCollection('raw')

        return collection.insert_one(document)
