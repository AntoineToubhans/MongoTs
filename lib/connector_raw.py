from connector_base import BaseConnector

class RawConnector(BaseConnector):
    def push(self, document):
        return self._collection.insert_one(document)
