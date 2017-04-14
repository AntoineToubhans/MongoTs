from lib.connector_base import BaseConnector

class RawConnector(BaseConnector):
    def push(self, document):
        result = self.get_collection('raw').insert_one(document)

        return 1 if result.acknowledged else 0
