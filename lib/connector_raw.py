from lib.connector_base import Connector as Base_connector

class Connector(Base_connector):
    def push(self, document):
        result = self.get_collection('raw').insert_one(document)

        return 1 if result.acknowledged else 0
