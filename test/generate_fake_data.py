import datetime, pymongo, random, time

# Temporary hack to import lib files
import sys
sys.path.append('./lib/')

from insert_raw import Insert as InsertRaw
from insert_fixed_range import Insert as InsertFixedRange

class FakeDataGenerator():

    def __init__(
        self,
        startTime=time.time(),
        timeIncrement=3
    ):
        self._time = startTime
        self._timeIncrement = timeIncrement

    def generateDocument(self):
        self._time += random.randint(0, self._timeIncrement)

        return {
            'value': random.gauss(1, 1),
            'param_foo': random.randint(0,10),
            'param_bar': random.randint(0,10),
            'datetime': datetime.datetime.fromtimestamp(self._time),
        }

    def insert(self, number, insertionModules=[], debug=False):
        for index in range(0, number):
            document = self.generateDocument()

            for module in insertionModules:
                module.insert(document)

            if debug and index % 1000 == 999:
                print('%i document inserted ...' % index, flush=True)

if __name__ == "__main__":
    mongoURI = 'mongodb://localhost:27017/TestDb'
    mongoDbName = 'TestDb'

    insertRaw = InsertRaw(mongoURI, mongoDbName, 'rawDocuments')
    insertFixedRange = InsertFixedRange(mongoURI, mongoDbName, 'fixedRangeDocuments')

    fakeDataGenerator = FakeDataGenerator()

    fakeDataGenerator.insert(10000, [
        insertRaw,
        insertFixedRange,
    ], debug=True)
