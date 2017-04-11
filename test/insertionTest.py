import math, pymongo, unittest, sys
from functools import reduce

# Temporary hack to import lib files
sys.path.append('./lib/')

from generate_fake_data import FakeDataGenerator
from insert_raw import Insert as InsertRaw
from insert_fixed_range import Insert as InsertFixedRange

class TestInsertion(unittest.TestCase):
    def __init__(self, testName):
        super().__init__(testName)

        mongoURI = 'mongodb://localhost:27017/TestDb'
        mongoDbName = 'TestDb'

        rawDocumentCollectionName = 'rawDocuments__test'
        fixedRangeDocumentCollectionName = 'fixedRangeDocuments__test'

        db = pymongo.MongoClient(mongoURI + '/' + mongoDbName)[mongoDbName]
        self._rawDocumentCollection = db[rawDocumentCollectionName]
        self._fixedRangeDocumentCollection = db[fixedRangeDocumentCollectionName]

        self._insertRaw = InsertRaw(mongoURI, mongoDbName, rawDocumentCollectionName)
        self._insertFixedRange = InsertFixedRange(mongoURI, mongoDbName, fixedRangeDocumentCollectionName)

        self._fakeDataGenerator = FakeDataGenerator()

    def assertAlmostEqual(self, value1, value2):
        self.assertEqual(math.floor(100000 * value1), math.floor(100000 * value2))

    def test_0_insert(self):
        """ It should insert 10000 documents """
        # Removing all documents from test collection
        self._rawDocumentCollection.delete_many({})
        self._fixedRangeDocumentCollection.delete_many({})

        result = self._fakeDataGenerator.insert(10000, [
            self._insertRaw,
            self._insertFixedRange,
        ])

        self.assertEqual('OK', result)

    def test_1_count(self):
        """ Fixex-range total count should be 10000 """
        fixedRangeAggregates = self._fixedRangeDocumentCollection.find({})
        totalCount = reduce(lambda count, doc: count + doc['count'], fixedRangeAggregates, 0)

        self.assertEqual(totalCount, 10000)

    def test_2_aggregate(self):
        """ Fixex-range collection should be retrieved by aggregating the 10000 raw documents """
        rawAggregates = self._rawDocumentCollection.aggregate([{
            '$group': {
                '_id': {
                    'param_foo': '$param_foo',
                    'param_bar': '$param_bar',
                },
                'count': { '$sum': 1 },
                'sum': { '$sum': '$value' },
            }
        }])

        for aggregate in rawAggregates:
            fixedRangeDocument = self._fixedRangeDocumentCollection.find_one(aggregate['_id'])
            self.assertEqual(fixedRangeDocument['count'], aggregate['count'])
            self.assertAlmostEqual(fixedRangeDocument['sum'], aggregate['sum'])
