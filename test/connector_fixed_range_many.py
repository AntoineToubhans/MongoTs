import math, random, unittest
from functools import reduce

from test.base_test import BaseTest
from lib.connector_raw import RawConnector
from lib.connector_fixed_range import FixedRangeConnector

class FixedRangeCustomParamsTest(BaseTest):
    def __init__(self, testName):
        super().__init__(testName)

        timeParamName='time_yo'
        aggregateParams = ['value_foo', 'value_bar']
        groupbyParams = ['param_foo', 'param_bar']

        self._rawConnector = RawConnector(
            self._mongoConfig,
            timeParamName=timeParamName,
            aggregateParams=aggregateParams,
            groupbyParams=groupbyParams
        )

        self._fixedRangeConnector = FixedRangeConnector(
            self._mongoConfig,
            timeParamName=timeParamName,
            aggregateParams=aggregateParams,
            groupbyParams=groupbyParams
        )

        self._rawDocumentCollection = self._rawConnector.getCollection('raw')
        self._fixedRangeDocumentCollection = self._fixedRangeConnector.getCollection('fixed_range')

        # Use fake data with custom field names
        self._fakeDataGenerator.setTimeParamName(timeParamName)
        self._fakeDataGenerator.setParams([{
            'name': 'param_foo',
            'generator': lambda: random.randint(0, 10),
        }, {
            'name': 'param_bar',
            'generator': lambda: random.randint(0, 10),
        }, {
            'name': 'value_foo',
            'generator': lambda: random.gauss(1, 1),
        }, {
            'name': 'value_bar',
            'generator': lambda: random.gauss(2, -1),
        }])

        self._number = 10000

    def test_00_push(self):
        """ It should push many documents """
        # Removing all documents from test collection
        self._rawDocumentCollection.delete_many({})
        self._fixedRangeDocumentCollection.delete_many({})

        documents = self._fakeDataGenerator.generateDocuments(self._number)

        self.assertInsertCount(documents, self._rawConnector)
        self.assertInsertCount(documents, self._fixedRangeConnector)

    def test_01_foo_count(self):
        """ Fixex-range value_foo total count should be right """
        fixedRangeAggregates = self._fixedRangeDocumentCollection.find({})
        fooTotalCount = reduce(lambda count, doc: count + doc['value_foo__count'], fixedRangeAggregates, 0)
        self.assertEqual(fooTotalCount, self._number)

    def test_02_bar_count(self):
        """ Fixex-range value_bar total count should be right """
        fixedRangeAggregates = self._fixedRangeDocumentCollection.find({})
        barTotalCount = reduce(lambda count, doc: count + doc['value_bar__count'], fixedRangeAggregates, 0)
        self.assertEqual(barTotalCount, self._number)

    def test_03_aggregate(self):
        """ Fixex-range collection should be retrieved by aggregating the raw documents """
        rawAggregates = self._rawDocumentCollection.aggregate([{
            '$group': {
                '_id': {
                    'param_foo': '$param_foo',
                    'param_bar': '$param_bar',
                },
                'value_foo__count': { '$sum': 1 },
                'value_foo__sum': { '$sum': '$value_foo' },
                'value_bar__count': { '$sum': 1 },
                'value_bar__sum': { '$sum': '$value_bar' },
            }
        }])

        for aggregate in rawAggregates:
            fixedRangeDocument = self._fixedRangeDocumentCollection.find_one(aggregate['_id'])

            self.assertEqual(fixedRangeDocument['value_foo__count'], aggregate['value_foo__count'])
            self.assertAlmostEqual(fixedRangeDocument['value_foo__sum'], aggregate['value_foo__sum'])

            self.assertEqual(fixedRangeDocument['value_bar__count'], aggregate['value_bar__count'])
            self.assertAlmostEqual(fixedRangeDocument['value_bar__sum'], aggregate['value_bar__sum'])
