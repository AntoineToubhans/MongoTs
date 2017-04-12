import math, random, unittest
from functools import reduce

from test.base_test import BaseTest
from lib.connector_raw import RawConnector
from lib.connector_fixed_range import FixedRangeConnector

class FixedRangeCustomParamsTest(BaseTest):
    def __init__(self, testName):
        super().__init__(testName)

        self._rawConnector = RawConnector(self._mongoConfig, timeParamName='time_yo')
        self._fixedRangeConnector = FixedRangeConnector(self._mongoConfig, timeParamName='time_yo')

        self._rawDocumentCollection = self._rawConnector.getCollection('raw')
        self._fixedRangeDocumentCollection = self._fixedRangeConnector.getCollection('fixed_range')

        # Use fake data with custom field names
        self._fakeDataGenerator.setTimeParamName('time_yo')
        self._fakeDataGenerator.setParams([{
            'name': 'param_foo',
            'generator': lambda: random.randint(0, 10),
        }, {
            'name': 'param_bar',
            'generator': lambda: random.randint(0, 10),
        }, {
            'name': 'value',
            'generator': lambda: random.gauss(1, 1),
        }])

    def test_00_push(self):
        """ It should push 10000 documents with custom groupBy / aggregate / time param names """
        # Removing all documents from test collection
        self._rawDocumentCollection.delete_many({})
        self._fixedRangeDocumentCollection.delete_many({})

        result = self._fakeDataGenerator.pushMany(10000, [
            self._rawConnector,
            self._fixedRangeConnector,
        ])

        self.assertEqual('OK', result)
