import datetime, math, random, unittest
from functools import reduce

from test.base_test import BaseTest
from lib.connector_fixed_range import FixedRangeConnector

class FixedRangeCustomParamsTest(BaseTest):
    def __init__(self, testName):
        super().__init__(testName)

        timeParamName='time'
        aggregateParams = ['goal', 'yellow_card', 'red_card']
        groupbyParams = []

        self._fixedRangeConnector = FixedRangeConnector(
            self._mongoConfig,
            timeParamName=timeParamName,
            aggregateParams=aggregateParams,
            groupbyParams=groupbyParams
        )

        self._fixedRangeDocumentCollection = self._fixedRangeConnector.getCollection('fixed_range')

    def test_00_push(self):
        """ It should push one document """
        # Removing all documents from test collection
        self._fixedRangeDocumentCollection.delete_many({})

        self.assertPushOneDocument({
            'time': datetime.datetime(2017, 4, 12, 18, 55, 33),
            'goal': 1,
            'yellow_card': 0,
            'red_card': 0,
        }, self._fixedRangeConnector)

    def test_01_push(self):
        """ It should push a second document """
        self.assertPushOneDocument({
            'time': datetime.datetime(2017, 4, 12, 19, 1, 3),
            'goal': 1,
            'yellow_card': 0,
            'red_card': 0,
        }, self._fixedRangeConnector)

    def test_02_push(self):
        """ It should push a third document """
        self.assertPushOneDocument({
            'time': datetime.datetime(2017, 4, 12, 19, 7, 42),
            'goal': 0,
            'yellow_card': 1,
            'red_card': 0,
        }, self._fixedRangeConnector)
