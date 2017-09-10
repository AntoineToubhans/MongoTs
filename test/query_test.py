import unittest
import mongomock
from datetime import datetime

import mongots


class MongoTSQueryBuilderTest(unittest.TestCase):

    def test_build_filter_add_a_timestamp_to_tags(self):
        query_builder = mongots.query.MongoTSQueryBuilder()

        filters = query_builder.build_filters(
            datetime(2003, 12, 31, 12, 33, 15),
            tags={ 'city': 'Paris', 'station': 2 },
        )

        self.assertEqual(filters, {
            'city': 'Paris',
            'station': 2,
            mongots.query.DATETIME_KEY: datetime(2003, 1, 1, 0, 0)},
        )

    def test_build_filter_add_a_timestamp_when_no_tags_are_provided(self):
        query_builder = mongots.query.MongoTSQueryBuilder()

        filters = query_builder.build_filters(
            datetime(2003, 12, 31, 12, 33, 15),
        )

        self.assertEqual(filters, {
            mongots.query.DATETIME_KEY: datetime(2003, 1, 1, 0, 0)},
        )
