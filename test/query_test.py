import unittest
import mongomock
from datetime import datetime

from mongots import query


class MongoTSQueryBuilderTest(unittest.TestCase):

    def test_build_filter_add_a_timestamp_to_tags(self):
        filters = query.build_filter_query(
            datetime(2003, 12, 31, 12, 33, 15),
            tags={ 'city': 'Paris', 'station': 2 },
        )

        self.assertEqual(filters, {
            'city': 'Paris',
            'station': 2,
            query.DATETIME_KEY: datetime(2003, 1, 1, 0, 0)},
        )

    def test_build_filter_add_a_timestamp_when_no_tags_are_provided(self):
        filters = query.build_filter_query(
            datetime(2003, 12, 31, 12, 33, 15),
        )

        self.assertEqual(filters, {
            query.DATETIME_KEY: datetime(2003, 1, 1, 0, 0)},
        )

    def test_build_update_query_succeeds(self):
        update = query.build_update_query(42.666, datetime(2019, 7, 2, 15, 12))

        self.assertIsNotNone(update)
        self.assertIsNotNone(update.get('$inc'))
