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

    def test_build_update_query_returns_correct_inc_update(self):
        update = query.build_update_query(42.6, datetime(2019, 7, 2, 15, 12))

        self.assertIn('$inc', update)

        inc_update = update['$inc']

        self.assertEqual(inc_update, {
            'count': 1,
            'sum': 42.6,
            'sum2': 1814.7600000000002,
            'months.6.count': 1,
            'months.6.sum': 42.6,
            'months.6.sum2': 1814.7600000000002,
            'months.6.days.1.count': 1,
            'months.6.days.1.sum': 42.6,
            'months.6.days.1.sum2': 1814.7600000000002,
            'months.6.days.1.hours.15.count': 1,
            'months.6.days.1.hours.15.sum': 42.6,
            'months.6.days.1.hours.15.sum2': 1814.7600000000002,
        })
