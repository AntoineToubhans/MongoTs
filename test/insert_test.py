import unittest
from datetime import datetime
from unittest_data_provider import data_provider

from mongots import insert


class InsertTest(unittest.TestCase):

    def test_build_filter_add_a_timestamp_to_tags(self):
        filters = insert.build_filter(
            datetime(2003, 12, 31, 12, 33, 15),
            tags={ 'city': 'Paris', 'station': 2 },
        )

        self.assertEqual(filters, {
            'city': 'Paris',
            'station': 2,
            insert.DATETIME_KEY: datetime(2003, 1, 1, 0, 0)},
        )

    def test_build_filter_add_a_timestamp_when_no_tags_are_provided(self):
        filters = insert.build_filter(
            datetime(2003, 12, 31, 12, 33, 15),
        )

        self.assertEqual(filters, {
            insert.DATETIME_KEY: datetime(2003, 1, 1, 0, 0)},
        )

    def test_build_update_succeeds(self):
        update = insert.build_update(42.666, datetime(2019, 7, 2, 15, 12))

        self.assertIsNotNone(update)

    def test_build_update_returns_correct_inc_update(self):
        update = insert.build_update(42.6, datetime(2019, 7, 2, 15, 12))

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

    def empty_document_data():
        return [
            (datetime(1966, 3, 2, 13), datetime(1966, 1, 1), [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]),
            (datetime(1964, 12, 31, 23, 59), datetime(1964, 1, 1), [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]),
            (datetime(2000, 1, 1), datetime(2000, 1, 1), [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]),
        ]

    @data_provider(empty_document_data)
    def test_empty_document(self, timestamp, year_timestamp, month_day_count):
        empty_document = insert.build_empty_document(timestamp)

        self.assertEqual(empty_document['count'], 0)
        self.assertEqual(empty_document['sum'], 0)
        self.assertEqual(empty_document['sum2'], 0)
        self.assertEqual(len(empty_document['months']), 12)
        self.assertEqual(year_timestamp, empty_document['datetime'])

        for month_index, month in enumerate(empty_document['months']):
            self.assertEqual(month['count'], 0)
            self.assertEqual(month['sum'], 0)
            self.assertEqual(month['sum2'], 0)
            self.assertEqual(len(month['days']), month_day_count[month_index])

            for day in month['days']:
                self.assertEqual(day['count'], 0)
                self.assertEqual(day['sum'], 0)
                self.assertEqual(day['sum2'], 0)
                self.assertEqual(len(day['hours']), 24)
