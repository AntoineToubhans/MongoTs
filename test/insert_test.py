import unittest
from datetime import datetime
from pandas import np
from unittest_data_provider import data_provider

from mongots import insert


class InsertTest(unittest.TestCase):

    def test_build_filter_add_a_timestamp_to_tags(self):
        filters = insert.build_filter(
            datetime(2003, 12, 31, 12, 33, 15),
            tags={'city': 'Paris', 'station': 2},
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

    def test_build_filter_do_not_modify_the_tag_object(self):
        tags = {'city': 'Paris', 'station': 2}
        insert.build_filter(
            datetime(2003, 12, 31, 12, 33, 15),
            tags=tags,
        )

        self.assertEqual(tags, {'city': 'Paris', 'station': 2})

    def test_build_update_succeeds(self):
        update = insert.build_update(42.666, datetime(2019, 7, 2, 15, 12))

        self.assertIsNotNone(update)

    def test_build_update_keys_return_correct_reults(self):
        update_keys = insert._build_update_keys(datetime(1987, 5, 8, 15))

        self.assertEqual(update_keys, [
            '',
            'months.4.',
            'months.4.days.7.',
            'months.4.days.7.hours.15.',
        ])

    def test_build_inc_update_returns_correct_result(self):
        inc_update = insert._build_inc_update(42.6, [
            '',
            'months.6.',
            'months.6.days.1.',
            'months.6.days.1.hours.15.',
        ])

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

    def test_build_min_max_update_returns_correct_result(self):
        min_update, max_update = insert._build_min_max_update(42.6, [
            '',
            'months.3.',
            'months.3.days.1.',
            'months.3.days.1.hours.15.',
        ])

        self.assertEqual(min_update, {
            'min': 42.6,
            'months.3.min': 42.6,
            'months.3.days.1.min': 42.6,
            'months.3.days.1.hours.15.min': 42.6,
        })

        self.assertEqual(max_update, {
            'max': 42.6,
            'months.3.max': 42.6,
            'months.3.days.1.max': 42.6,
            'months.3.days.1.hours.15.max': 42.6,
        })

    @unittest.mock.patch('mongots.insert._build_inc_update')
    def test_build_update_calls_build_inc_update(self, _build_inc_update):
        update = insert.build_update(42.6, datetime(2019, 7, 2, 15, 12))

        self.assertIn('$inc', update)

        _build_inc_update.assert_called_with(42.6, [
            '',
            'months.6.',
            'months.6.days.1.',
            'months.6.days.1.hours.15.',
        ])

    @unittest.mock.patch('mongots.insert._build_min_max_update')
    def test_build_update_calls_build_max_update(self, _build_min_max_update):
        _build_min_max_update.return_value = {}, {}
        update = insert.build_update(42.6, datetime(2019, 7, 2, 15, 12))

        self.assertIn('$max', update)
        self.assertIn('$min', update)

        _build_min_max_update.assert_called_with(42.6, [
            '',
            'months.6.',
            'months.6.days.1.',
            'months.6.days.1.hours.15.',
        ])

    @unittest.mock.patch('mongots.insert._build_update_keys')
    def test_build_update_returns_correct_result(self, _build_update_keys):
        _build_update_keys.return_value = [
            '',
            'months.2.',
        ]
        update = insert.build_update(42.6, datetime(2022, 3, 2))

        _build_update_keys.assert_called_with(datetime(2022, 3, 2))

        self.assertEqual(update, {
            '$inc': {
                'count': 1,
                'sum': 42.6,
                'sum2': 1814.7600000000002,
                'months.2.count': 1,
                'months.2.sum': 42.6,
                'months.2.sum2': 1814.7600000000002,
            },
            '$max': {'max': 42.6, 'months.2.max': 42.6},
            '$min': {'min': 42.6, 'months.2.min': 42.6},
        })

    def empty_document_data():
        return [
            (
                datetime(1966, 3, 2, 13),
                datetime(1966, 1, 1),
                [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
            ), (
                datetime(1964, 12, 31, 23, 59),
                datetime(1964, 1, 1),
                [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
            ), (
                datetime(2000, 1, 1),
                datetime(2000, 1, 1),
                [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
            ),
        ]

    @data_provider(empty_document_data)
    def test_empty_document(self, timestamp, year_timestamp, month_day_count):
        empty_document = insert.build_empty_document(timestamp)

        self.assertEqual(empty_document['count'], 0)
        self.assertEqual(empty_document['sum'], 0)
        self.assertEqual(empty_document['sum2'], 0)
        self.assertEqual(empty_document['max'], -np.infty)
        self.assertEqual(empty_document['min'], np.infty)

        self.assertEqual(len(empty_document['months']), 12)
        self.assertEqual(year_timestamp, empty_document['datetime'])

        for month_index, month in enumerate(empty_document['months']):
            self.assertEqual(month['count'], 0)
            self.assertEqual(month['sum'], 0)
            self.assertEqual(month['sum2'], 0)
            self.assertEqual(month['max'], -np.infty)
            self.assertEqual(month['min'], np.infty)
            self.assertEqual(len(month['days']), month_day_count[month_index])

            for day in month['days']:
                self.assertEqual(day['count'], 0)
                self.assertEqual(day['sum'], 0)
                self.assertEqual(day['sum2'], 0)
                self.assertEqual(day['max'], -np.infty)
                self.assertEqual(day['min'], np.infty)
                self.assertEqual(len(day['hours']), 24)
