import unittest
from unittest_data_provider import data_provider

from mongots import interval


class IntervalTest(unittest.TestCase):

    def valid_str_intervals():
        return [
            ('1y', 0, 1),
            ('2y', 0, 2),
            ('1M', 1, 1),
            ('3M', 1, 3),
            ('1d', 2, 1),
            ('4d', 2, 4),
            ('1h', 3, 1),
            ('12h', 3, 12),
            ('1m', 4, 1),
            ('5m', 4, 5),
            ('1s', 5, 1),
            ('30s', 5, 30),
        ]

    @data_provider(valid_str_intervals)
    def test_parse_valid_str_interval(self, str_interval, int_interval, coef):
        parsed_interval = interval.parse_interval(str_interval)

        self.assertIsInstance(parsed_interval, interval.Interval)
        self.assertEqual(parsed_interval._interval, int_interval)
        self.assertEqual(parsed_interval._coef, coef)

    def invalid_str_intervals():
        return [('t'), ('d'), ('m'), ('1')]

    @data_provider(invalid_str_intervals)
    def test_fails_to_parse_invalid_interval(self, str_interval):
        with self.assertRaises(Exception):
            interval.parse_interval(str_interval)

    def aggregation_keys_per_interval():
        return [
            (0, []),
            (1, ['months']),
            (2, ['months', 'days']),
            (3, ['months', 'days', 'hours']),
        ]

    @data_provider(aggregation_keys_per_interval)
    def test_can_parse_valid_str_interval(
        self,
        int_interval,
        aggregation_keys,
    ):
        parsed_interval = interval.Interval(int_interval)

        self.assertEqual(parsed_interval.aggregation_keys, aggregation_keys)
