import unittest
from datetime import datetime
from unittest_data_provider import data_provider

from mongots import query


class QueryTest(unittest.TestCase):

    def valid_intervals():
        return [
            ('1y', []),
            ('year', []),
            ('1m', ['months']),
            ('month', ['months']),
            ('1d', ['months', 'days']),
            ('day', ['months', 'days']),
            ('1h', ['months', 'days', 'hours']),
            ('hour', ['months', 'days', 'hours']),
        ]

    @data_provider(valid_intervals)
    def test_get_keys_from_interval_returns_correct_keys(self, interval, keys):
        self.assertEqual(query._get_keys_from_interval(interval), keys)

    def invalid_intervals():
        return [('t'), ('d'), ('m'), ('1')]

    @data_provider(invalid_intervals)
    def test_get_keys_from_interval_fails_for_invalid_interval(self, invalid_interval):
        with self.assertRaises(Exception):
            query._get_keys_from_interval(invalid_interval)
