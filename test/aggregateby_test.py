import unittest
from unittest_data_provider import data_provider

from mongots import aggregateby


class AggregatebyTest(unittest.TestCase):

    def valid_str_aggregatebys():
        return [
            ('1y', 0, 1),
            ('2y', 0, 2),
            ('1m', 1, 1),
            ('3m', 1, 3),
            ('1d', 2, 1),
            ('4d', 2, 4),
            ('1h', 3, 1),
            ('12h', 3, 12),
            ('1min', 4, 1),
            ('5min', 4, 5),
            ('1s', 5, 1),
            ('30s', 5, 30),
        ]

    @data_provider(valid_str_aggregatebys)
    def test_parse_valid_str_aggregateby(
        self,
        str_aggregateby,
        int_aggregateby,
        coef
    ):
        parsed_aggregateby = aggregateby.parse_aggregateby(str_aggregateby)

        self.assertIsInstance(parsed_aggregateby, aggregateby.Aggregateby)
        self.assertEqual(parsed_aggregateby._interval, int_aggregateby)
        self.assertEqual(parsed_aggregateby._coef, coef)

    def invalid_str_aggregatebys():
        return [('t'), ('d'), ('m'), ('1')]

    @data_provider(invalid_str_aggregatebys)
    def test_fails_to_parse_invalid_aggregateby(self, str_aggregateby):
        with self.assertRaises(Exception):
            aggregateby.parse_aggregateby(str_aggregateby)

    def aggregation_keys_per_aggregateby():
        return [
            (0, []),
            (1, ['months']),
            (2, ['months', 'days']),
            (3, ['months', 'days', 'hours']),
        ]

    @data_provider(aggregation_keys_per_aggregateby)
    def test_can_parse_valid_str_aggregateby(
        self,
        int_aggregateby,
        aggregation_keys,
    ):
        parsed_aggregateby = aggregateby.Aggregateby(int_aggregateby)

        self.assertEqual(parsed_aggregateby.aggregation_keys, aggregation_keys)

    def freq_per_aggregateby():
        return [
            (0, 1, '1AS'),
            (1, 2, '2MS'),
            (2, 3, '3D'),
            (3, 4, '4H'),
            (4, 5, '5T'),
            (5, 20, '20S'),
        ]

    @data_provider(freq_per_aggregateby)
    def test_aggregateby_freq(
        self,
        interval,
        coef,
        expected_freq,
    ):
        parsed_aggregateby = aggregateby.Aggregateby(interval, coef=coef)

        self.assertEqual(parsed_aggregateby.freq, expected_freq)
