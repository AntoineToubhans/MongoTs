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

    def test_build_initial_match_succeeds(self):
        self.assertEqual(query.build_initial_match(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            {'Derrick': 'contre Superman'},
        ), {
            '$match': {
                'Derrick': 'contre Superman',
                'datetime': {
                    '$gte': datetime(2001, 1, 1, 0, 0),
                    '$lte': datetime(2002, 1, 1, 0, 0),
                }
            }
        })

    def build_unwind_and_match_data():
        years_pipeline = []

        months_pipeline = [{
          '$unwind': '$months',
        }, {
          '$match': {
            'months.datetime': {
              '$gte': datetime(2001, 3, 1),
              '$lte': datetime(2001, 4, 1),
            }
          }
        }]

        days_pipeline = [{
          '$unwind': '$months',
        }, {
          '$match': {
            'months.datetime': {
              '$gte': datetime(2001, 3, 1),
              '$lte': datetime(2001, 4, 1),
            }
          }
        }, {
          '$unwind': '$months.days',
        }, {
          '$match': {
            'months.days.datetime': {
              '$gte': datetime(2001, 3, 22),
              '$lte': datetime(2001, 4, 2),
            }
          }
        }]

        hours_pipeline = [{
          '$unwind': '$months',
        }, {
          '$match': {
            'months.datetime': {
              '$gte': datetime(2001, 3, 1),
              '$lte': datetime(2001, 4, 1),
            }
          }
        }, {
          '$unwind': '$months.days',
        }, {
          '$match': {
            'months.days.datetime': {
              '$gte': datetime(2001, 3, 22),
              '$lte': datetime(2001, 4, 2),
            }
          }
        }, {
          '$unwind': '$months.days.hours',
        }, {
          '$match': {
            'months.days.hours.datetime': {
              '$gte': datetime(2001, 3, 22, 12),
              '$lte': datetime(2001, 4, 2, 0),
            }
          }
        }]

        return [
            (datetime(2001, 3, 22, 12), datetime(2001, 4, 2), '1y', years_pipeline),
            (datetime(2001, 3, 22, 12), datetime(2001, 4, 2), '1m', months_pipeline),
            (datetime(2001, 3, 22, 12), datetime(2001, 4, 2), '1d', days_pipeline),
            (datetime(2001, 3, 22, 12), datetime(2001, 4, 2), '1h', hours_pipeline),
        ]

    @data_provider(build_unwind_and_match_data)
    def test_build_unwind_and_match_succeeds(self, start, end, interval, pipeline):
        self.assertEqual(query.build_unwind_and_match(start, end, interval), pipeline)


    def build_project_data():
        years = {
            '$project': {
                'datetime': '$datetime',
                'count': '$count',
                'sum': '$sum',
                'sum2': '$sum2',
            }
        }

        months = {
            '$project': {
                'datetime': '$months.datetime',
                'count': '$months.count',
                'sum': '$months.sum',
                'sum2': '$months.sum2',
            }
        }

        days = {
            '$project': {
                'datetime': '$months.days.datetime',
                'count': '$months.days.count',
                'sum': '$months.days.sum',
                'sum2': '$months.days.sum2',
            }
        }

        hours = {
            '$project': {
                'datetime': '$months.days.hours.datetime',
                'count': '$months.days.hours.count',
                'sum': '$months.days.hours.sum',
                'sum2': '$months.days.hours.sum2',
            }
        }

        return [
            ('1y', years),
            ('1m', months),
            ('1d', days),
            ('1h', hours),
        ]

    @data_provider(build_project_data)
    def test_build_project_succeeds(self, interval, project_stage):
        self.assertEqual(query.build_project(interval), project_stage)
