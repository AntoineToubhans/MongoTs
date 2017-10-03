import unittest
from datetime import datetime
from unittest_data_provider import data_provider

from mongots import aggregateby
from mongots import query


class QueryTest(unittest.TestCase):

    def valid_intervals_to_be_floored():
        return [(
            'months', datetime(1848, 7, 20, 12, 30), datetime(1848, 7, 1),
        ), (
            'days', datetime(1848, 7, 20, 12, 30), datetime(1848, 7, 20),
        ), (
            'hours', datetime(1848, 7, 20, 12, 30), datetime(1848, 7, 20, 12),
        )]

    @data_provider(valid_intervals_to_be_floored)
    def test_get_floor_datetime_returns_correct_datetime(
        self,
        interval,
        dt,
        floor_dt,
    ):
        self.assertEqual(query._get_floor_datetime(interval, dt), floor_dt)

    def invalid_intervals_to_be_floored():
        return [
            ('years', datetime(1848, 7, 20, 12, 30)),
            ('minutes', datetime(1848, 7, 20, 12, 30)),
            ('seconds', datetime(1848, 7, 20, 12, 30)),
            ('plop', datetime(1848, 7, 20, 12, 30)),
            ('1m', datetime(1848, 7, 20, 12, 30)),
        ]

    @data_provider(invalid_intervals_to_be_floored)
    def test_get_floor_datetime_fails_for_invalid_interval(self, interval, dt):
        with self.assertRaises(Exception):
            query._get_floor_datetime(interval, dt)

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

        start = datetime(2001, 3, 22, 12)
        end = datetime(2001, 4, 2)

        return [
            (start, end, aggregateby.Aggregateby(0), years_pipeline),
            (start, end, aggregateby.Aggregateby(1), months_pipeline),
            (start, end, aggregateby.Aggregateby(2), days_pipeline),
            (start, end, aggregateby.Aggregateby(3), hours_pipeline),
        ]

    @data_provider(build_unwind_and_match_data)
    def test_build_unwind_and_match_succeeds(
        self,
        start,
        end,
        interval,
        pipeline,
    ):
        self.assertEqual(
            query.build_unwind_and_match(start, end, interval),
            pipeline,
        )

    def build_project_data():
        years = {
            '$project': {
                'datetime': '$datetime',
                'count': '$count',
                'sum': '$sum',
                'sum2': '$sum2',
                'min': '$min',
                'max': '$max',
            }
        }

        months = {
            '$project': {
                'datetime': '$months.datetime',
                'count': '$months.count',
                'sum': '$months.sum',
                'sum2': '$months.sum2',
                'min': '$months.min',
                'max': '$months.max',
            }
        }

        days = {
            '$project': {
                'datetime': '$months.days.datetime',
                'count': '$months.days.count',
                'sum': '$months.days.sum',
                'sum2': '$months.days.sum2',
                'min': '$months.days.min',
                'max': '$months.days.max',
            }
        }

        hours = {
            '$project': {
                'datetime': '$months.days.hours.datetime',
                'count': '$months.days.hours.count',
                'sum': '$months.days.hours.sum',
                'sum2': '$months.days.hours.sum2',
                'min': '$months.days.hours.min',
                'max': '$months.days.hours.max',
            }
        }

        return [
            (aggregateby.Aggregateby(0), years),
            (aggregateby.Aggregateby(1), months),
            (aggregateby.Aggregateby(2), days),
            (aggregateby.Aggregateby(3), hours),
        ]

    @data_provider(build_project_data)
    def test_build_project_succeeds(self, interval, project_stage):
        self.assertEqual(query.build_project(interval, []), project_stage)
