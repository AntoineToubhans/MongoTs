from lib.connector_base import BaseConnector
from lib.helpers_datetime import get_day_count
from datetime import datetime

AGGR_MONTH_KEY='__months'
AGGR_DAY_KEY='__days'
AGGR_HOUR_KEY='__hours'

class FixedRangeConnector(BaseConnector):
    def _get_query(self, document):
        return {
            tag_key: document[tag_key]
            for tag_key in self._tag_keys
        }

    def _create_empty_aggregate_document(self):
        return {
            value_key: { 'count': 0, 'sum': 0 }
            for value_key in self._value_keys
        }

    def _create_empty_one_hour_document(self, year, month, day, hour):
        base = self._create_empty_aggregate_document()
        base['datetime'] = datetime(year, month, day, hour)

        return base

    def _create_empty_one_day_document(self, year, month, day):
        base = self._create_empty_aggregate_document()
        base['datetime'] = datetime(year, month, day)
        base[AGGR_HOUR_KEY] = [
            self._create_empty_one_hour_document(year, month, day, hour)
            for hour in range(0, 24)
        ]

        return base

    def _create_empty_one_month_document(self, year, month):
        day_count = get_day_count(year, month)

        base = self._create_empty_aggregate_document()
        base['datetime'] = datetime(year, month, 1)
        base[AGGR_DAY_KEY] = [
            self._create_empty_one_day_document(year, month, day)
            for day in range(1, day_count+1)
        ]

        return base

    def _create_empty_one_year_document(self, year):
        base = self._create_empty_aggregate_document()
        base['datetime'] = datetime(year, 1, 1)
        base[AGGR_MONTH_KEY] = [
            self._create_empty_one_month_document(year, month)
            for month in range(1, 13)
        ]

        return base

    def push(self, document):
        # 1. parse datetime
        docDatetime = document[self._time_key]

        # 2. build the query
        query = self._get_query(document)
        query['datetime'] = datetime(docDatetime.year, 1, 1)

        # 3. build the $inc update
        month = str(docDatetime.month - 1) # Array index: range from 0 to 11
        day = str(docDatetime.day - 1)     # Array index: range from 0 to 27 / 28 / 29 or 30
        hour = str(docDatetime.hour)       # range from 0 to 23

        base_inc_keys = [
            ''.join([]),
            ''.join([AGGR_MONTH_KEY, '.', month, '.']),
            ''.join([AGGR_MONTH_KEY, '.', month, '.', AGGR_DAY_KEY, '.', day, '.']),
            ''.join([AGGR_MONTH_KEY, '.', month, '.', AGGR_DAY_KEY, '.', day, '.', AGGR_HOUR_KEY, '.', hour, '.']),
        ]

        inc_update = {
            '%s%s.%s' % (base_inc_key, value_key, aggregate_type): document[value_key] if aggregate_type is "sum" else 1
            for base_inc_key in base_inc_keys
            for value_key in self._value_keys
            for aggregate_type in ['count', 'sum']
        }

        # 4. do the update in mongo
        result = self.get_collection('fixed_range').update_one(query, {
            '$inc': inc_update,
        }, False)

        if result.modified_count == 0:
            # 5. insert empty document
            empty_document = self._create_empty_one_year_document(docDatetime.year)
            empty_document.update(query)
            self.get_collection('fixed_range').insert_one(empty_document)

            # 6. do the update in mongo (again)
            result = self.get_collection('fixed_range').update_one(query, {
                '$inc': inc_update,
            }, False)

        return result.modified_count

    def _get_range_from_interval(self, interval):
        try:
            int_interval = {
                '1y': 0,
                'year': 0,
                '1m': 1,
                'month': 1,
                '1d': 2,
                'd': 2,
                '1h': 3,
                'hour': 3,
            }[interval]

            return [AGGR_MONTH_KEY, AGGR_DAY_KEY, AGGR_HOUR_KEY][0:int_interval]
        except Exception as e:
            raise Exception('Bad interval %s: %s' % (interval, e))

    def _get_floor_datetime(self, interval, dt):
        if interval == AGGR_MONTH_KEY:
            return datetime(dt.year, dt.month, 1)
        elif interval == AGGR_DAY_KEY:
            return datetime(dt.year, dt.month, dt.day)
        elif interval == AGGR_HOUR_KEY:
            return datetime(dt.year, dt.month, dt.day, dt.hour)
        else:
            raise Exception('Bad interval %s' % interval)

    def _format_value_query_type(self, mongo_path, value_query):
        if value_query['type'] in ['sum', 'count']:
            return '$%s%s.%s' % (mongo_path, value_query['key'], value_query['type'])
        elif value_query['type'] == 'average':
            return {
                '$cond': [{
                    '$eq': [ '$%s%s.count' % (mongo_path, value_query['key']), 0 ],
                }, None, {
                    '$divide': [
                        '$%s%s.sum' % (mongo_path, value_query['key']),
                        '$%s%s.count' % (mongo_path, value_query['key']),
                    ]
                }]
            }
        else:
            raise Exception('Bad value query type: %s' % value_query['type'])

    def getData(self, start, end, interval, tag_query, value_queries):
        # 1. build pipeline: $match stage for tag query
        pipeline = [{
            '$match': tag_query,
        }]

        # 2. build pipeline: $unwind + $match stages for time range and interval
        interval_range = self._get_range_from_interval(interval)

        interval_mongo_path = ''

        for interval in interval_range:
            pipeline.extend([{
                '$unwind': '$%s%s' % (interval_mongo_path, interval)
            }, {
                '$match': {
                    '%s%s.datetime' % (interval_mongo_path, interval): {
                        '$gte': self._get_floor_datetime(interval, start),
                        '$lte': self._get_floor_datetime(interval, end),
                    }
                }
            }])

            interval_mongo_path = '%s%s.' % (interval_mongo_path, interval)

        # 3. Add $sort stage
        pipeline.append({
            '$sort': {
                '%sdatetime' % interval_mongo_path: 1
            }
        })

        # 4. Add group by tag stage
        group_stage = {
            value_query['name']: {
                '$push': self._format_value_query_type(interval_mongo_path, value_query),
            }
            for value_query in value_queries
        }

        group_stage.update({
            '_id': {
                tag_key: '$%s' % tag_key
                for tag_key in self._tag_keys
            },
            'datetimes': {
                '$push': '$%sdatetime' % interval_mongo_path,
            },
        })

        pipeline.append({
            '$group': group_stage,
        })

        # 5. Add final projection stage
        data_project_stage = {
            value_query['name']: '$%s' % value_query['name']
            for value_query in value_queries
        }
        data_project_stage['datetimes'] = '$datetimes'

        pipeline.append({
            '$project': {
                '_id': 0,
                'metadata': '$_id',
                'data': data_project_stage,
            },
        })

        return self.get_collection('fixed_range').aggregate(pipeline)
