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
