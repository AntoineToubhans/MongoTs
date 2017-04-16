from lib.connector_base import BaseConnector
from datetime import datetime

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

    def _get_day_count(self, year, month):
        return {
            1: 31,
            2: 29 if year % 4 == 0 and year % 400 != 0 else 28,
            3: 31,
            4: 30,
            5: 31,
            6: 30,
            7: 31,
            8: 31,
            9: 30,
            10: 31,
            11: 30,
            12: 31,
        }[month]

    def _create_empty_one_hour_document(self, year, month, day, hour):
        base = self._create_empty_aggregate_document()
        base['datetime'] = datetime(year, month, day, hour)

        return base

    def _create_empty_one_day_document(self, year, month, day):
        base = self._create_empty_aggregate_document()
        base['datetime'] = datetime(year, month, day)
        base['__hours'] = [
            self._create_empty_one_hour_document(year, month, day, hour)
            for hour in range(0, 24)
        ]

        return base

    def _create_empty_one_month_document(self, year, month):
        day_count = self._get_day_count(year, month)

        base = self._create_empty_aggregate_document()
        base['datetime'] = datetime(year, month, 1)
        base['__days'] = [
            self._create_empty_one_day_document(year, month, day)
            for day in range(1, day_count+1)
        ]

        return base

    def _create_empty_one_year_document(self, year):
        base = self._create_empty_aggregate_document()
        base['datetime'] = datetime(year, 1, 1)
        base['__months'] = [
            self._create_empty_one_month_document(year, month)
            for month in range(1, 13)
        ]

        return base

    def push(self, document):
        # 1. parse datetime
        docDatetime = document[self._time_key]

        year = docDatetime.year
        month = docDatetime.month - 1 # Array index: range from 0 to 11
        day = docDatetime.day - 1     # Array index: range from 0 to 27 / 28 / 29 or 30
        hour = docDatetime.hour       # range from 0 to 23

        # 2. build the query
        query = self._get_query(document)

        # 3. build the $inc update
        base_inc_keys = [
            '',
            '__months.%s.' % month,
            '__months.%s.days.%s.' % (month, day),
            '__months.%s.days.%s.hours.%s.' % (month, day, hour),
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
            empty_document = self._create_empty_one_year_document(year)
            empty_document.update(query)
            self.get_collection('fixed_range').insert_one(empty_document)

            # 6. do the update in mongo (again)
            result = self.get_collection('fixed_range').update_one(query, {
                '$inc': inc_update,
            }, False)

        return result.modified_count
