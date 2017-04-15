from lib.connector_base import BaseConnector
import datetime

class FixedRangeConnector(BaseConnector):
    def _get_query(self, document):
        return {
            paramName: document[paramName]
            for paramName in self._groupbyParams
        }

    def _create_empty_aggregate_params_document(self):
        return {
            '%s__%s' % (paramName, aggregateType): 0
            for aggregateType in ['count', 'sum']
            for paramName in self._aggregateParams
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
        base = self._create_empty_aggregate_params_document()
        base['datetime'] = datetime.datetime(year, month, day, hour)

        return base

    def _create_empty_one_day_document(self, year, month, day):
        base = self._create_empty_aggregate_params_document()
        base['datetime'] = datetime.datetime(year, month, day)
        base['hours'] = [
            self._create_empty_one_hour_document(year, month, day, hour)
            for hour in range(0, 24)
        ]

        return base

    def _create_empty_one_month_document(self, year, month):
        day_count = self._get_day_count(year, month)

        base = self._create_empty_aggregate_params_document()
        base['datetime'] = datetime.datetime(year, month, 1)
        base['days'] = [
            self._create_empty_one_day_document(year, month, day)
            for day in range(1, day_count+1)
        ]

        return base

    def _create_empty_one_year_document(self, year, base={}):
        base.update(self._create_empty_aggregate_params_document())
        base['datetime'] = datetime.datetime(year, 1, 1)
        base['months'] = [
            self._create_empty_one_month_document(year, month)
            for month in range(1, 13)
        ]

        return base

    def push(self, document):
        # 1. parse datetime
        docDatetime = document[self._timeParamName]

        year = docDatetime.year
        month = docDatetime.month - 1 # Array index: range from 0 to 11
        day = docDatetime.day - 1     # Array index: range from 0 to 27 / 28 / 29 or 30
        hour = docDatetime.hour       # range from 0 to 23

        # 2. build the query
        query = self._get_query(document)

        # 3. build the $inc update
        baseIncKeys = [
            '',
            'months.%s.' % month,
            'months.%s.days.%s.' % (month, day),
            'months.%s.days.%s.hours.%s.' % (month, day, hour),
        ]

        incUpdate = {
            '%s%s__%s' % (baseIncKey, paramName, aggregateType): document[paramName] if aggregateType is "sum" else 1
            for baseIncKey in baseIncKeys
            for paramName in self._aggregateParams
            for aggregateType in ['count', 'sum']
        }

        # 4. do the update in mongo
        result = self.get_collection('fixed_range').update_one(query, {
            '$inc': incUpdate,
        }, False)

        if result.modified_count == 0:
            # 5. insert empty document
            empty_document = self._create_empty_one_year_document(year, base=query)
            self.get_collection('fixed_range').insert_one(empty_document)

            # 6. do the update in mongo (again)
            result = self.get_collection('fixed_range').update_one(query, {
                '$inc': incUpdate,
            }, False)

        return result.modified_count
