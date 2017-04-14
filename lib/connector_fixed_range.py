from lib.connector_base import BaseConnector
import datetime

class FixedRangeConnector(BaseConnector):
    def _get_query(self, document):
        return {
            paramName: document[paramName]
            for paramName in self._groupbyParams
        }

    def push(self, document):
        # 1. parse datetime
        docDatetime = document[self._timeParamName]

        year = docDatetime.year
        month = docDatetime.month
        day = docDatetime.day
        hour = docDatetime.hour
        minute = docDatetime.minute
        second = docDatetime.second

        yearDate = datetime.datetime(year, 1, 1)
        monthDate = datetime.datetime(year, month, 1)
        dayDate = datetime.datetime(year, month, day)
        hourDate = datetime.datetime(year, month, day, hour)
        minuteDate = datetime.datetime(year, month, day, hour, minute)
        secondDate = datetime.datetime(year, month, day, hour, minute, second)

        # 2. build the query
        query = self._get_query(document)
        query['year'] = yearDate

        # 3. build the $set update
        setUpdate = {
            'months.%s.datetime' % month: monthDate,
            'months.%s.days.%s.datetime' % (month, day): dayDate,
            'months.%s.days.%s.hours.%s.datetime' % (month, day, hour): hourDate,
            'months.%s.days.%s.hours.%s.minutes.%s.datetime' % (month, day, hour, minute): minuteDate,
            'months.%s.days.%s.hours.%s.minutes.%s.seconds.%s.datetime' % (month, day, hour, minute, second): secondDate,
        }

        # 4. build the $inc update
        baseIncKeys = [
            '',
            'months.%s.' % month,
            'months.%s.days.%s.' % (month, day),
            'months.%s.days.%s.hours.%s.' % (month, day, hour),
            'months.%s.days.%s.hours.%s.minutes.%s.' % (month, day, hour, minute),
            'months.%s.days.%s.hours.%s.minutes.%s.seconds.%s.' % (month, day, hour, minute, second),
        ]

        incUpdate = {
            ('%s%s__%s' % (baseIncKey, paramName, aggregateType)): document[paramName] if aggregateType is "sum" else 1
            for baseIncKey in baseIncKeys
            for paramName in self._aggregateParams
            for aggregateType in ['count', 'sum']
        }

        # 5. do the update in mongo
        result = self.get_collection('fixed_range').update_one(query, {
            '$inc': incUpdate,
            '$set': setUpdate,
        }, True)

        return 1 if result.acknowledged else 0
