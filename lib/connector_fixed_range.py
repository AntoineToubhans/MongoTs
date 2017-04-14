from lib.connector_base import BaseConnector
import datetime

class FixedRangeConnector(BaseConnector):
    def _get_query(self, document):
        return {
            paramName: document[paramName]
            for paramName in self._groupbyParams
        }

    def push(self, document):
        # 1. build the query
        query = self._get_query(document)

        # 2. parse datetime
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

        # 3. build the $set update
        setUpdate = {
            'year.%s.datetime' % year: yearDate,
            'year.%s.month.%s.datetime' % (year, month): monthDate,
            'year.%s.month.%s.days.%s.datetime' % (year, month, day): dayDate,
            'year.%s.month.%s.days.%s.hours.%s.datetime' % (year, month, day, hour): hourDate,
            'year.%s.month.%s.days.%s.hours.%s.minutes.%s.datetime' % (year, month, day, hour, minute): minuteDate,
            'year.%s.month.%s.days.%s.hours.%s.minutes.%s.seconds.%s.datetime' % (year, month, day, hour, minute, second): secondDate,
        }

        # 4. build the $inc update
        baseIncKeys = [
            '',
            'year.%s.' % year,
            'year.%s.month.%s.' % (year, month),
            'year.%s.month.%s.days.%s.' % (year, month, day),
            'year.%s.month.%s.days.%s.hours.%s.' % (year, month, day, hour),
            'year.%s.month.%s.days.%s.hours.%s.minutes.%s.' % (year, month, day, hour, minute),
            'year.%s.month.%s.days.%s.hours.%s.minutes.%s.seconds.%s.' % (year, month, day, hour, minute, second),
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
