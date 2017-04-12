from lib.connector_base import BaseConnector
import datetime

class FixedRangeConnector(BaseConnector):
    def _getQuery(self, document):
        return {
            paramName: document[paramName]
            for paramName in self._groupbyParams
        }

    def push(self, document):
        # 1. build the query
        query = self._getQuery(document)

        # 2. parse datetime
        docDatetime = document[self._timeParamName]

        year = docDatetime.year
        month = docDatetime.month
        day = docDatetime.day
        hour = docDatetime.hour
        minute = docDatetime.minute
        second = docDatetime.second

        dayDate = datetime.datetime(year, month, day)
        hourDate = datetime.datetime(year, month, day, hour)
        minuteDate = datetime.datetime(year, month, day, hour, minute)
        secondDate = datetime.datetime(year, month, day, hour, minute, second)

        # 3. build the $set update
        setUpdate = {
            'days.%s.datetime' % day: dayDate,
            'days.%s.hours.%s.datetime' % (day, hour): hourDate,
            'days.%s.hours.%s.minutes.%s.datetime' % (day, hour, minute): minuteDate,
            'days.%s.hours.%s.minutes.%s.seconds.%s.datetime' % (day, hour, minute, second): secondDate,
        }

        # 4. build the $inc update
        baseIncKeys = [
            '',
            'days.%s.' % day,
            'days.%s.hours.%s.' % (day, hour),
            'days.%s.hours.%s.minutes.%s.' % (day, hour, minute),
            'days.%s.hours.%s.minutes.%s.seconds.%s.' % (day, hour, minute, second),
        ]

        incUpdate = {
            ('%s%s__%s' % (baseIncKey, paramName, aggregateType)): document[paramName] if aggregateType is "sum" else 1
            for baseIncKey in baseIncKeys
            for paramName in self._aggregateParams
            for aggregateType in ['count', 'sum']
        }

        # 5. do the update in mongo
        self.getCollection('fixed_range').update_one(query, {
            '$inc': incUpdate,
            '$set': setUpdate,
        }, True)
