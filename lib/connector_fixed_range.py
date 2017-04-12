from lib.connector_base import BaseConnector
import datetime

class FixedRangeConnector(BaseConnector):
    def push(self, document):
        # the document MUST contain a time param, for now
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

        collection = self.getCollection('fixed_range')

        return collection.update_one({
            'param_foo': document['param_foo'],
            'param_bar': document['param_bar'],
        }, {
            '$inc': {
                'count': 1,
                'sum': document['value'],
                'days.%s.count' % day: 1,
                'days.%s.sum' % day: document['value'],
                'days.%s.hours.%s.count' % (day, hour): 1,
                'days.%s.hours.%s.sum' % (day, hour): document['value'],
                'days.%s.hours.%s.minutes.%s.count' % (day, hour, minute): 1,
                'days.%s.hours.%s.minutes.%s.sum' % (day, hour, minute): document['value'],
                'days.%s.hours.%s.minutes.%s.seconds.%s.count' % (day, hour, minute, second): 1,
                'days.%s.hours.%s.minutes.%s.seconds.%s.sum' % (day, hour, minute, second): document['value'],
            },
            '$set': {
                'days.%s.datetime' % day: dayDate,
                'days.%s.hours.%s.datetime' % (day, hour): hourDate,
                'days.%s.hours.%s.minutes.%s.datetime' % (day, hour, minute): minuteDate,
                'days.%s.hours.%s.minutes.%s.seconds.%s.datetime' % (day, hour, minute, second): secondDate,
            },
        }, True)
