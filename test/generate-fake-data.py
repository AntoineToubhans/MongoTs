import datetime, pymongo, random, time

_mongoURI = 'mongodb://localhost:27017/TestDb'

_db = pymongo.MongoClient(_mongoURI)['TestDb']
_flatCollection = _db['TestFlatCollection']
_tsCollection = _db['TestTsCollection']

_bulkSize = 1000
_bulkNumber = 100

_flatCollection.remove({})
_tsCollection.remove({})

_tsCollection.create_index([
    ('param_foo', pymongo.ASCENDING),
    ('param_bar', pymongo.ASCENDING),
])

_time = time.time()

def generate_document():
    return {
        'value': random.gauss(1, 1),
        'param_foo': random.randint(0,10),
        'param_bar': random.randint(0,10),
        'datetime': datetime.datetime.fromtimestamp(_time),
    }

for i in range(0, _bulkNumber):
    flatBulk = _flatCollection.initialize_unordered_bulk_op()
    tsBulk = _tsCollection.initialize_unordered_bulk_op()
    for j in range(0, _bulkSize):
        _time += random.randint(0,3)
        document = generate_document()

        flatBulk.insert(document)

        year = document['datetime'].year
        month = document['datetime'].month
        day = document['datetime'].day
        hour = document['datetime'].hour
        minute = document['datetime'].minute
        second = document['datetime'].second

        dayDate = datetime.datetime(year, month, day)
        hourDate = datetime.datetime(year, month, day, hour)
        minuteDate = datetime.datetime(year, month, day, hour, minute)
        secondDate = datetime.datetime(year, month, day, hour, minute, second)

        tsBulk.find({
            'param_foo': document['param_foo'],
            'param_bar': document['param_bar'],
        }).upsert().update({
            '$inc': {
                'count': 1,
                'value': document['value'],
                'days.%s.count' % day: 1,
                'days.%s.value' % day: document['value'],
                'days.%s.hours.%s.count' % (day, hour): 1,
                'days.%s.hours.%s.value' % (day, hour): document['value'],
                'days.%s.hours.%s.minutes.%s.count' % (day, hour, minute): 1,
                'days.%s.hours.%s.minutes.%s.value' % (day, hour, minute): document['value'],
                'days.%s.hours.%s.minutes.%s.seconds.%s.count' % (day, hour, minute, second): 1,
                'days.%s.hours.%s.minutes.%s.seconds.%s.value' % (day, hour, minute, second): document['value'],
            },
            '$set': {
                'days.%s.datetime' % day: dayDate,
                'days.%s.hours.%s.datetime' % (day, hour): hourDate,
                'days.%s.hours.%s.minutes.%s.datetime' % (day, hour, minute): minuteDate,
                'days.%s.hours.%s.minutes.%s.seconds.%s.datetime' % (day, hour, minute, second): secondDate,
            }
        })

    print('Execute bulk #%s...' % i)
    flatBulk.execute()
    tsBulk.execute()
