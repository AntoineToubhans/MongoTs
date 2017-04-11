import datetime, random, time

class FakeDataGenerator():

    def __init__(
        self,
        startTime=time.time(),
        timeIncrement=3,
        idParamsRange=10
    ):
        self._time = startTime
        self._timeIncrement = timeIncrement
        self._idParamsRange = idParamsRange

    def generateDocument(self):
        self._time += random.randint(0, self._timeIncrement)

        return {
            'value': random.gauss(1, 1),
            'param_foo': random.randint(0, self._idParamsRange),
            'param_bar': random.randint(0, self._idParamsRange),
            'datetime': datetime.datetime.fromtimestamp(self._time),
        }

    def pushMany(self, number, connectors=[], debug=False):
        for index in range(0, number):
            document = self.generateDocument()

            for connector in connectors:
                connector.push(document)

            if debug and index % 1000 == 999:
                print('%i document inserted ...' % index, flush=True)

        return 'OK'
