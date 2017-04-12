import datetime, random, time

defaultParams = [{
    'name': 'param_foo',
    'generator': lambda: random.randint(0, 10),
}, {
    'name': 'param_bar',
    'generator': lambda: random.randint(0, 10),
}, {
    'name': 'value',
    'generator': lambda: random.gauss(1, 1),
}]

class FakeDataGenerator():

    def __init__(
        self,
        startTime=time.time(),
        timeParamName='datetime',
        timeIncrement=3,
        params=defaultParams
    ):
        self._time = startTime
        self._timeParamName = timeParamName
        self._timeIncrement = timeIncrement
        self._params = params

    def generateDocument(self):
        self._time += random.randint(0, self._timeIncrement)

        document = {
            param['name']: param['generator']()
            for param in self._params
        }
        document[self._timeParamName] = datetime.datetime.fromtimestamp(self._time)

        return document

    def pushMany(self, number, connectors=[], debug=False):
        for index in range(0, number):
            document = self.generateDocument()

            for connector in connectors:
                connector.push(document)

            if debug and index % 1000 == 999:
                print('%i document inserted ...' % index, flush=True)

        return 'OK'
