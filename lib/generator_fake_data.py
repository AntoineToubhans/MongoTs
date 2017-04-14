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

    def set_time_param_name(self, timeParamName):
        self._timeParamName = timeParamName

    def set_params(self, params):
        self._params = params

    def generate_document(self):
        self._time += random.randint(0, self._timeIncrement)

        document = {
            param['name']: param['generator']()
            for param in self._params
        }
        document[self._timeParamName] = datetime.datetime.fromtimestamp(self._time)

        return document

    def generate_documents(self, number):
        return [ self.generate_document() for i in range(0, number) ]
