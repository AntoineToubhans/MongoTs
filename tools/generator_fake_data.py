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
        start_time=time.time(),
        time_key='datetime',
        time_increment=3,
        params=defaultParams
    ):
        self._time = start_time
        self._time_key = time_key
        self._time_increment = time_increment
        self._params = params

    def set_time_param_name(self, time_key):
        self._time_key = time_key

    def set_params(self, params):
        self._params = params

    def generate_document(self):
        self._time += random.randint(0, self._time_increment)

        document = {
            param['name']: param['generator']()
            for param in self._params
        }
        document[self._time_key] = datetime.datetime.fromtimestamp(self._time)

        return document

    def generate_documents(self, number):
        return [ self.generate_document() for i in range(0, number) ]
