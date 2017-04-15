from lib.utils import get_key_from_config, get_mongo_db_from_config

class BaseConnector():
    def __init__(self, config, time_key='datetime', value_keys=[], tag_keys=[]):
        self._db = get_mongo_db_from_config(config)
        self._collectionName = get_key_from_config('collectionName', config)

        self._time_key = time_key
        self._value_keys = value_keys
        self._tag_keys = tag_keys

    def get_collection(self, suffix=""):
        return self._db["%s__%s" % (self._collectionName, suffix)]
