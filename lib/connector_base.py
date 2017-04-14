from lib.utils import get_key_from_config, get_mongo_db_from_config

class BaseConnector():
    def __init__(self, config, timeParamName='datetime', aggregateParams=[], groupbyParams=[]):
        self._db = get_mongo_db_from_config(config)
        self._collectionName = get_key_from_config('collectionName', config)

        self._timeParamName = timeParamName
        self._aggregateParams = aggregateParams
        self._groupbyParams = groupbyParams

    def get_collection(self, suffix=""):
        return self._db["%s__%s" % (self._collectionName, suffix)]
