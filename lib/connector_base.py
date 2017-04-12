from lib.utils import getKeyFromConfig, getMongoDbFromConfig

class BaseConnector():
    def __init__(self, config, timeParamName='datetime', aggregateParams=[], groupbyParams=[]):
        self._db = getMongoDbFromConfig(config)
        self._collectionName = getKeyFromConfig('collectionName', config)

        self._timeParamName = timeParamName

    def getCollection(self, suffix=""):
        return self._db["%s__%s" % (self._collectionName, suffix)]
