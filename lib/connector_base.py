from utils import getKeyFromConfig, getMongoDbFromConfig

class BaseConnector():
    def __init__(self, config):
        self._db = getMongoDbFromConfig(config)
        self._collectionName = getKeyFromConfig('collectionName', config)

    def getCollection(self, suffix=""):
        return self._db["%s__%s" % (self._collectionName, suffix)]
