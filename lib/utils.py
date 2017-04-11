import pymongo

def getKeyFromConfig(key, config):
    try:
        return config[key]
    except Exception as e:
        raise Exception('Bad config, key "%s" is missing: %s' % (key, e))

def getMongoDbFromConfig(config):
    uri = getKeyFromConfig('uri', config)
    port = config.get('port', 27017)
    dbName = getKeyFromConfig('dbName', config)

    return pymongo.MongoClient('mongodb://%s:%s/%s' % (uri, port, dbName))[dbName]
