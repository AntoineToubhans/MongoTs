import pymongo

def get_key_from_config(key, config):
    try:
        return config[key]
    except Exception as e:
        raise Exception('Bad config, key "%s" is missing: %s' % (key, e))

def get_mongo_db_from_config(config):
    uri = get_key_from_config('uri', config)
    port = config.get('port', 27017)
    dbName = get_key_from_config('dbName', config)

    return pymongo.MongoClient('mongodb://%s:%s/%s' % (uri, port, dbName))[dbName]
