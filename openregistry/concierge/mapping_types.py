# -*- coding: utf-8 -*-
from redis import StrictRedis
from lazydb import Db as LazyDB


class MappingConfigurationException(Exception):
    pass


class MappingInterface(object):

    def get(self, key):
        raise NotImplementedError

    def set_value(self, key, value):
        raise NotImplementedError

    def has(self, key):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError


class LazyDBMapping(MappingInterface):

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        self.type = 'lazy'
        db = self.config.get('name', 'lots_mapping')
        self.db = LazyDB(db)
        self.logger.info('Set lazydb "{}" as lots mapping'.format(db))
        self._set_value = self.db.put
        self._has_value = self.db.has

    def get(self, key):
        return self.db.get(key)

    def set_value(self, key, value):
        self.db.put(key, value)

    def has(self, key):
        return self.db.has(key)

    def delete(self, key):
        self.db.delete(key)


class RedisMapping(MappingInterface):

    def __init__(self, config, logger):
        self.validate_config(config)

        self.logger = logger
        self.config = config
        self.type = 'redis'

        self.expire_time = self.config.get('expire_time', 30)

        config = {
            'host': self.config.get('host'),
            'port': self.config.get('port') or 6379,
            'db': self.config.get('name') or 0,
            'password': self.config.get('password') or None
        }
        self.db = StrictRedis(**config)

        self._check_redis_connection()

        self.logger.info('Set redis store "{db}" at {host}:{port} '
                    'as lots mapping'.format(**config))

    def _check_redis_connection(self):
        self.db.set('check', 'check')
        self.db.delete('check')

    def get(self, key):
        return self.db.get(key)

    def set_value(self, key, value):
        self.db.set(key, value, ex=self.expire_time)

    def has(self, key):
        return self.db.exists(key)

    def delete(self, key):
        self.db.delete(key)

    def validate_config(self, config):
        if 'host' not in config:
            raise MappingConfigurationException('host should be present for redis type mapping')
        if 'port' not in config:
            raise MappingConfigurationException('port should be present for redis type mapping')


class VoidMapping(MappingInterface):
    def __init__(self, config, logger):
        self.logger = logger
        self.logger.info('Set void mapping. Caching is disabled')

    def get(self, key):
        return None

    def set_value(self, key, value):
        return None

    def has(self, key):
        return True

    def delete(self, key):
        return None


MAPPING_TYPES = {
    'redis': RedisMapping,
    'lazy': LazyDBMapping,
    'void': VoidMapping
}
