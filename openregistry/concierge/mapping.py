# -*- coding: utf-8 -*-
from functools import partial

from redis import StrictRedis
from lazydb import Db as LazyDB


class LotMapping(object):
    """Mapping for processed auctions"""
    type = ''

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        self.enable = self.config.get('enable', False)
        if 'host' in self.config:
            self.type = 'redis'
            config = {
                'host': self.config.get('host'),
                'port': self.config.get('port') or 6379,
                'db': self.config.get('name') or 0,
                'password': self.config.get('password') or None
            }
            self.db = StrictRedis(**config)
            self.logger.info('Set redis store "{db}" at {host}:{port} '
                        'as lots mapping'.format(**config))
            set = partial(self.db.set, ex=30)
            self._set_value = set
            self._has_value = self.db.exists
        else:
            self.type = 'lazy'
            db = self.config.get('name', 'lots_mapping')
            self.db = LazyDB(db)
            self.logger.info('Set lazydb "{}" as lots mapping'.format(db))
            self._set_value = self.db.put
            self._has_value = self.db.has

    def get(self, key):
        if not self.enable:
            self.logger.warning('Caching is disabled, when `get` method is called. Returned None by default')
            return None

        key = str(key)
        return self.db.get(key)

    def put(self, key, value, **kwargs):
        if not self.enable:
            self.logger.warning('Caching is disabled, when `put` method is called. Returned None by default')
            return None

        key = str(key)
        self._set_value(key, value, **kwargs)

    def has(self, key):
        if not self.enable:
            self.logger.warning('Caching is disabled, when `has` method is called. Returned True by default')
            return True

        key = str(key)
        return self._has_value(key)

    def delete(self, key):
        if not self.enable:
            self.logger.warning('Caching is disabled, when `delete` method is called. Returned None by default')
            return None

        key = str(key)
        return self.db.delete(key)


def prepare_lot_mapping(config, logger, check=False):
    """
    Initialization of auctions_mapping, which are used for tracking auctions,
    which already were processed by convoy.
    :param config: configuration for auctions_mapping
    :type config: dict
    :param check: run doctest if set to True
    :type check: bool
    :return: auctions_mapping instance
    :rtype: AuctionsMapping
    """

    db = LotMapping(config, logger)
    if check:
        db.put('test', '1')
        assert db.has('test') is True
        assert db.get('test') == '1'
        db.delete('test')
        assert db.has('test') is False
    return db