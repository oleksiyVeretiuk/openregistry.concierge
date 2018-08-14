# -*- coding: utf-8 -*-
from openregistry.concierge.mapping_types import (
    MAPPING_TYPES,
    MappingConfigurationException
)


class LotMapping(object):
    """Mapping for processed lots"""
    type = ''

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        mapping_class = MAPPING_TYPES.get(self.config['type'], None)

        if mapping_class is None:
            raise MappingConfigurationException('Only `void`, `redis` and `lazy` types are available')

        self.db = mapping_class(config, self.logger)

    def get(self, key):
        return self.db.get(key)

    def put(self, key, value):
        self.db.set_value(key, value)

    def has(self, key):
        return self.db.has(key)

    def delete(self, key):
        self.db.delete(key)


def prepare_lot_mapping(config, logger, check=False):
    """
    Initialization of lots_mapping, which are used for tracking lots,
    which already were processed by convoy.
    :param config: configuration for lots_mapping
    :type config: dict
    :param check: run doctest if set to True
    :type check: bool
    :return: lots_mapping instance
    :rtype: LotsMapping
    """

    db = LotMapping(config, logger)
    if check and config.get('type', 'void') != 'void':
        db.put('test', '1')
        assert db.has('test') is True
        assert db.get('test') == '1'
        db.delete('test')
        assert db.has('test') is False
    return db