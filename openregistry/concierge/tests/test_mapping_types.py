# -*- coding: utf-8 -*-
import unittest
import mock

from openregistry.concierge.mapping_types import (
    LazyDBMapping,
    RedisMapping,
    VoidMapping
)


class TestRedisDB(unittest.TestCase):

    @mock.patch('openregistry.concierge.mapping_types.StrictRedis')
    def test_lots_mapping_redis(self, mock_redis):
        mock_logger = mock.MagicMock()

        config = {
            'type': 'redis',
            'host': '127.0.0.1',
            'port': 6379,
            'name': 0,
            'password': 'test',
        }
        RedisMapping(config, mock_logger)

        mock_redis.assert_called_once_with(
            host=config['host'],
            port=config['port'],
            db=config['name'],
            password=config['password']
        )
        mock_logger.info.assert_called_once_with(
            'Set redis store "{name}" at {host}:{port} as lots mapping'.format(
                **config
            )
        )


class TestLazyDB(unittest.TestCase):

    @mock.patch('openregistry.concierge.mapping_types.LazyDB')
    def test_lots_mapping_lazydb(self, mock_lazy_db):
        mock_logger = mock.MagicMock()

        config = {
            'type': 'lazy',
            'name': 'test',
        }
        LazyDBMapping(config, mock_logger)

        mock_lazy_db.assert_called_once_with(
            config['name']
        )
        mock_logger.info.assert_called_once_with(
            'Set lazydb "{name}" as lots mapping'.format(**config)
        )


class TestVoidDB(unittest.TestCase):
    def test_lots_mapping_void(self):
        mock_logger = mock.MagicMock()

        config = {
            'name': 'test',
            'type': 'void',
        }
        mapping = VoidMapping(config, mock_logger)
        mock_logger.info.assert_called_with(
            'Set void mapping. Caching is disabled'
        )

        self.assertEqual(mapping.get('some_wrong_id'), None)

        self.assertEqual(mapping.put('some_wrong_id', 'some_value'), None)

        self.assertEqual(mapping.has('some_wrong_id'), False)

        self.assertEqual(mapping.delete('some_wrong_id'), None)

        self.assertEqual(mapping.is_empty(), True)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestRedisDB))
    suite.addTest(unittest.makeSuite(TestLazyDB))
    suite.addTest(unittest.makeSuite(TestVoidDB))
    return suite