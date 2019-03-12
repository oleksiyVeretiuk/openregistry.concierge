# -*- coding: utf-8 -*-
import unittest
import mock

from openregistry.concierge.mapping_types import (
    LazyDBMapping,
    RedisMapping,
    VoidMapping,
    MappingConfigurationException
)


class TestRedisDB(unittest.TestCase):

    def setUp(self):
        self.patch_strict_redis = mock.patch('openregistry.concierge.mapping_types.StrictRedis')
        self.mocked_redis_class = self.patch_strict_redis.start()
        self.mocked_redis = mock.MagicMock()
        self.mocked_redis_class.return_value = self.mocked_redis

        mock_logger = mock.MagicMock()

        config = {
            'type': 'redis',
            'host': '127.0.0.1',
            'port': 6379,
            'name': 0,
            'password': 'test',
        }
        self.mapping = RedisMapping(config, mock_logger)

    def tearDown(self):
        self.patch_strict_redis.stop()

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

    def test_check_redis_connection(self):
        self.mapping._check_redis_connection()

        self.mocked_redis.set.assert_called_with('check', 'check')
        self.mocked_redis.delete.assert_called_with('check')

    def test_put(self):
        key = 'key'
        value = True

        self.mapping.put(key, value)
        self.mocked_redis.set.assert_called_with(
            key,
            1,
            ex=self.mapping.expire_time
        )

    def test_get(self):
        key = 'key'
        value = 1

        self.mocked_redis.get.return_value = value

        result = self.mapping.get(key)
        self.assertIs(result, True)
        self.mocked_redis.get.assert_called_with(key)

    def test_has(self):
        key = 'key'
        value = True

        self.mocked_redis.exists.return_value = value

        result = self.mapping.has(key)
        self.assertIs(result, value)
        self.mocked_redis.exists.assert_called_with(key)

    def test_delete(self):
        key = 'key'

        self.mapping.delete(key)
        self.mocked_redis.delete.assert_called_with(key)

    def test_is_empty_true(self):
        self.mocked_redis.scan_iter.return_value = ['some']

        result = self.mapping.is_empty()
        self.assertIs(result, True)
        self.mocked_redis.scan_iter.assert_called_with(count=1)

    def test_is_empty_false(self):
        self.mocked_redis.scan_iter.return_value = []

        result = self.mapping.is_empty()
        self.assertIs(result, False)
        self.mocked_redis.scan_iter.assert_called_with(count=1)

    def test_clean(self):
        self.mapping.clean()
        self.assertEqual(self.mocked_redis.flushdb.call_count, 1)

    def test_validate_config_without_host(self):
        config = {}

        with self.assertRaises(MappingConfigurationException) as exp:
            self.mapping.validate_config(config)
            self.assertEqual(
                exp.message,
                'host should be present for redis type mapping'
            )

    def test_validate_config_without_port(self):
        config = {'host': 'some'}

        with self.assertRaises(MappingConfigurationException) as exp:
            self.mapping.validate_config(config)
            self.assertEqual(
                exp.message,
                'port should be present for redis type mapping'
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