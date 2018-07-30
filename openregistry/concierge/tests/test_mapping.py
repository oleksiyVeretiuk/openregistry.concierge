# -*- coding: utf-8 -*-
import unittest
import mock

from lazydb import Db as LazyDB

from openregistry.concierge.mapping import LotMapping


class TestUtilsSuite(unittest.TestCase):

    def tearDown(self):
        test_mapping_name = 'lots_mapping'
        LazyDB(test_mapping_name).destroy(test_mapping_name)

    @mock.patch('openregistry.concierge.mapping.StrictRedis')
    def test_lots_mapping_redis(self, mock_redis):
        mock_logger = mock.MagicMock()

        config = {
            'host': '127.0.0.1',
            'port': 6379,
            'name': 0,
            'password': 'test'
        }
        LotMapping(config, mock_logger)

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

    @mock.patch('openregistry.concierge.mapping.LazyDB')
    def test_lots_mapping_lazydb(self, mock_lazy_db):
        mock_logger = mock.MagicMock()

        config = {
            'name': 'test'
        }
        LotMapping(config, mock_logger)

        mock_lazy_db.assert_called_once_with(
            config['name']
        )
        mock_logger.info.assert_called_once_with(
            'Set lazydb "{name}" as lots mapping'.format(**config)
        )


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestUtilsSuite))
    return suite