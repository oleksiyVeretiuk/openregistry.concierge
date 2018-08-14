# -*- coding: utf-8 -*-
import unittest
import mock

from lazydb import Db as LazyDB

from openregistry.concierge.mapping import LotMapping
from openregistry.concierge.mapping_types import MappingConfigurationException


class TestMappingSuite(unittest.TestCase):

    def tearDown(self):
        test_mapping_name = 'lots_mapping'
        LazyDB(test_mapping_name).destroy(test_mapping_name)

    def test_lots_mapping_with_wrong_type(self):
        config = {'type': 'wrong'}
        mocked_logger = mock.MagicMock()

        with self.assertRaises(MappingConfigurationException) as exc:
            LotMapping(config, mocked_logger)
            self.assertEqual(exc.message, 'Only `void`, `redis` and `lazy` types are available')
            self.assertIs(exc, MappingConfigurationException)

    def test_lots_mapping_succeed_init(self):
        config = {'type': 'void'}
        mocked_db = mock.MagicMock()

        mocked_logger = mock.MagicMock()

        mapping = LotMapping(config, mocked_logger)
        mapping.db = mocked_db

        mapping.get('test')
        mocked_db.get.assert_called_with('test')

        mapping.put('test', 'value')
        mocked_db.set_value.assert_called_with('test', 'value')

        mapping.has('test')
        mocked_db.has.assert_called_with('test')

        mapping.delete('test')
        mocked_db.delete.assert_called_with('test')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestMappingSuite))
    return suite