# -*- coding: utf-8 -*-
import unittest
import mock

from lazydb import Db as LazyDB

from openregistry.concierge.mapping import prepare_lot_mapping
from openregistry.concierge.mapping_types import MappingConfigurationException, MAPPING_TYPES


class TestMappingSuite(unittest.TestCase):

    def tearDown(self):
        test_mapping_name = 'lots_mapping'
        LazyDB(test_mapping_name).destroy(test_mapping_name)

    def test_lots_mapping_with_wrong_type(self):
        config = {'type': 'wrong'}
        mocked_logger = mock.MagicMock()

        with self.assertRaises(MappingConfigurationException) as exc:
            prepare_lot_mapping(config, mocked_logger)
            self.assertEqual(exc.message, 'Only `void`, `redis` and `lazy` types are available')
            self.assertIs(exc, MappingConfigurationException)

    def test_lots_mapping_succeed_init(self):
        config = {'type': 'void'}

        mocked_logger = mock.MagicMock()

        mapping = prepare_lot_mapping(config, mocked_logger)
        self.assertIsInstance(mapping, MAPPING_TYPES[config['type']])


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestMappingSuite))
    return suite