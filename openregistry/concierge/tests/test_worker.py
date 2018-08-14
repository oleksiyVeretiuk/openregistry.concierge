# -*- coding: utf-8 -*-
import os
from copy import deepcopy
from json import load
from lazydb import Db

import pytest

from openregistry.concierge.tests.conftest import TEST_CONFIG
from openregistry.concierge.worker import BotWorker

ROOT = os.path.dirname(__file__) + '/data/'


def test_concierge_init(db, logger, mocker):
    mocker.patch('openregistry.concierge.utils.LotsClient', autospec=True)
    mocker.patch('openregistry.concierge.utils.AssetsClient', autospec=True)
    mocker.patch('openregistry.concierge.utils.AuctionsClient', autospec=True)
    mocker.patch('openregistry.concierge.utils.prepare_lot_mapping', autospec=True)
    processing_loki = mocker.patch('openregistry.concierge.worker.ProcessingLoki', autospec=True)
    processing_loki.get_condition.return_value = 'condition_loki'
    processing_loki = processing_loki.return_value
    processing_loki.handled_lot_types = ['loki']
    processing_basic = mocker.patch('openregistry.concierge.worker.ProcessingBasic', autospec=True)
    processing_basic.get_condition.return_value = 'condition_basic'
    processing_basic = processing_basic.return_value
    processing_basic.handled_lot_types = ['basic']
    BotWorker(TEST_CONFIG)
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'auction_client - ok'
    assert log_strings[1] == 'lots_client - ok'
    assert log_strings[2] == 'assets_client - ok'
    assert log_strings[3] == 'couchdb - ok'
    assert log_strings[4] == 'lots_mapping - ok'


def test_get_lot(bot, logger, mocker):
    mock_continuous_changes_feed = mocker.patch('openregistry.concierge.worker.continuous_changes_feed', autospec=True)
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)
    mock_continuous_changes_feed.return_value = (lot['data'] for lot in lots)

    result = bot.get_lot()

    assert 'next' and '__iter__' in dir(result)  # assert generator object is returned

    assert result.next() == lots[0]['data']
    assert result.next() == lots[1]['data']
    assert result.next() == lots[2]['data']
    assert result.next() == lots[3]['data']
    assert result.next() == lots[4]['data']
    assert result.next() == lots[5]['data']
    assert result.next() == lots[6]['data']

    with pytest.raises(StopIteration):
        result.next()

    log_strings = logger.log_capture_string.getvalue().split('\n')

    assert log_strings[0] == 'Getting Lots'


def test_run(bot, logger, mocker, almost_always_true):
    if not bot.lots_mapping.db.db.is_empty():
        Db('lots_mapping').destroy('lots_mapping')

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    mock_get_lot = mocker.patch.object(bot, 'get_lot', autospec=True)
    mock_process_lot = mocker.patch.object(bot, 'process_lot', autospec=True)

    mocker.patch('openregistry.concierge.worker.IS_BOT_WORKING', almost_always_true(3))
    mock_get_lot.return_value = (lot['data'] for lot in lots[:3])

    bot.run()

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Starting worker"

    assert mock_get_lot.call_count == 3
    assert mock_process_lot.call_count == 3
    mock_process_lot.assert_called_with(lots[2]['data'])


    bot.lots_mapping.put(str(lots[0]['data']['id']), True)
    mocker.patch('openregistry.concierge.worker.IS_BOT_WORKING', almost_always_true(3))
    mock_get_lot.return_value = (lot['data'] for lot in lots[3:5])

    bot.run()

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == "Starting worker"

    assert mock_get_lot.call_count == 6
    assert mock_process_lot.call_count == 5


def test_process_lot(bot, logger, mocker, almost_always_true):
    mock_resolve_broken_lot = mocker.patch('openregistry.concierge.worker.resolve_broken_lot', autospec=True)
    mock_process_basic = mocker.MagicMock()
    mock_process_loki = mocker.MagicMock()
    bot.lot_type_processing_configurator = {}
    bot.lot_type_processing_configurator['basic'] = mock_process_basic
    bot.lot_type_processing_configurator['loki'] = mock_process_loki
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = deepcopy(lots[0]['data'])

    # Test aliase for lots
    lot['lotType'] = 'basic'
    bot.process_lot(lot)
    assert mock_process_basic.process_lots.call_count == 1
    mock_process_basic.process_lots.assert_called_with(lot)
    assert mock_process_loki.process_lots.call_count == 0

    lot['lotType'] = 'loki'
    bot.process_lot(lot)
    assert mock_process_basic.process_lots.call_count == 1

    assert mock_process_loki.process_lots.call_count == 1
    mock_process_loki.process_lots.assert_called_with(lot)

    # Test if lotType not registered in aliase
    lot['lotType'] = 'wrong'
    bot.process_lot(lot)
    assert mock_process_basic.process_lots.call_count == 1
    assert mock_process_loki.process_lots.call_count == 1

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Such lotType %s is not supported by this concierge configuration' % lot['lotType']

    # Work with broken lot
    broken_lot_with_different_rev = deepcopy(lots[0]['data'])
    broken_lot_with_different_rev['rev'] = '234'
    broken_lot_with_different_rev['lotType'] = 'basic'

    broken_lot = deepcopy(lots[1]['data'])
    broken_lot['lotType'] = 'basic'
    broken_lot['rev'] = '123'

    bot.errors_doc[lot['id']] = lot
    # Add lots to bot.errors_doc and set revisions to compare
    for lot in [broken_lot, broken_lot_with_different_rev]:
        bot.errors_doc[lot['id']] = deepcopy(lot)
        bot.errors_doc[lot['id']]['rev'] = '123'
    bot.db.save(bot.errors_doc)

    # Test broken lot with same rev
    bot.process_lot(broken_lot)
    assert mock_process_basic.process_lots.call_count == 1
    assert mock_process_loki.process_lots.call_count == 1

    assert mock_resolve_broken_lot.call_count == 0

    # Test broken lot with different rev
    bot.process_lot(broken_lot_with_different_rev)
    assert mock_process_basic.process_lots.call_count == 2
    assert mock_process_loki.process_lots.call_count == 1

    assert mock_resolve_broken_lot.call_count == 1


def test_process_single_lot(bot, logger, mocker):
    mock_lots_client = bot.lots_client
    mock_process_lot = mocker.patch.object(bot, 'process_lot', autospec=True)
    lot_id = '1' * 32

    lot = {'data': {'id': lot_id, 'status': 'draft'}}
    mock_lots_client.get_lot.side_effect = iter([
        lot
    ])
    bot.process_single_lot(lot_id)

    assert mock_lots_client.get_lot.call_count == 1
    mock_lots_client.get_lot.assert_called_with(lot_id)

    assert mock_process_lot.call_count == 1
    mock_process_lot.assert_called_with(lot['data'])
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Received Lot {} in status {}'.format(lot_id, lot['data']['status'])

    # When lot doesn't exist
    mock_lots_client.get_lot.side_effect = iter([
        None
    ])
    bot.process_single_lot(lot_id)

    assert mock_lots_client.get_lot.call_count == 2
    mock_lots_client.get_lot.assert_called_with(lot_id)

    assert mock_process_lot.call_count == 1
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == 'Lot with id {} not found in API'.format(lot_id)
