# -*- coding: utf-8 -*-
import os
from copy import deepcopy
from json import load

import pytest

from openregistry.concierge.tests.conftest import TEST_CONFIG
from openregistry.concierge.worker import BotWorker

ROOT = os.path.dirname(__file__) + '/data/'


def test_concierge_init(db, logger, mocker):
    mocker.patch('openregistry.concierge.utils.LotsClient', autospec=True)
    mocker.patch('openregistry.concierge.utils.AssetsClient', autospec=True)
    mocker.patch('openregistry.concierge.utils.AuctionsClient', autospec=True)
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
    mock_get_lot = mocker.patch.object(bot, 'get_lot', autospec=True)
    mock_process_basic = mocker.MagicMock()
    mock_process_loki = mocker.MagicMock()
    bot.lot_type_processing_configurator = {}
    bot.lot_type_processing_configurator['basic'] = mock_process_basic
    bot.lot_type_processing_configurator['loki'] = mock_process_loki
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)
    for lot in lots:
        lot['data']['rev'] = '123'
    mock_get_lot.return_value = (lot['data'] for lot in lots)

    mocker.patch('openregistry.concierge.worker.IS_BOT_WORKING', almost_always_true(3))

    if bot.errors_doc.get(lots[0]['data']['id'], None):
        del bot.errors_doc[lots[0]['data']['id']]
    if bot.errors_doc.get(lots[1]['data']['id'], None):
        del bot.errors_doc[lots[1]['data']['id']]
    if bot.errors_doc.get(lots[2]['data']['id'], None):
        del bot.errors_doc[lots[2]['data']['id']]
    if bot.errors_doc.get(lots[3]['data']['id'], None):
        del bot.errors_doc[lots[3]['data']['id']]
    if bot.errors_doc.get(lots[4]['data']['id'], None):
        del bot.errors_doc[lots[4]['data']['id']]
    if bot.errors_doc.get(lots[5]['data']['id'], None):
        del bot.errors_doc[lots[5]['data']['id']]
    # if bot.errors_doc.get(lots[6]['data']['id'], None):
    #     del bot.errors_doc[lots[6]['data']['id']]
    bot.db.save(bot.errors_doc)

    bot.run()

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Starting worker"

    assert mock_get_lot.call_count is 3
    assert mock_process_basic.process_lots.call_count == 5
    assert mock_process_loki.process_lots.call_count == 2

    assert mock_process_basic.process_lots.call_args_list[0][0][0] == lots[0]['data']
    assert mock_process_basic.process_lots.call_args_list[1][0][0] == lots[1]['data']
    assert mock_process_basic.process_lots.call_args_list[2][0][0] == lots[2]['data']
    assert mock_process_basic.process_lots.call_args_list[3][0][0] == lots[3]['data']
    assert mock_process_basic.process_lots.call_args_list[4][0][0] == lots[4]['data']
    assert mock_process_loki.process_lots.call_args_list[0][0][0] == lots[5]['data']
    assert mock_process_loki.process_lots.call_args_list[1][0][0] == lots[6]['data']

    error_lots = deepcopy(lots)
    error_lots[1]['data']['rev'] = '234'
    for lot in error_lots:
        bot.errors_doc[lot['data']['id']] = lot['data']
    bot.db.save(bot.errors_doc)

    mocker.patch('openregistry.concierge.worker.IS_BOT_WORKING', almost_always_true(2))
    mock_get_lot.return_value = (lot['data'] for lot in lots)

    bot.run()
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == "Starting worker"

    assert mock_get_lot.call_count is 5
    assert mock_process_basic.process_lots.call_count == 6
    assert mock_process_loki.process_lots.call_count == 2

    assert mock_process_basic.process_lots.call_args_list[5][0][0] == error_lots[1]['data']

    mocker.patch('openregistry.concierge.worker.IS_BOT_WORKING', almost_always_true(1))
    not_recognized_lot = lots[0]
    not_recognized_lot['data']['lotType'] = 'wrong'
    mock_get_lot.return_value = (lot['data'] for lot in [not_recognized_lot])
    bot.run()
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[2] == "Starting worker"
    assert log_strings[3] == 'Such lotType %s is not supported by this concierge configuration' % not_recognized_lot['data']['lotType']

    assert mock_get_lot.call_count is 6

    assert mock_process_basic.process_lots.call_count == 6
    assert mock_process_loki.process_lots.call_count == 2


def test_process_single_lot(bot, logger, mocker):
    mock_lots_client = bot.lots_client
    mock_process_lot = mocker.patch.object(bot, 'process_lot', autospec=True)
    lot_id = '1' * 32

    lot = {'id': lot_id, 'status': 'draft'}
    mock_lots_client.get_lot.side_effect = iter([
        lot
    ])
    bot.process_single_lot(lot_id)

    assert mock_lots_client.get_lot.call_count == 1
    mock_lots_client.get_lot.assert_called_with(lot_id)

    assert mock_process_lot.call_count == 1
    mock_process_lot.assert_called_with(lot)
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Received Lot {} in status {}'.format(lot_id, lot['status'])

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
