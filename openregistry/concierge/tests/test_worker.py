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
    mocker.patch('openregistry.concierge.worker.ProcessingLoki', autospec=True)
    mocker.patch('openregistry.concierge.worker.ProcessingBasic', autospec=True)
    BotWorker(TEST_CONFIG)
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'lots_client - ok'
    assert log_strings[1] == 'assets_client - ok'
    assert log_strings[2] == 'couchdb - ok'


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
    bot.process_basic = mock_process_basic
    bot.process_loki = mock_process_loki
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
    assert log_strings[0] == "Starting worker"

    assert mock_get_lot.call_count is 5
    assert mock_process_basic.process_lots.call_count == 6

    assert mock_process_basic.process_lots.call_args_list[5][0][0] == error_lots[1]['data']
