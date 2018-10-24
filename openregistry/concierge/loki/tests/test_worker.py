# -*- coding: utf-8 -*-
import os
from copy import deepcopy
from datetime import datetime, timedelta
from json import load

import pytest
from isodate import parse_duration
from munch import munchify

from openregistry.concierge.constants import TZ
from openregistry.concierge.loki.tests.conftest import TEST_CONFIG
from openregistry.concierge.loki.processing import logger as LOGGER
from openregistry.concierge.loki.utils import calculate_business_date
from openregistry.concierge.loki.processing import ProcessingLoki, HANDLED_STATUSES
from openprocurement_client.exceptions import (
    Forbidden,
    ResourceNotFound,
    RequestFailed,
    UnprocessableEntity
)
from openregistry.concierge.loki.constants import KEYS_FOR_LOKI_PATCH, KEYS_FOR_AUCTION_CREATE

ROOT = os.path.dirname(__file__) + '/data/'


def test_processing_loki_init(db, logger, mocker):
    lots_client = mocker.patch('openregistry.concierge.utils.LotsClient', autospec=True).return_value
    assets_client = mocker.patch('openregistry.concierge.utils.AssetsClient', autospec=True).return_value
    mock_create_condition = mocker.patch('openregistry.concierge.loki.processing.create_filter_condition', autospec=True)
    mock_create_condition.return_value = 'condition'

    clients = {'lots_client': lots_client, 'assets_client': assets_client, 'db': db}
    errors_doc = db.get(TEST_CONFIG['errors_doc'])
    processing = ProcessingLoki(TEST_CONFIG['lots']['loki'], clients, errors_doc)
    assert set(processing.allowed_asset_types) == {'bounce', 'domain'}
    assert set(processing.handled_lot_types) == {'loki'}

    assert processing.get_condition(TEST_CONFIG['lots']['loki']) == 'condition'
    assert mock_create_condition.call_count == 1
    mock_create_condition.assert_called_with(TEST_CONFIG['lots']['loki']['aliases'], HANDLED_STATUSES)


def test_patch_lot(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)
    mock_patch_lot = mocker.MagicMock()
    test_lot = deepcopy(lots[0])
    test_lot['data']['status'] = 'active.salable'
    mock_patch_lot.side_effect = [
        munchify(test_lot),
        Forbidden(response=munchify({"text": "Operation is forbidden."})),
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        UnprocessableEntity(response=munchify({"text": "Unprocessable Entity."}))
    ]
    bot.lots_client.patch_lot = mock_patch_lot
    lot = lots[0]['data']
    status = 'active.salable'

    result = bot.patch_lot(lot=lot, status=status)
    assert result is True

    result = bot.patch_lot(lot=lot, status=status)
    assert result is False

    result = bot.patch_lot(lot=lot, status=status)
    assert result is False

    result = bot.patch_lot(lot=lot, status=status)
    assert result is False

    assert bot.lots_client.patch_lot.call_count == 4

    log_strings = logger.log_capture_string.getvalue().split('\n')

    assert log_strings[0] == 'Successfully patched Lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable'
    assert log_strings[1] == 'Failed to patch Lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Operation is forbidden.)'
    assert log_strings[2] == 'Failed to patch Lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Server error: 502)'
    assert log_strings[3] == 'Failed to patch Lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Unprocessable Entity.)'


def test_patch_assets_pending_success(bot, logger, mocker):
    mock_patch_asset = mocker.MagicMock()
    bot.assets_client.patch_asset = mock_patch_asset

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[1]['data']
    status = 'pending'

    mock_patch_asset.side_effect = [
        munchify(assets[4]),
    ]

    result, patched_assets = bot.patch_assets(lot=lot, status=status)
    assert result is True
    assert patched_assets == [
        '8034c43e2d764006ad6e655e339e5fec',
    ]

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset 8034c43e2d764006ad6e655e339e5fec to pending'

    assert bot.assets_client.patch_asset.call_count == 1


def test_patch_assets_pending_fail(bot, logger, mocker):
    mock_patch_asset = mocker.MagicMock()
    bot.assets_client.patch_asset = mock_patch_asset

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[1]['data']
    status = 'pending'

    mock_patch_asset.side_effect = [
        munchify(assets[4]),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
    ]

    result, patched_assets = bot.patch_assets(lot=lot, status=status)
    assert result is True
    assert patched_assets == ['8034c43e2d764006ad6e655e339e5fec']

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset 8034c43e2d764006ad6e655e339e5fec to pending'

    result, patched_assets = bot.patch_assets(lot=lot, status=status)
    assert result is False
    assert patched_assets == []

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == 'Failed to patch asset 8034c43e2d764006ad6e655e339e5fec to pending (Server error: 502)'


    assert bot.assets_client.patch_asset.call_count == 6

    mock_patch_asset.side_effect = [
        Forbidden(response=munchify({"text": "Operation is forbidden."})),
        munchify(assets[4]),
    ]

    result, patched_assets = bot.patch_assets(lot=lot, status=status)
    assert result is False
    assert patched_assets == []
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[2] == 'Failed to patch asset 8034c43e2d764006ad6e655e339e5fec to pending (Operation is forbidden.)'

    result, patched_assets = bot.patch_assets(lot=lot, status=status)
    assert result is True
    assert patched_assets == ['8034c43e2d764006ad6e655e339e5fec']
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[3] == 'Successfully patched asset 8034c43e2d764006ad6e655e339e5fec to pending'

    assert bot.assets_client.patch_asset.call_count == 8


def test_patch_assets_verification_success(bot, logger, mocker):
    mock_patch_asset = mocker.MagicMock()
    bot.assets_client.patch_asset = mock_patch_asset

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[0]['data']
    status = 'verification'

    mock_patch_asset.side_effect = [
        munchify(assets[0])
    ]

    result, patched_assets = bot.patch_assets(lot=lot, status=status)
    assert result is True
    assert patched_assets == [
        'e519404fd0b94305b3b19ec60add05e7'
    ]

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset e519404fd0b94305b3b19ec60add05e7 to verification'
    assert bot.assets_client.patch_asset.call_count == 1


def test_patch_assets_active_fail(bot, logger, mocker):
    mock_patch_asset = mocker.MagicMock()
    bot.assets_client.patch_asset = mock_patch_asset

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[0]['data']
    status = 'verification'

    mock_patch_asset.side_effect = [
        munchify(assets[0]),
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
    ]

    result, patched_assets = bot.patch_assets(lot=lot, status=status)
    assert result is True
    assert patched_assets == ['e519404fd0b94305b3b19ec60add05e7']

    result, patched_assets = bot.patch_assets(lot=lot, status=status)
    assert result is False
    assert patched_assets == []


    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset e519404fd0b94305b3b19ec60add05e7 to verification'
    assert log_strings[1] == 'Failed to patch asset e519404fd0b94305b3b19ec60add05e7 to verification (Server error: 502)'

    assert bot.assets_client.patch_asset.call_count == 6


def test_process_lots(bot, logger, mocker):
    mock_get_asset = mocker.MagicMock()
    mock_mapping = bot.lots_mapping
    bot.assets_client.get_asset = mock_get_asset

    bot.add_related_process_to_assets = mocker.MagicMock()
    bot.add_related_process_to_assets.return_value = (True, [])
    bot.clean_related_processes = mocker.MagicMock()

    mock_log_broken_lot = mocker.patch('openregistry.concierge.loki.processing.log_broken_lot', autospec=True)
    mock_patch_related_processes = mocker.patch.object(bot, '_patch_lot_asset_related_processes', autospec=True)
    mock_patch_related_processes.return_value = (True, ['all_rPs'])

    mock_check_lot = mocker.patch.object(bot, 'check_lot', autospec=True)
    mock_patch_auction = mocker.patch.object(bot, '_patch_auction', autospec=True)

    mock_check_assets = mocker.patch.object(bot, 'check_assets', autospec=True)

    mock_patch_assets = mocker.patch.object(bot, 'patch_assets', autospec=True)

    mock_patch_lot = mocker.patch.object(bot, 'patch_lot', autospec=True)
    mock_patch_lot.return_value = True

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)


    verification_lot = lots[0]['data']
    pending_dissolution_lot = lots[1]['data']

    # status == 'verification'
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (False, []),
        (True, ['all_assets']),
    ])
    bot.process_lots(verification_lot)  # assets_available: True; patch_assets: [(False, []), (True, []]; check_lot: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

    assert mock_check_assets.call_count == 1
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_patch_related_processes.call_count == 0

    assert mock_check_lot.call_count == 1
    assert mock_check_lot.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 1
    assert mock_patch_assets.call_args_list[0][0] == (verification_lot, 'verification')

    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
        (True, ['all_assets']),
    ])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])

    to_compare = {l_key: assets[9]['data'].get(a_key, None) for a_key, l_key in KEYS_FOR_LOKI_PATCH.items()}
    asset_decision = deepcopy(assets[9]['data']['decisions'][0])
    asset_decision['relatedItem'] = assets[9]['data']['id']
    to_compare['decisions'] = [
        verification_lot['decisions'][0],
        asset_decision
    ]

    bot.process_lots(verification_lot)  # assets_available: True; patch_assets: [(True, []), (True, [])]; check_lot: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

    assert mock_patch_related_processes.call_count == 1

    assert mock_check_assets.call_count == 2
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_check_lot.call_count == 2
    assert mock_check_lot.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 3
    assert mock_patch_assets.call_args_list[1][0] == (verification_lot, 'verification')
    assert mock_patch_assets.call_args_list[2][0] == (verification_lot, 'active')

    assert mock_patch_lot.call_count == 1
    assert mock_patch_lot.call_args[0] == (verification_lot, 'pending', to_compare)

    assert mock_mapping.put.call_count == 1
    mock_mapping.put.assert_called_with(verification_lot['id'], True)

    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        False
    ])
    mock_patch_assets.side_effect = iter([
    ])
    bot.process_lots(verification_lot)  # assets_available: False; patch_assets: None; check_lot: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[2] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

    assert mock_check_assets.call_count == 3
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_check_lot.call_count == 3
    assert mock_check_lot.call_args[0] == (verification_lot,)

    assert mock_patch_lot.call_count == 2
    assert mock_patch_lot.call_args[0] == (verification_lot, 'invalid')

    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        RequestFailed(response=munchify({"text": "Request failed."}))
    ])
    mock_patch_assets.side_effect = iter([
    ])
    bot.process_lots(verification_lot)  # assets_available: raises exception; patch_assets: None; check_lot: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[3] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'
    assert log_strings[4] == 'Due to fail in getting assets, Lot 9ee8f769438e403ebfb17b2240aedcf1 is skipped'

    assert mock_check_assets.call_count == 4
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_patch_lot.call_count == 2

    assert mock_check_lot.call_count == 4
    assert mock_check_lot.call_args[0] == (verification_lot,)

    # when can`t patch lot to pending
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        (True, ['all_assets'])
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
        (True, ['all_assets']),
        (True, ['all_assets']),
    ])
    mock_patch_lot.side_effect = iter([False, True])
    bot.process_lots(verification_lot)

    assert mock_patch_related_processes.call_count == 3
    mock_patch_related_processes.assert_called_with(verification_lot, cleanup=True)

    assert mock_check_assets.call_count == 5
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_patch_lot.call_count == 4
    mock_patch_lot.assert_called_with(verification_lot, 'composing')

    assert mock_check_lot.call_count == 5
    assert mock_check_lot.call_args[0] == (verification_lot,)
    mock_patch_lot.side_effect = None

    log_strings = logger.log_capture_string.getvalue().split('\n')
    lot_assets = [rP['relatedProcessID'] for rP in verification_lot['relatedProcesses'] if rP['type'] == 'asset']
    assert log_strings[6] == "Assets {} from Lot {} will be patched to '{}'".format(lot_assets, verification_lot['id'], 'pending')


    # status == 'pending.dissolution'
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        None
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets'])
    ])
    bot.process_lots(pending_dissolution_lot)  # assets_available: None; patch_assets: (True, []); check_lot: True

    lot_assets = [rP['relatedProcessID'] for rP in pending_dissolution_lot['relatedProcesses'] if rP['type'] == 'asset']
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[7] == 'Processing Lot b844573afaa24e4fb098f3027e605c87 in status pending.dissolution'
    assert log_strings[8] == "Assets {} from Lot {} will be patched to 'pending'".format(lot_assets,
                                                                                         pending_dissolution_lot['id'])

    assert mock_patch_lot.call_count == 5
    assert mock_patch_lot.call_args[0] == (pending_dissolution_lot, 'dissolved')

    assert mock_mapping.put.call_count == 2
    mock_mapping.put.assert_called_with(pending_dissolution_lot['id'], True)

    assert mock_check_lot.call_count == 6
    assert mock_check_lot.call_args[0] == (pending_dissolution_lot,)

    # Lot is not available
    mock_check_lot.side_effect = iter([
        False
    ])
    mock_check_assets.side_effect = iter([
    ])
    mock_patch_assets.side_effect = iter([
    ])
    bot.process_lots(pending_dissolution_lot)  # assets_available: None; patch_assets: None; check_lot: False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[9] == 'Skipping Lot {}'.format(pending_dissolution_lot['id'])

    assert mock_patch_lot.call_count == 5

    assert mock_check_lot.call_count == 7
    assert mock_check_lot.call_args[0] == (pending_dissolution_lot,)

    # Pending dissolution
    pending_dissolution_lot = lots[2]['data']
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        False
    ])
    mock_patch_assets.side_effect = iter([
        (False, []),
        (True, ['all_assets'])
    ])
    bot.process_lots(pending_dissolution_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    lot_assets = [rP['relatedProcessID'] for rP in pending_dissolution_lot['relatedProcesses'] if rP['type'] == 'asset']
    assert log_strings[11] == 'Not valid assets {} in Lot {}'.format(lot_assets, pending_dissolution_lot['id'])
    assert mock_check_lot.call_count == 8
    assert mock_check_lot.call_args[0] == (pending_dissolution_lot,)

    assert mock_patch_lot.call_count == 6

    assert mock_mapping.put.call_count == 3
    mock_mapping.put.assert_called_with(pending_dissolution_lot['id'], True)

    assert mock_check_assets.call_count == 5
    assert mock_patch_assets.call_args[0] == (pending_dissolution_lot, 'pending')

    # Pending sold lot
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
    ])
    pending_sold_lot = lots[4]['data']


    bot.process_lots(pending_sold_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    lot_assets = [rP['relatedProcessID'] for rP in pending_sold_lot['relatedProcesses'] if rP['type'] == 'asset']
    assert log_strings[12] == 'Processing Lot {} in status pending.sold'.format(pending_sold_lot['id'])
    assert log_strings[13] == "Assets {} from Lot {} will be patched to 'complete'".format(lot_assets,
                                                                                           pending_sold_lot['id'])
    assert mock_check_lot.call_count == 9
    assert mock_check_lot.call_args[0] == (pending_sold_lot,)

    assert mock_patch_lot.call_count == 7
    assert mock_patch_lot.call_args[0] == (pending_sold_lot, 'sold')

    assert mock_mapping.put.call_count == 4
    mock_mapping.put.assert_called_with(pending_sold_lot['id'], True)

    assert mock_check_assets.call_count == 5
    assert mock_patch_assets.call_args[0] == (pending_sold_lot, 'complete')

    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (False, []),
        (True, ['all_assets']),
    ])
    bot.process_lots(pending_sold_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    lot_assets = [rP['relatedProcessID'] for rP in pending_sold_lot['relatedProcesses'] if rP['type'] == 'asset']
    assert log_strings[14] == 'Processing Lot {} in status pending.sold'.format(pending_sold_lot['id'])
    assert log_strings[15] == 'Not valid assets {} in Lot {}'.format(lot_assets, pending_sold_lot['id'])
    assert mock_check_lot.call_count == 10
    assert mock_check_lot.call_args[0] == (pending_sold_lot,)

    assert mock_mapping.put.call_count == 5
    mock_mapping.put.assert_called_with(pending_sold_lot['id'], True)

    assert mock_patch_lot.call_count == 8
    assert mock_patch_lot.call_args[0] == (pending_sold_lot, 'sold')

    assert mock_check_assets.call_count == 5
    assert mock_patch_assets.call_args[0] == (pending_sold_lot, 'complete')


    # Verification lot
    loki_verfication_lot = lots[5]['data']
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
        (True, ['all_assets']),
    ])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])
    asset_decision = assets[9]['data']['decisions'][0]
    asset_decision['relatedItem'] = assets[9]['data']['id']
    to_compare = {l_key:assets[9]['data'].get(a_key, None) for a_key, l_key in KEYS_FOR_LOKI_PATCH.items()}
    to_compare['decisions'] = [
        loki_verfication_lot['decisions'][0],
        assets[9]['data']['decisions'][0],
    ]
    bot.process_lots(loki_verfication_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[16] == 'Processing Lot {} in status verification'.format(loki_verfication_lot['id'])

    assert mock_patch_related_processes.call_count == 4

    assert mock_check_lot.call_count == 11
    assert mock_check_lot.call_args[0] == (loki_verfication_lot,)

    assert mock_patch_lot.call_count == 9
    assert mock_patch_lot.call_args[0] == (loki_verfication_lot, 'pending', to_compare)

    assert mock_mapping.put.call_count == 6
    mock_mapping.put.assert_called_with(loki_verfication_lot['id'], True)

    assert mock_check_assets.call_count == 6
    assert mock_patch_assets.call_args[0] == (loki_verfication_lot, 'active')

    # When something wrong
    loki_verfication_lot = lots[5]['data']
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        False
    ])
    mock_patch_assets.side_effect = iter([
        (False, [])
    ])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])
    bot.process_lots(loki_verfication_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[17] == 'Processing Lot {} in status verification'.format(loki_verfication_lot['id'])
    assert mock_check_lot.call_count == 12
    assert mock_check_lot.call_args[0] == (loki_verfication_lot,)

    assert mock_patch_lot.call_count == 10
    assert mock_patch_lot.call_args[0] == (loki_verfication_lot, 'invalid')

    assert mock_check_assets.call_count == 7
    assert mock_patch_assets.call_args[0] == (loki_verfication_lot, 'active')


    # Test pending.deleted lot
    pending_deleted_lot = lots[6]['data']
    pending_deleted_lot['assets'] = [assets[9]]
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
        (True, ['all_assets']),
    ])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])
    bot.process_lots(pending_deleted_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[18] == 'Processing Lot {} in status pending.deleted'.format(pending_deleted_lot['id'])
    assert mock_check_lot.call_count == 13
    assert mock_check_lot.call_args[0] == (pending_deleted_lot,)

    assert mock_patch_lot.call_count == 11
    assert mock_patch_lot.call_args[0] == (pending_deleted_lot, 'deleted')

    assert mock_mapping.put.call_count == 7
    mock_mapping.put.assert_called_with(pending_deleted_lot['id'], True)

    assert mock_check_assets.call_count == 7
    assert mock_patch_assets.call_args[0] == (pending_deleted_lot, 'pending')

    # Test active.salable lot
    mock_create_auction = mocker.patch.object(bot, '_create_auction', autospec=True)
    mock_check_previous_auction = mocker.patch.object(bot, 'check_previous_auction', autospec=True)

    active_salable_lot = lots[7]['data']
    active_salable_lot['assets'] = [assets[9]]

    created_auction = munchify({'data': deepcopy(active_salable_lot['auctions'][0])})
    auction_id = 'id_of_auction'
    internal_id = '1' * 32
    created_auction.data.auctionID = auction_id
    lot_auction_id = '2' * 32
    created_auction.data.id = internal_id
    mock_create_auction.return_value = (created_auction, lot_auction_id)

    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])
    bot.process_lots(active_salable_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[20] == 'Processing Lot {} in status active.salable'.format(active_salable_lot['id'])

    assert mock_mapping.put.call_count == 8
    mock_mapping.put.assert_called_with(active_salable_lot['id'], True)

    assert mock_check_lot.call_count == 14
    assert mock_check_lot.call_args[0] == (active_salable_lot,)

    patched_data = {
        'auctionID': created_auction.data.auctionID,
        'status': 'active',
        'relatedProcessID': created_auction.data.id
    }

    assert mock_patch_lot.call_count == 11

    assert mock_patch_auction.call_count == 1
    mock_patch_auction.assert_called_with(
        patched_data,
        active_salable_lot['id'],
        lot_auction_id
    )

    assert mock_create_auction.call_count == 1
    mock_create_auction.assert_called_with(active_salable_lot)

    assert mock_check_assets.call_count == 8
    assert mock_check_assets.call_args[0] == (active_salable_lot, 'active')

    assert mock_check_previous_auction.call_count == 1
    mock_check_previous_auction.assert_called_with(active_salable_lot)

    # Test active.salable Lot when it contain not valid auctions
    active_salable_lot['auctions'][0]['status'] = 'cancelled'
    active_salable_lot = lots[7]['data']
    active_salable_lot['assets'] = [assets[9]]
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])
    bot.process_lots(active_salable_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[21] == 'Processing Lot {} in status active.salable'.format(active_salable_lot['id'])

    assert mock_check_lot.call_count == 15
    assert mock_check_lot.call_args[0] == (active_salable_lot,)

    assert mock_patch_lot.call_count == 11

    assert mock_create_auction.call_count == 1
    mock_create_auction.assert_called_with(active_salable_lot)

    assert mock_check_assets.call_count == 9
    assert mock_check_assets.call_args[0] == (active_salable_lot, 'active')

    assert mock_check_previous_auction.call_count == 1
    mock_check_previous_auction.assert_called_with(active_salable_lot)

    assert mock_mapping.put.call_count == 8

    # Test active.salable Lot when auction not created
    active_salable_lot['auctions'][0]['status'] = 'scheduled'
    active_salable_lot = lots[7]['data']
    active_salable_lot['assets'] = [assets[9]]
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_create_auction.return_value = False
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
    ])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])

    bot.process_lots(active_salable_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[22] == 'Processing Lot {} in status active.salable'.format(active_salable_lot['id'])

    assert mock_check_lot.call_count == 16
    assert mock_check_lot.call_args[0] == (active_salable_lot,)

    assert mock_patch_lot.call_count == 12
    mock_patch_lot.assert_called_with(active_salable_lot, 'composing')

    assert mock_create_auction.call_count == 2
    mock_create_auction.assert_called_with(active_salable_lot)

    mock_patch_assets.assert_called_with(active_salable_lot, 'pending')

    assert mock_check_assets.call_count == 10
    assert mock_check_assets.call_args[0] == (active_salable_lot, 'active')

    assert mock_check_previous_auction.call_count == 2
    mock_check_previous_auction.assert_called_with(active_salable_lot)

    assert mock_mapping.put.call_count == 8

    # Test working _patch_lot_asset_related_processes
    mock_patch_related_processes.return_value = (True, ['all_rPs'])
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
        (True, ['all_assets']),
    ])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])

    bot.process_lots(verification_lot)  # assets_available: True; patch_assets: [(True, []), (True, [])]; check_lot: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

    assert mock_patch_related_processes.call_count == 5
    mock_patch_related_processes.assert_called_with(verification_lot)

    assert mock_check_assets.call_count == 11
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_check_lot.call_count == 17
    assert mock_check_lot.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 16
    mock_patch_assets.assert_called_with(verification_lot, 'active')

    assert mock_patch_lot.call_count == 13
    assert mock_patch_lot.call_args[0] == (verification_lot, 'pending', to_compare)

    assert mock_mapping.put.call_count == 9
    mock_mapping.put.assert_called_with(verification_lot['id'], True)

    # Test when _patch_lot_asset_related_processes can`t patch
    mock_patch_related_processes.return_value = (False, [])
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
        (True, ['all_assets']),
        (True, ['all_assets']),
    ])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])

    bot.process_lots(verification_lot)  # assets_available: True; patch_assets: [(True, []), (True, [])]; check_lot: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

    assert mock_patch_related_processes.call_count == 6
    mock_patch_related_processes.assert_called_with(verification_lot)

    assert mock_check_assets.call_count == 12
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_check_lot.call_count == 18
    assert mock_check_lot.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 19

    mock_patch_assets.assert_called_with(verification_lot, 'pending')

    assert mock_patch_lot.call_count == 13

    assert mock_mapping.put.call_count == 9

    # Test when _patch_lot_asset_related_processes can`t patch all but patch some
    related_process_list = [
        {
            'type': 'asset',
            'id': '2' * 32,
            'relatedProcessID': '1' * 32
        }
    ]
    mock_patch_related_processes.return_value = (False, related_process_list)
    mock_check_lot.side_effect = iter([
        True
    ])
    mock_check_assets.side_effect = iter([
        True
    ])
    mock_patch_assets.side_effect = iter([
        (True, ['all_assets']),
        (True, ['all_assets']),
        (True, ['all_assets']),
    ])
    mock_get_asset.side_effect = iter([
        munchify(assets[9])
    ])

    bot.process_lots(verification_lot)  # assets_available: True; patch_assets: [(True, []), (True, [])]; check_lot: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

    assert mock_patch_related_processes.call_count == 8
    lot_with_patched_rps = {
        'id': verification_lot['id'],
        'relatedProcesses': related_process_list
    }
    mock_patch_related_processes.assert_called_with(lot_with_patched_rps, cleanup=True)

    assert mock_check_assets.call_count == 13
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_check_lot.call_count == 19
    assert mock_check_lot.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 22
    mock_patch_assets.assert_called_with(verification_lot, 'pending')

    assert mock_patch_lot.call_count == 13

    assert mock_mapping.put.call_count == 9


def test_process_lots_broken(bot, logger, mocker):

    mock_log_broken_lot = mocker.patch('openregistry.concierge.loki.processing.log_broken_lot', autospec=True)

    mock_check_lot = mocker.patch.object(bot, 'check_lot', autospec=True)
    mock_check_lot.return_value = True

    mock_check_assets = mocker.patch.object(bot, 'check_assets', autospec=True)
    mock_check_assets.return_value = True

    mock_patch_related_processes = mocker.patch.object(bot, '_patch_lot_asset_related_processes', autospec=True)
    mock_patch_related_processes.return_value = (True, ['all_assets'])

    mock_patch_assets = mocker.patch.object(bot, 'patch_assets', autospec=True)
    mock_patch_assets.side_effect = iter([
        (False, ['successfully_patched_assets']), (False, []),
        (True, ['']), (False, ['successfully_patched_assets']), (False, []),
        (True, []), (True, []), (True, [])
    ])

    mock_patch_lot = mocker.patch.object(bot, 'patch_lot', autospec=True)
    mock_patch_lot.return_value = False

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    mock_get_asset = mocker.MagicMock()
    mock_get_asset.return_value = munchify(assets[7])

    bot.assets_client.get_asset = mock_get_asset


    lot = deepcopy(lots[0]['data'])

    # failed on patching assets to verification
    bot.process_lots(lot)  # patch_assets: [False, False]

    assert mock_patch_assets.call_count == 2
    assert mock_patch_assets.call_args_list[0][0] == (lot, 'verification')
    assert mock_patch_assets.call_args_list[1][0] == ({'assets': ['successfully_patched_assets']}, 'pending')

    assert mock_log_broken_lot.call_count == 1
    assert mock_log_broken_lot.call_args_list[0][0] == (
        bot.db, LOGGER, bot.errors_doc, lot, 'patching assets to verification'
    )

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'
    assert log_strings[1] == "Assets ['successfully_patched_assets'] will be repatched to 'pending'"

    # failed on patching assets to active
    mock_deepcopy = mocker.patch('openregistry.concierge.loki.processing.deepcopy', autospec=True)

    bot.process_lots(lot)  # patch_assets: [True, False, False]

    assert mock_patch_assets.call_count == 5
    assert mock_patch_assets.call_args_list[2][0] == (lot, 'verification')
    assert mock_patch_assets.call_args_list[3][0] == (lot, 'active')
    assert mock_patch_assets.call_args_list[4][0] == (lot, 'pending')

    assert mock_log_broken_lot.call_count == 2
    assert mock_log_broken_lot.call_args_list[1][0] == (
        bot.db, LOGGER, bot.errors_doc, lot, 'patching assets to active'
    )

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[2] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'
    assert log_strings[3] == "Assets [u'e519404fd0b94305b3b19ec60add05e7'] will" \
                             " be repatched to 'pending'"

    # failed on patching Lot to active.salable
    bot.process_lots(lot)  # patch_assets: [True, True]; patch_lot: False

    assert mock_patch_assets.call_count == 8
    assert mock_patch_assets.call_args_list[5][0] == (lot, 'verification')
    assert mock_patch_assets.call_args_list[6][0] == (lot, 'active')
    assert mock_patch_assets.call_args_list[7][0] == (lot, 'pending')

    assert mock_log_broken_lot.call_count == 3
    assert mock_log_broken_lot.call_args_list[2][0] == (
        bot.db, LOGGER, bot.errors_doc, lot, 'patching Lot to pending'
    )

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[4] == 'Processing Lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'


def test_check_assets(bot, logger, mocker):
    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    verification_lot = deepcopy(lots[0]['data'])
    verification_lot['relatedProcesses'] = [
        {
            'relatedProcessID': 'e519404fd0b94305b3b19ec60add05e7',
            'type': 'asset',

        }
    ]
    dissolved_lot = deepcopy(lots[1]['data'])
    dissolved_lot['relatedProcesses'] = [
        {
            'relatedProcessID': '0a7eba27b22a454180d3a49b02a1842f',
            'type': 'asset',
        }
    ]

    bot.get_asset_related_lot = mocker.MagicMock()
    bot.get_asset_related_lot.return_value = None

    mock_get_asset = mocker.MagicMock()
    mock_get_asset.side_effect = [
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        ResourceNotFound(response=munchify({"text": "Asset could not be found."})),
        munchify(assets[0]),
        munchify(assets[7])
    ]

    bot.assets_client.get_asset = mock_get_asset

    with pytest.raises(RequestFailed):
        bot.check_assets(verification_lot)

    result = bot.check_assets(verification_lot)
    assert result is False

    result = bot.check_assets(verification_lot)
    assert result is True

    result = bot.check_assets(dissolved_lot)
    assert result is False

    loki_verification_lot = deepcopy(lots[5]['data'])
    basic_asset = deepcopy(assets[7])
    basic_asset['data']['status'] = 'pending'
    basic_asset['data']['assetType'] = 'basic'

    bounce_asset = deepcopy(assets[9])
    bounce_asset['data']['status'] = 'pending'
    bounce_asset['data']['relatedLot'] = loki_verification_lot['id']

    bot.get_asset_related_lot.side_effect = [
        munchify({
            'relatedProcessID': loki_verification_lot['id']
        }),
        munchify({
            'relatedProcessID': loki_verification_lot['id']
        }),
    ]

    mock_get_asset.side_effect = [
        munchify(basic_asset),
        munchify(bounce_asset)
    ]

    loki_verification_lot['relatedProcesses'] = [
        {
            'relatedProcessID': basic_asset['data']['id'],
            'type': 'asset',
        }
    ]
    result = bot.check_assets(loki_verification_lot)
    assert result is False

    loki_verification_lot['relatedProcesses'] = [
        {
            'relatedProcessID': bounce_asset['data']['id'],
            'type': 'asset',
        }
    ]
    result = bot.check_assets(loki_verification_lot)
    assert result is True


    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Failed to get asset e519404fd0b94305b3b19ec60add05e7. Status code: 502"
    assert log_strings[1] == "Failed to get asset e519404fd0b94305b3b19ec60add05e7: Asset could not be found."
    assert log_strings[2] == "Successfully got asset e519404fd0b94305b3b19ec60add05e7"
    assert log_strings[3] == "Successfully got asset 0a7eba27b22a454180d3a49b02a1842f"
    assert log_strings[4] == "Successfully got asset {}".format(basic_asset['data']['id'])
    assert log_strings[5] == "Successfully got asset {}".format(bounce_asset['data']['id'])


    # If asset.mode and and lot.mode not equal
    asset_with_mode = deepcopy(bounce_asset)
    asset_with_mode['data']['mode'] = 'test'
    loki_verification_lot['assets'] = [asset_with_mode['data']['id']]
    mock_get_asset.side_effect = [
        munchify(asset_with_mode)
    ]

    result = bot.check_assets(loki_verification_lot)
    assert result is False


def test_check_lot(bot, logger, mocker):

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = deepcopy(lots[0]['data'])
    wrong_status_lot = deepcopy(lot)
    wrong_status_lot['status'] = 'pending'

    mock_get_lot = mocker.MagicMock()
    mock_get_lot.side_effect = [
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        ResourceNotFound(response=munchify({"text": "Lot could not be found."})),
        munchify({"data": lot}),
        munchify({"data": wrong_status_lot})
    ]

    bot.lots_client.get_lot = mock_get_lot

    result = bot.check_lot(lot)
    assert result is False

    result = bot.check_lot(lot)
    assert result is False

    result = bot.check_lot(lot)
    assert result is True

    result = bot.check_lot(wrong_status_lot)
    assert result is False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Failed to get Lot 9ee8f769438e403ebfb17b2240aedcf1. Status code: 502"
    assert log_strings[1] == "Failed to get Lot 9ee8f769438e403ebfb17b2240aedcf1: Lot could not be found."
    assert log_strings[2] == "Successfully got Lot 9ee8f769438e403ebfb17b2240aedcf1"
    assert log_strings[3] == "Successfully got Lot 9ee8f769438e403ebfb17b2240aedcf1"
    assert log_strings[4] == "Lot 9ee8f769438e403ebfb17b2240aedcf1 can not be processed in current status ('pending')"


def test_dict_from_object(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)
    lot = lots[7]['data']
    lot['contracts'] = []
    lot['contracts'].append(
        {
            'type': 'yoke'
        }
    )

    auction_index = 0
    auction_dict = bot._dict_from_object(KEYS_FOR_AUCTION_CREATE, lot, auction_index)
    assert auction_dict['title'] == lot['title']
    assert auction_dict['description'] == lot['description']
    assert auction_dict['merchandisingObject'] == lot['id']
    assert 'items' not in auction_dict
    assert auction_dict['procuringEntity'] == lot['lotCustodian']
    assert auction_dict['value'] == lot['auctions'][auction_index]['value']
    assert auction_dict['minimalStep'] == lot['auctions'][auction_index]['minimalStep']
    assert auction_dict['guarantee'] == lot['auctions'][auction_index]['guarantee']
    assert auction_dict['registrationFee'] == lot['auctions'][auction_index]['registrationFee']
    assert auction_dict['contractTerms']['type'] == lot['contracts'][0]['type']
    assert 'documents' not in auction_dict
    assert 'backAccount' not in auction_dict
    assert 'auctionParameters' not in auction_dict

    auction_index = 2
    auction_dict = bot._dict_from_object(KEYS_FOR_AUCTION_CREATE, lot, auction_index)
    assert auction_dict['title'] == lot['title']
    assert auction_dict['description'] == lot['description']
    assert auction_dict['merchandisingObject'] == lot['id']
    assert 'items' not in auction_dict
    assert auction_dict['procuringEntity'] == lot['lotCustodian']
    assert auction_dict['value'] == lot['auctions'][auction_index]['value']
    assert auction_dict['minimalStep'] == lot['auctions'][auction_index]['minimalStep']
    assert auction_dict['guarantee'] == lot['auctions'][auction_index]['guarantee']
    assert auction_dict['registrationFee'] == lot['auctions'][auction_index]['registrationFee']
    assert 'documents' not in auction_dict
    assert 'backAccount' not in auction_dict
    assert auction_dict['auctionParameters'] == lot['auctions'][auction_index]['auctionParameters']


def test_create_auction(bot, logger, mocker):
    mock_dict_from_object = mocker.patch.object(bot, '_dict_from_object', autospec=True)
    mock_get_next_auction = mocker.patch.object(bot, 'get_next_auction', autospec=True)
    mock_post_auction = mocker.patch.object(bot, '_post_auction', autospec=True)
    mock_datetime = mocker.patch('openregistry.concierge.loki.processing.datetime', autospec=True)
    mock_extract_transfer_token = mocker.patch.object(bot, '_extract_transfer_token', autospec=True)
    mock_extract_transfer_token.side_effect = lambda l: 'transfer_token'


    dict_with_value = {'value': 'value'}

    auction_obj = 'auction'

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    active_salable_lot = lots[7]['data']

    # With first auction
    mock_dict_from_object.side_effect = iter([deepcopy(dict_with_value)])

    mock_post_auction.side_effect = iter([auction_obj])
    auction = deepcopy(active_salable_lot['auctions'][0])
    now_date = datetime.now(TZ)
    mock_get_next_auction.side_effect = iter([auction])

    mock_datetime.now.side_effect = iter([now_date])
    auction_date = calculate_business_date(now_date, timedelta(3), None, True)

    auction['auctionPeriod']['startDate'] = auction_date.isoformat()

    data_with_auction_period = deepcopy(dict_with_value)
    data_with_auction_period['auctionPeriod'] = {'startDate': auction_date.isoformat()}
    data_with_auction_period['transfer_token'] = 'transfer_token'
    data_with_auction_period['documents'] = []
    data_with_auction_period['status'] = 'pending.activation'

    result = bot._create_auction(active_salable_lot)

    assert result == (auction_obj, auction['id'])

    assert mock_dict_from_object.call_count == 1
    mock_dict_from_object.assert_called_with(KEYS_FOR_AUCTION_CREATE, active_salable_lot, auction['tenderAttempts'] - 1)

    assert mock_get_next_auction.call_count == 1
    mock_get_next_auction.assert_called_with(active_salable_lot)

    assert mock_extract_transfer_token.call_count == 1

    assert mock_post_auction.call_count == 1
    mock_post_auction.assert_called_with({'data': data_with_auction_period}, active_salable_lot['id'])

    # Test if auctionPeriod.startDate is less than datetime.now
    mock_dict_from_object.side_effect = iter([deepcopy(dict_with_value)])

    mock_post_auction.side_effect = iter([auction_obj])
    auction = active_salable_lot['auctions'][0]
    mock_get_next_auction.side_effect = iter([auction])

    start_date = datetime.now(TZ) - timedelta(2)
    old_period_lot = deepcopy(active_salable_lot)
    old_period_lot['auctions'][0]['auctionPeriod']['startDate'] = start_date.isoformat()
    now_date = datetime.now(TZ)

    mock_datetime.now.side_effect = iter([now_date])

    data_with_auction_period = deepcopy(dict_with_value)
    data_with_auction_period['auctionPeriod'] = {
        'startDate': calculate_business_date(now_date, timedelta(1), None, True).isoformat()
    }
    data_with_auction_period['documents'] = []
    data_with_auction_period['transfer_token'] = 'transfer_token'
    data_with_auction_period['status'] = 'pending.activation'

    result = bot._create_auction(old_period_lot)

    assert result == (auction_obj, auction['id'])

    assert mock_dict_from_object.call_count == 2
    mock_dict_from_object.assert_called_with(KEYS_FOR_AUCTION_CREATE, old_period_lot, auction['tenderAttempts'] - 1)

    assert mock_get_next_auction.call_count == 2
    mock_get_next_auction.assert_called_with(old_period_lot)

    assert mock_extract_transfer_token.call_count == 2

    assert mock_post_auction.call_count == 2
    mock_post_auction.assert_called_with({'data': data_with_auction_period}, old_period_lot['id'])

    # Tender attempts more than 1
    mock_dict_from_object.side_effect = iter([deepcopy(dict_with_value)])

    mock_post_auction.side_effect = iter([auction_obj])
    data_with_tender_period = deepcopy(dict_with_value)
    data_with_tender_period['documents'] = []
    data_with_tender_period['transfer_token'] = 'transfer_token'
    data_with_tender_period['status'] = 'pending.activation'

    auction = active_salable_lot['auctions'][1]
    mock_get_next_auction.side_effect = iter([auction])

    start_date = now_date + parse_duration(active_salable_lot['auctions'][1]['tenderingDuration'])
    data_with_tender_period['auctionPeriod'] = {
        'startDate': start_date.isoformat(),
    }

    mock_datetime.now.side_effect = iter([now_date])

    result = bot._create_auction(active_salable_lot)

    assert result == (auction_obj, auction['id'])

    assert mock_dict_from_object.call_count == 3
    mock_dict_from_object.assert_called_with(KEYS_FOR_AUCTION_CREATE, active_salable_lot, auction['tenderAttempts'] - 1)

    assert mock_get_next_auction.call_count == 3
    mock_get_next_auction.assert_called_with(active_salable_lot)

    assert mock_extract_transfer_token.call_count == 3

    assert mock_post_auction.call_count == 3
    mock_post_auction.assert_called_with({'data': data_with_tender_period}, active_salable_lot['id'])

    # When you get error
    mock_dict_from_object.side_effect = iter([deepcopy(dict_with_value)])

    mock_datetime.now.side_effect = iter([now_date])

    auction = active_salable_lot['auctions'][1]
    mock_get_next_auction.side_effect = iter([auction])

    mock_post_auction.side_effect = iter([
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        ])
    result = bot._create_auction(active_salable_lot)

    assert result is False

    assert mock_dict_from_object.call_count == 4
    mock_dict_from_object.assert_called_with(KEYS_FOR_AUCTION_CREATE, active_salable_lot, auction['tenderAttempts'] - 1)

    assert mock_get_next_auction.call_count == 4
    mock_get_next_auction.assert_called_with(active_salable_lot)

    assert mock_extract_transfer_token.call_count == 4

    assert mock_post_auction.call_count == 4
    mock_post_auction.assert_called_with({'data': data_with_tender_period}, active_salable_lot['id'])

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Failed to create auction from Lot {} (Server error: 502)'.format(active_salable_lot['id'])

    # Create auction with wrong procurementMethodType
    mock_dict_from_object.side_effect = iter([deepcopy(dict_with_value)])

    mock_datetime.now.side_effect = iter([start_date])

    auction = deepcopy(active_salable_lot['auctions'][0])
    auction['procurementMethodType'] = 'wrong'
    mock_get_next_auction.side_effect = iter([auction])

    result = bot._create_auction(active_salable_lot)

    assert result is None

    assert mock_dict_from_object.call_count == 4

    assert mock_get_next_auction.call_count == 5
    mock_get_next_auction.assert_called_with(active_salable_lot)

    assert mock_extract_transfer_token.call_count == 4
    assert mock_post_auction.call_count == 4

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == "Such procurementMethodType is not allowed to create {}. " \
                             "Allowed procurementMethodType {}".format(auction['procurementMethodType'], bot.allowed_pmt)

    # When can`t extract credentials
    mock_dict_from_object.side_effect = iter([deepcopy(dict_with_value)])
    mock_extract_transfer_token.side_effect = iter([
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
    ])

    mock_datetime.now.side_effect = iter([start_date])

    auction = deepcopy(active_salable_lot['auctions'][0])
    mock_get_next_auction.side_effect = iter([auction])

    result = bot._create_auction(active_salable_lot)

    assert result is None

    assert mock_dict_from_object.call_count == 5

    assert mock_get_next_auction.call_count == 6
    mock_get_next_auction.assert_called_with(active_salable_lot)

    assert mock_extract_transfer_token.call_count == 5
    assert mock_post_auction.call_count == 4

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[2] == "Failed to extract transfer token from " \
                             "Lot {} (Server error: 502)".format(active_salable_lot['id'])


    # Check if lot.documents is united with auction.documents
    dict_with_documents = deepcopy(dict_with_value)
    dict_with_documents['documents'] = [
        {
            'title': 'something',
            'documentOf': 'auction'
        }
    ]
    lot_with_docs = deepcopy(active_salable_lot)
    lot_with_docs['documents'] = [
        {
            'title': 'something',
            'documentOf': 'lot'
        },
        {
            'title': 'something',
            'documentOf': 'item',
            'relatedItem': '1' * 32
        }
    ]
    mock_dict_from_object.side_effect = iter([deepcopy(dict_with_documents)])
    mock_extract_transfer_token.side_effect = iter([
        'transfer_token'
    ])

    mock_post_auction.side_effect = iter([auction_obj])
    auction = deepcopy(active_salable_lot['auctions'][0])
    now_date = datetime.now(TZ)
    mock_get_next_auction.side_effect = iter([auction])

    mock_datetime.now.side_effect = iter([now_date])
    auction_date = calculate_business_date(now_date, timedelta(3), None, True)

    auction['auctionPeriod']['startDate'] = auction_date.isoformat()

    data_with_documents = deepcopy(dict_with_documents)
    data_with_documents['auctionPeriod'] = {'startDate': auction_date.isoformat()}
    data_with_documents['transfer_token'] = 'transfer_token'
    lot_docs = deepcopy(lot_with_docs.get('documents', []))
    for d in lot_docs:
        if d['documentOf'] == 'lot':
            d['relatedItem'] = lot_with_docs['id']

    data_with_documents['documents'] = dict_with_documents['documents'] + lot_docs
    data_with_documents['status'] = 'pending.activation'

    result = bot._create_auction(lot_with_docs)

    assert result == (auction_obj, auction['id'])

    assert mock_dict_from_object.call_count == 6
    mock_dict_from_object.assert_called_with(KEYS_FOR_AUCTION_CREATE, lot_with_docs, auction['tenderAttempts'] - 1)

    assert mock_get_next_auction.call_count == 7
    mock_get_next_auction.assert_called_with(lot_with_docs)

    assert mock_extract_transfer_token.call_count == 6

    assert mock_post_auction.call_count == 5
    mock_post_auction.assert_called_with({'data': data_with_documents}, active_salable_lot['id'])


def test_get_next_auction(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = lots[7]['data']

    # Test when next auction is first auction
    result = bot.get_next_auction(lot)
    assert result == lot['auctions'][0]


    # Test when first auction is unsuccessful
    lot['auctions'][0]['status'] = 'unsuccessful'

    result = bot.get_next_auction(lot)
    assert result == lot['auctions'][1]

    # Test when all auctions is unsuccessful
    lot['auctions'][0]['status'] = 'unsuccessful'
    lot['auctions'][1]['status'] = 'unsuccessful'
    lot['auctions'][2]['status'] = 'unsuccessful'

    result = bot.get_next_auction(lot)
    assert result is None


def test_post_auction(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = lots[7]['data']

    mock_auction_client = bot.auction_client

    # Test when post is success
    auction = {'data': {'id': 'auctionID'}}
    mock_auction_client.create_auction.side_effect = iter([auction])

    result = bot._post_auction(lot['auctions'][0], lot['id'])

    assert result == auction

    assert mock_auction_client.create_auction.call_count == 1
    mock_auction_client.create_auction.assert_called_with(lot['auctions'][0])

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Successfully created auction {} from Lot {})".format(auction['data']['id'], lot['id'])

    # Test when post is failed
    auction = {'id': 'auctionID'}
    mock_auction_client.side_effect = [auction]

    mock_auction_client.create_auction.side_effect = iter([
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
    ])

    try:
        bot._post_auction(lot['auctions'][0], lot['id'])
    except RequestFailed as ex:
        pass

    assert isinstance(ex, RequestFailed) is True
    assert mock_auction_client.create_auction.call_count == 6
    mock_auction_client.create_auction.assert_called_with(lot['auctions'][0])


def test_patch_auction(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = lots[7]['data']

    mock_lots_client = bot.lots_client
    auction = 'auction'

    auction_id = '1' * 32
    # Test when patch is success
    patched_data = {'auctionID': auction_id, 'status': 'active'}
    mock_lots_client.patch_resource_item_subitem.side_effect = iter([
        auction
    ])

    result = bot._patch_auction(patched_data, lot['id'], auction_id)

    assert result == auction

    assert mock_lots_client.patch_resource_item_subitem.call_count == 1
    mock_lots_client.patch_resource_item_subitem.assert_called_with(
        resource_item_id=lot['id'],
        patch_data={'data': patched_data},
        subitem_name='auctions',
        subitem_id=auction_id
    )

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Successfully patched Lot.auction {} from Lot {})".format(auction_id, lot['id'])

    # Test when post is failed
    mock_lots_client.patch_resource_item_subitem.side_effect = iter([
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
    ])

    try:
        bot._patch_auction(patched_data, lot['id'], auction_id)
    except RequestFailed as ex:
        pass

    assert isinstance(ex, RequestFailed) is True
    assert mock_lots_client.patch_resource_item_subitem.call_count == 6
    mock_lots_client.patch_resource_item_subitem.assert_called_with(
        resource_item_id=lot['id'],
        patch_data={'data': patched_data},
        subitem_name='auctions',
        subitem_id='1' * 32
    )


def test_check_previous_auction(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = lots[7]['data']

    # Test first auction is scheduled
    result = bot.check_previous_auction(lot)
    assert result is True

    # Test first auction is unsuccessful
    lot['auctions'][0]['status'] = 'unsuccessful'
    result = bot.check_previous_auction(lot)
    assert result is True

    # Test first auction is cancelled
    lot['auctions'][0]['status'] = 'cancelled'
    result = bot.check_previous_auction(lot)
    assert result is False

    # Test all auctions is unsuccessful
    lot['auctions'][0]['status'] = 'unsuccessful'
    lot['auctions'][1]['status'] = 'unsuccessful'
    lot['auctions'][2]['status'] = 'unsuccessful'
    result = bot.check_previous_auction(lot)
    assert result is False


def test_patch_lot_asset_related_processes(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    lot = lots[0]['data']
    related_processes = [rP for rP in lot['relatedProcesses'] if rP['type'] == 'asset']
    used_rP = related_processes[-1]

    mock_get_asset = mocker.MagicMock()
    mock_patch_related_process = mocker.MagicMock()
    bot.assets_client.get_asset = mock_get_asset
    bot._patch_related_process = mock_patch_related_process

    # Cleanup False
    mock_get_asset.side_effect = [
        munchify(assets[0])
    ]

    result, patched_rPs = bot._patch_lot_asset_related_processes(lot=lot)

    data = {'identifier': assets[0]['data']['assetID']}

    assert result is True
    assert patched_rPs == [
        used_rP
    ]

    assert mock_get_asset.call_count == 1
    mock_get_asset.assert_called_with(used_rP['relatedProcessID'])

    assert mock_patch_related_process.call_count == 1
    mock_patch_related_process.assert_called_with(data, lot['id'], used_rP['id'])

    # Cleanup True
    mock_get_asset.side_effect = [
        munchify(assets[0])
    ]

    result, patched_rPs = bot._patch_lot_asset_related_processes(lot=lot, cleanup=True)

    data = {'identifier': ''}

    assert result is True
    assert patched_rPs == [
        used_rP
    ]

    assert mock_get_asset.call_count == 2
    mock_get_asset.assert_called_with(used_rP['relatedProcessID'])

    assert mock_patch_related_process.call_count == 2
    mock_patch_related_process.assert_called_with(data, lot['id'], used_rP['id'])


def test_patch_related_process(bot, logger, mocker):
    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = lots[0]['data']

    mock_lots_client = bot.lots_client

    related_process_id = '1' * 32
    # Test when patch is success
    patched_data = {'identifier': 'SOME-VALUE'}
    mocked_patch_subitem = mock_lots_client.patch_resource_item_subitem

    bot._patch_related_process(patched_data, lot['id'], related_process_id)

    assert mocked_patch_subitem.call_count == 1
    mocked_patch_subitem.assert_called_with(
        resource_item_id=lot['id'],
        patch_data={'data': patched_data},
        subitem_name='related_processes',
        subitem_id=related_process_id
    )

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Successfully patched Lot.relatedProcess {} from Lot {})".format(related_process_id, lot['id'])

    # Test when post is failed
    mocked_patch_subitem.side_effect = iter([
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Bad Gateway", "status_code": 502})),
    ])

    try:
        bot._patch_related_process(patched_data, lot['id'], related_process_id)
    except RequestFailed as ex:
        pass

    assert isinstance(ex, RequestFailed) is True
    assert mocked_patch_subitem.call_count == 6
    mocked_patch_subitem.assert_called_with(
        resource_item_id=lot['id'],
        patch_data={'data': patched_data},
        subitem_name='related_processes',
        subitem_id='1' * 32
    )


def test_clean_related_processes(bot, logger, mocker):
    bot._remove_asset_lot_related_process = mocker.MagicMock()

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = lots[0]['data']
    assets_rPs = [
        {
            'id': 'if of first asset related process',
            'asset_parent': 'id of first asset'
        },
    ]

    # Removing is succesful
    bot._remove_asset_lot_related_process.return_value = {}
    result = bot.clean_asset_related_processes(lot, assets_rPs)

    assert result is True
    assert bot._remove_asset_lot_related_process.call_count == 1
    bot._remove_asset_lot_related_process.assert_called_with(assets_rPs[0]['asset_parent'], assets_rPs[0]['id'])

    # Removing is failed
    bot._remove_asset_lot_related_process.side_effect = [
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502}))
    ]

    result = bot.clean_asset_related_processes(lot, assets_rPs)

    assert result is False
    assert bot._remove_asset_lot_related_process.call_count == 2
    bot._remove_asset_lot_related_process.assert_called_with(assets_rPs[0]['asset_parent'], assets_rPs[0]['id'])


def test_add_related_process_to_assets(bot, logger, mocker):
    bot._create_asset_related_process = mocker.MagicMock()
    bot.make_lot_related_process = mocker.MagicMock()

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = lots[0]['data']
    lot_assets = [
        {'relatedProcessID': 'first related process id', 'type': 'some type'},
        {'relatedProcessID': 'second related process id', 'type': 'asset'},
        {'relatedProcessID': 'third related process id', 'type': 'asset'},
    ]
    lot['relatedProcesses'] = lot_assets

    asset_related_process_data = {
        'type': 'lot',
        'relatedProcessID': lot['id'],
        'identifier': lot['lotID']
    }
    bot.make_lot_related_process.return_value = asset_related_process_data

    # All adding is succesful
    bot._create_asset_related_process.side_effect = [
        {'id': '1'},
        {'id': '2'}
    ]
    expected_patched_rPs = [
        {'id': '1', 'asset_parent': lot_assets[1]['relatedProcessID']},
        {'id': '2', 'asset_parent': lot_assets[2]['relatedProcessID']}
    ]

    result, patched_rPs = bot.add_related_process_to_assets(lot)

    assert result is True
    assert patched_rPs == expected_patched_rPs

    assert bot._create_asset_related_process.call_count == 2
    bot._create_asset_related_process.assert_called_with(lot_assets[2]['relatedProcessID'], asset_related_process_data)

    # One of adding failed
    bot._create_asset_related_process.side_effect = [
        {'id': '1'},
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502}))
    ]
    expected_patched_rPs = [
        {'id': '1', 'asset_parent': lot_assets[1]['relatedProcessID']},
    ]

    result, patched_rPs = bot.add_related_process_to_assets(lot)

    assert result is False
    assert patched_rPs == expected_patched_rPs

    assert bot._create_asset_related_process.call_count == 4
    bot._create_asset_related_process.assert_called_with(lot_assets[2]['relatedProcessID'], asset_related_process_data)


    # All failed
    bot._create_asset_related_process.side_effect = [
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502})),
        RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502}))
    ]
    expected_patched_rPs = []

    result, patched_rPs = bot.add_related_process_to_assets(lot)

    assert result is False
    assert patched_rPs == expected_patched_rPs

    assert bot._create_asset_related_process.call_count == 6
    bot._create_asset_related_process.assert_called_with(lot_assets[2]['relatedProcessID'], asset_related_process_data)


def test_create_asset_related_process(bot, logger, mocker):
    subitem_create_mock = mocker.MagicMock()

    bot.assets_client.create_resource_item_subitem = subitem_create_mock

    related_process_data = {'some': 'data'}
    asset_id = '1' * 32

    # Test if create is successful
    response = 'response'
    bot.assets_client.create_resource_item_subitem.side_effect = [
        response
    ]
    related_process = bot._create_asset_related_process(asset_id, related_process_data)

    assert related_process == response
    assert subitem_create_mock.call_count == 1
    subitem_create_mock.assert_called_with(
        resource_item_id=asset_id,
        subitem_obj={'data': related_process_data},
        subitem_name='related_processes'
    )

    # Test if create is failed
    response = RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502}))
    bot.assets_client.create_resource_item_subitem.side_effect = [
        response,
        response,
        response,
        response,
        response
    ]
    exception = None

    related_process = None
    try:
        related_process = bot._create_asset_related_process(asset_id, related_process_data)
    except RequestFailed as ex:
        exception = ex

    assert related_process is None
    assert subitem_create_mock.call_count == 6
    subitem_create_mock.assert_called_with(
        resource_item_id=asset_id,
        subitem_obj={'data': related_process_data},
        subitem_name='related_processes'
    )
    assert exception is response


def test_remove_asset_lot_related_process(bot, logger, mocker):
    subitem_delete_mock = mocker.MagicMock()

    bot.assets_client.delete_resource_item_subitem = subitem_delete_mock

    asset_id = '1' * 32
    subitem_id = '2' * 32

    # Test if delete is successful
    response = 'response'
    bot.assets_client.delete_resource_item_subitem.side_effect = [
        response
    ]
    related_process = bot._remove_asset_lot_related_process(asset_id, subitem_id)

    assert related_process == response
    assert subitem_delete_mock.call_count == 1
    subitem_delete_mock.assert_called_with(
        resource_item_id=asset_id,
        subitem_id=subitem_id,
        subitem_name='related_processes'
    )

    # Test if delete is failed
    response = RequestFailed(response=munchify({"text": "Request failed.", "status_code": 502}))
    bot.assets_client.delete_resource_item_subitem.side_effect = [
        response,
        response,
        response,
        response,
        response
    ]
    exception = None

    related_process = None
    try:
        related_process = bot._remove_asset_lot_related_process(asset_id, subitem_id)
    except RequestFailed as ex:
        exception = ex

    assert related_process is None
    assert subitem_delete_mock.call_count == 6
    subitem_delete_mock.assert_called_with(
        resource_item_id=asset_id,
        subitem_id=subitem_id,
        subitem_name='related_processes'
    )
    assert exception is response


def test_get_asset_related_lot(bot, logger, mocker):
    # Test if lot related process exist
    asset = {
        'relatedProcesses': [
            {'type': 'some type', 'id': 'first id'},
            {'type': 'lot', 'id': 'second id'},
        ]
    }
    asset = munchify(asset)

    related_process = bot.get_asset_related_lot(asset)
    assert related_process == asset['relatedProcesses'][1]

    # Test if no lot relate process in asset
    asset = {
        'relatedProcesses': [
            {'type': 'some type', 'id': 'first id'},
            {'type': 'another type', 'id': 'second id'},
        ]
    }
    asset = munchify(asset)

    related_process = bot.get_asset_related_lot(asset)
    assert related_process is None


def test_make_lot_related_process(bot, logger, mocker):
    lot = {
        'id': 'lot_id',
        'lotID': 'lot identifier'
    }

    expected_result = {
            'type': 'lot',
            'relatedProcessID': lot['id'],
            'identifier': lot['lotID']
        }

    result = bot.make_lot_related_process(lot)
    assert result == expected_result
