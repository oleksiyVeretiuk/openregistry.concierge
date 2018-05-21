# -*- coding: utf-8 -*-
import os
from copy import deepcopy
from json import load

import pytest
from munch import munchify

from openregistry.concierge.tests.conftest import TEST_CONFIG
from openregistry.concierge.loki.processing import logger as LOGGER
from openregistry.concierge.loki.processing import ProcessingLoki
from openprocurement_client.exceptions import (
    Forbidden,
    ResourceNotFound,
    RequestFailed,
    UnprocessableEntity
)
from openregistry.concierge.loki.constants import KEYS_FOR_LOKI_PATCH

ROOT = os.path.dirname(__file__) + '/data/'


def test_processing_loki_init(db, logger, mocker):
    mocker.patch('openregistry.concierge.utils.LotsClient', autospec=True)
    mocker.patch('openregistry.concierge.utils.AssetsClient', autospec=True)
    ProcessingLoki(TEST_CONFIG)
    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'lots_client - ok'
    assert log_strings[1] == 'assets_client - ok'
    assert log_strings[2] == 'couchdb - ok'


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

    assert log_strings[0] == 'Successfully patched lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable'
    assert log_strings[1] == 'Failed to patch lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Operation is forbidden.)'
    assert log_strings[2] == 'Failed to patch lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Server error: 502)'
    assert log_strings[3] == 'Failed to patch lot 9ee8f769438e403ebfb17b2240aedcf1 to active.salable (Unprocessable Entity.)'


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

    result, patched_assets = bot.patch_assets(lot=lot, status=status, related_lot=lot['id'])
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

    result, patched_assets = bot.patch_assets(lot=lot, status=status, related_lot=lot['id'])
    assert result is True
    assert patched_assets == ['e519404fd0b94305b3b19ec60add05e7']

    result, patched_assets = bot.patch_assets(lot=lot, status=status, related_lot=lot['id'])
    assert result is False
    assert patched_assets == []


    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Successfully patched asset e519404fd0b94305b3b19ec60add05e7 to verification'
    assert log_strings[1] == 'Failed to patch asset e519404fd0b94305b3b19ec60add05e7 to verification (Server error: 502)'

    assert bot.assets_client.patch_asset.call_count == 6


def test_process_lots(bot, logger, mocker):
    mock_get_asset = mocker.MagicMock()
    bot.assets_client.get_asset = mock_get_asset

    mock_check_lot = mocker.patch.object(bot, 'check_lot', autospec=True)
    # mock_check_lot.side_effect = iter([
    #     True,
    #     True,
    #     True,
    #     True,
    #     True,
    #     False,
    #     True,
    #     True,
    #     True,
    #     True,
    #     True,
    # ])

    mock_check_assets = mocker.patch.object(bot, 'check_assets', autospec=True)
    # mock_check_assets.side_effect = iter([
    #     True,
    #     True,
    #     False,
    #     RequestFailed(response=munchify({"text": "Request failed."})),
    #     True,
    #     True,
    # ])

    mock_patch_assets = mocker.patch.object(bot, 'patch_assets', autospec=True)
    # mock_patch_assets.side_effect = iter([
    #     (False, []),
    #     (True, []),
    #     (True, ['all_assets']),
    #     (True, ['all_assets']),
    #     (False, []),
    #     (True, ['all_assets']),
    #     (False, []),
    #     (True, ['all_assets']),
    #     (False, [])
    # ])

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
    assert log_strings[0] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

    assert mock_check_assets.call_count == 1
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_check_lot.call_count == 1
    assert mock_check_lot.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 1
    assert mock_patch_assets.call_args_list[0][0] == (verification_lot, 'verification', verification_lot['id'])

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

    to_compare = {l_key:assets[9]['data'].get(a_key, None) for a_key, l_key in KEYS_FOR_LOKI_PATCH.items()}
    to_compare['decisions'] = [
        verification_lot['decisions'][0],
        assets[9]['data']['decisions'][0],
    ]

    bot.process_lots(verification_lot)  # assets_available: True; patch_assets: [(True, []), (True, [])]; check_lot: True

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[1] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

    assert mock_check_assets.call_count == 2
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_check_lot.call_count == 2
    assert mock_check_lot.call_args[0] == (verification_lot,)

    assert mock_patch_assets.call_count == 3
    assert mock_patch_assets.call_args_list[1][0] == (verification_lot, 'verification', verification_lot['id'])
    assert mock_patch_assets.call_args_list[2][0] == (verification_lot, 'active', verification_lot['id'])

    assert mock_patch_lot.call_count == 1
    assert mock_patch_lot.call_args[0] == (verification_lot, 'pending', to_compare)

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
    assert log_strings[2] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'

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
    assert log_strings[3] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'
    assert log_strings[4] == 'Due to fail in getting assets, lot 9ee8f769438e403ebfb17b2240aedcf1 is skipped'

    assert mock_check_assets.call_count == 4
    assert mock_check_assets.call_args[0] == (verification_lot,)

    assert mock_check_lot.call_count == 4
    assert mock_check_lot.call_args[0] == (verification_lot,)

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

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[5] == 'Processing lot b844573afaa24e4fb098f3027e605c87 in status pending.dissolution'
    assert log_strings[6] == "Assets {} from lot {} will be patched to 'pending'".format(pending_dissolution_lot['assets'],
                                                                                         pending_dissolution_lot['id'])


    assert mock_check_lot.call_count == 5
    assert mock_check_lot.call_args[0] == (pending_dissolution_lot,)

    # lot is not available
    mock_check_lot.side_effect = iter([
        False
    ])
    mock_check_assets.side_effect = iter([
    ])
    mock_patch_assets.side_effect = iter([
    ])
    bot.process_lots(pending_dissolution_lot)  # assets_available: None; patch_assets: None; check_lot: False

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[7] == 'Skipping lot {}'.format(pending_dissolution_lot['id'])

    assert mock_check_lot.call_count == 6
    assert mock_check_lot.call_args[0] == (pending_dissolution_lot,)

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
    assert log_strings[9] == 'Not valid assets {} in lot {}'.format(pending_dissolution_lot['assets'], pending_dissolution_lot['id'])
    assert mock_check_lot.call_count == 7
    assert mock_check_lot.call_args[0] == (pending_dissolution_lot,)

    assert mock_check_assets.call_count == 4
    assert mock_patch_assets.call_args[0] == (pending_dissolution_lot, 'pending')

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
    assert log_strings[10] == 'Processing lot {} in status pending.sold'.format(pending_sold_lot['id'])
    assert log_strings[11] == "Assets {} from lot {} will be patched to 'complete'".format(pending_sold_lot['assets'],
                                                                                           pending_sold_lot['id'])
    assert mock_check_lot.call_count == 8
    assert mock_check_lot.call_args[0] == (pending_sold_lot,)
    assert mock_patch_lot.call_args[0] == (pending_sold_lot, 'sold')

    assert mock_check_assets.call_count == 4
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
    assert log_strings[12] == 'Processing lot {} in status pending.sold'.format(pending_sold_lot['id'])
    assert log_strings[13] == 'Not valid assets {} in lot {}'.format(pending_sold_lot['assets'], pending_sold_lot['id'])
    assert mock_check_lot.call_count == 9
    assert mock_check_lot.call_args[0] == (pending_sold_lot,)
    assert mock_patch_lot.call_args[0] == (pending_sold_lot, 'sold')

    assert mock_check_assets.call_count == 4
    assert mock_patch_assets.call_args[0] == (pending_sold_lot, 'complete')


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
    to_compare = {l_key:assets[9]['data'].get(a_key, None) for a_key, l_key in KEYS_FOR_LOKI_PATCH.items()}
    to_compare['decisions'] = [
        loki_verfication_lot['decisions'][0],
        assets[9]['data']['decisions'][0],
    ]
    bot.process_lots(loki_verfication_lot)

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[14] == 'Processing lot {} in status verification'.format(loki_verfication_lot['id'])
    assert mock_check_lot.call_count == 10
    assert mock_check_lot.call_args[0] == (loki_verfication_lot,)
    assert mock_patch_lot.call_args[0] == (loki_verfication_lot, 'pending', to_compare)

    assert mock_check_assets.call_count == 5
    assert mock_patch_assets.call_args[0] == (loki_verfication_lot, 'active', loki_verfication_lot['id'])

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
    assert log_strings[15] == 'Processing lot {} in status verification'.format(loki_verfication_lot['id'])
    assert mock_check_lot.call_count == 11
    assert mock_check_lot.call_args[0] == (loki_verfication_lot,)
    assert mock_patch_lot.call_args[0] == (loki_verfication_lot, 'invalid')

    assert mock_check_assets.call_count == 6
    assert mock_patch_assets.call_args[0] == (loki_verfication_lot, 'active', loki_verfication_lot['id'])


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
    assert log_strings[16] == 'Processing lot {} in status pending.deleted'.format(pending_deleted_lot['id'])
    assert mock_check_lot.call_count == 12
    assert mock_check_lot.call_args[0] == (pending_deleted_lot,)
    assert mock_patch_lot.call_args[0] == (pending_deleted_lot, 'deleted')

    assert mock_check_assets.call_count == 6
    assert mock_patch_assets.call_args[0] == (pending_deleted_lot, 'pending')


def test_process_lots_broken(bot, logger, mocker):

    mock_log_broken_lot = mocker.patch('openregistry.concierge.loki.processing.log_broken_lot', autospec=True)

    mock_check_lot = mocker.patch.object(bot, 'check_lot', autospec=True)
    mock_check_lot.return_value = True

    mock_check_assets = mocker.patch.object(bot, 'check_assets', autospec=True)
    mock_check_assets.return_value = True

    mock_patch_assets = mocker.patch.object(bot, 'patch_assets', autospec=True)
    mock_patch_assets.side_effect = iter([
        (False, ['successfully_patched_assets']), (False, []),
        (True, ['']), (False, ['successfully_patched_assets']), (False, []),
        (True, []), (True, [])
    ])

    mock_patch_lot = mocker.patch.object(bot, 'patch_lot', autospec=True)
    mock_patch_lot.return_value = False

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    lot = lots[0]['data']

    # failed on patching assets to verification
    bot.process_lots(lot)  # patch_assets: [False, False]

    assert mock_patch_assets.call_count == 2
    assert mock_patch_assets.call_args_list[0][0] == (lot, 'verification', lot['id'])
    assert mock_patch_assets.call_args_list[1][0] == ({'assets': ['successfully_patched_assets']}, 'pending')

    assert mock_log_broken_lot.call_count == 1
    assert mock_log_broken_lot.call_args_list[0][0] == (
        bot.db, LOGGER, bot.errors_doc, lot, 'patching assets to verification'
    )

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'
    assert log_strings[1] == "Assets ['successfully_patched_assets'] will be repatched to 'pending'"

    # failed on patching assets to active
    bot.process_lots(lot)  # patch_assets: [True, False, False]

    assert mock_patch_assets.call_count == 5
    assert mock_patch_assets.call_args_list[2][0] == (lot, 'verification', lot['id'])
    assert mock_patch_assets.call_args_list[3][0] == (lot, 'active', lot['id'])
    assert mock_patch_assets.call_args_list[4][0] == (lot, 'pending')

    assert mock_log_broken_lot.call_count == 2
    assert mock_log_broken_lot.call_args_list[1][0] == (
        bot.db, LOGGER, bot.errors_doc, lot, 'patching assets to active'
    )

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[2] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'
    assert log_strings[3] == "Assets [u'e519404fd0b94305b3b19ec60add05e7'] will" \
                             " be repatched to 'pending'"

    # failed on patching lot to active.salable
    bot.process_lots(lot)  # patch_assets: [True, True]; patch_lot: False

    assert mock_patch_assets.call_count == 7
    assert mock_patch_assets.call_args_list[5][0] == (lot, 'verification', lot['id'])
    assert mock_patch_assets.call_args_list[6][0] == (lot, 'active', lot['id'])

    assert mock_log_broken_lot.call_count == 3
    assert mock_log_broken_lot.call_args_list[2][0] == (
        bot.db, LOGGER, bot.errors_doc, lot, 'patching lot to active.salable'
    )

    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[4] == 'Processing lot 9ee8f769438e403ebfb17b2240aedcf1 in status verification'


def test_check_assets(bot, logger, mocker):
    with open(ROOT + 'assets.json') as assets:
        assets = load(assets)

    with open(ROOT + 'lots.json') as lots:
        lots = load(lots)

    verification_lot = deepcopy(lots[0]['data'])
    verification_lot['assets'] = ['e519404fd0b94305b3b19ec60add05e7']
    dissolved_lot = deepcopy(lots[1]['data'])
    dissolved_lot['assets'] = ["0a7eba27b22a454180d3a49b02a1842f"]

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
    basic_asset['data']['relatedLot'] = loki_verification_lot['id']
    basic_asset['data']['assetType'] = 'basic'

    bounce_asset = deepcopy(assets[9])
    bounce_asset['data']['status'] = 'pending'
    bounce_asset['data']['relatedLot'] = loki_verification_lot['id']


    mock_get_asset.side_effect = [
        munchify(basic_asset),
        munchify(bounce_asset)
    ]

    loki_verification_lot['assets'] = [basic_asset['data']['id']]
    result = bot.check_assets(loki_verification_lot)
    assert result is False

    loki_verification_lot['assets'] = [bounce_asset['data']['id']]
    result = bot.check_assets(loki_verification_lot)
    assert result is True


    log_strings = logger.log_capture_string.getvalue().split('\n')
    assert log_strings[0] == "Falied to get asset e519404fd0b94305b3b19ec60add05e7. Status code: 502"
    assert log_strings[1] == "Falied to get asset e519404fd0b94305b3b19ec60add05e7: Asset could not be found."
    assert log_strings[2] == "Successfully got asset e519404fd0b94305b3b19ec60add05e7"
    assert log_strings[3] == "Successfully got asset 0a7eba27b22a454180d3a49b02a1842f"
    assert log_strings[4] == "Successfully got asset {}".format(basic_asset['data']['id'])
    assert log_strings[5] == "Successfully got asset {}".format(bounce_asset['data']['id'])


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
    assert log_strings[0] == "Falied to get lot 9ee8f769438e403ebfb17b2240aedcf1. Status code: 502"
    assert log_strings[1] == "Falied to get lot 9ee8f769438e403ebfb17b2240aedcf1: Lot could not be found."
    assert log_strings[2] == "Successfully got lot 9ee8f769438e403ebfb17b2240aedcf1"
    assert log_strings[3] == "Successfully got lot 9ee8f769438e403ebfb17b2240aedcf1"
    assert log_strings[4] == "Lot 9ee8f769438e403ebfb17b2240aedcf1 can not be processed in current status ('pending')"
