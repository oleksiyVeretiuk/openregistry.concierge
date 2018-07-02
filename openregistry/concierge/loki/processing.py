# -*- coding: utf-8 -*-
import argparse
import logging
import logging.config
import time
import yaml
from copy import deepcopy
from retrying import retry
from datetime import datetime, timedelta
from dpath import util
from isodate import parse_duration
from iso8601 import parse_date
from re import compile

from openprocurement_client.exceptions import (
    Forbidden,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity,
    Conflict,
    PreconditionFailed,
)

from openregistry.concierge.utils import (
    log_broken_lot,
    get_next_status,
    retry_on_error,
    create_filter_condition
)
from openregistry.concierge.constants import TZ
from openregistry.concierge.loki.utils import calculate_business_date
from openregistry.concierge.loki.constants import (
    KEYS_FOR_LOKI_PATCH,
    NEXT_STATUS_CHANGE,
    KEYS_FOR_AUCTION_CREATE
)


logger = logging.getLogger('openregistry.concierge.worker')

EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)

HANDLED_STATUSES = ('verification', 'pending.dissolution', 'pending.sold', 'pending.deleted', 'active.salable')
HANDLED_AUCTION_STATUSES = ('scheduled', 'unsuccessful')

ACCELERATOR_RE = compile(r'.accelerator=(?P<accelerator>\d+)')

IS_BOT_WORKING = True


class ProcessingLoki(object):

    def __init__(self, config, clients, errors_doc):
        """
        Args:
            config: dictionary with configuration data
        """
        self.config = config
        self.allowed_asset_types = []
        self.handled_lot_types = []
        self.allowed_pmt = []

        self._register_allowed_procurement_method_types()
        self._register_allowed_assets()
        self._register_handled_lot_types()

        for key, item in clients.items():
            setattr(self, key, item)
        self.errors_doc = errors_doc

    @staticmethod
    def get_condition(config):
        return create_filter_condition(config.get('aliases', []), HANDLED_STATUSES)

    def _register_allowed_procurement_method_types(self):
        self.allowed_pmt += self.config.get('planned_pmt', [])

    def _register_allowed_assets(self):
        for _, asset_aliases in self.config.get('assets', {}).items():
            self.allowed_asset_types += asset_aliases

    def _register_handled_lot_types(self):
        self.handled_lot_types += self.config.get('aliases', [])

    def process_lots(self, lot):
        """
        Performs the main processing of the lot. Checks the availability
        of a given lot and assets united by this lot and switches their
        statuses to required ones.

        Lot considered as available, if it is in status 'verification'
        or 'pending.dissolution'. If this condition is not satisfied,
        lot will be skipped. Assets considered as available, if all of
        them are in status 'pending'. If this condition is not satisfied,
        lot status will be switched to 'pending'.

        In case lot is in status 'pending.dissolution', switches assets statuses
        to 'pending', sets None as value of asset field 'relatedLot' and switch lot
        status to 'dissolved'. In case processed lot is in status 'verification', at
        first, switches assets statuses to 'verification' and sets lot id as
        'relatedLot' field value and after that switches to 'active' status. If all
        PATCH requests were successful, switches lot to status 'active.salable'

        In case error occurs during switching assets statuses, tries to switch
        assets, which were patched successfully, back to status 'pending'. If
        error occurs during this patch as well, lot will be marked as broken
        and added to db document, specified in configuration file.

        If error occurs during switching lot status to 'active.salable', this lot
        will be considered as broken as well and added to db document, specified
        in configuration file.

        Args:
            lot: dictionary which contains some fields of lot
                 document from db: id, rev, status, assets, lotID.
        Returns:
            None
        """
        lot_available = self.check_lot(lot)
        if not lot_available:
            logger.info("Skipping lot {}".format(lot['id']))
            return
        logger.info("Processing lot {} in status {}".format(lot['id'], lot['status']))
        if lot['status'] in ['verification']:
            try:
                assets_available = self.check_assets(lot)
            except RequestFailed:
                logger.info("Due to fail in getting assets, lot {} is skipped".format(lot['id']))
            else:
                if assets_available:
                    self._add_assets_to_lot(lot)
                else:
                    self.patch_lot(lot, get_next_status(NEXT_STATUS_CHANGE, 'lot', lot['status'], 'fail'))
        elif lot['status'] == 'active.salable':
            if self.check_assets(lot, 'active'):
                is_all_auction_valid = all([a['status'] in HANDLED_AUCTION_STATUSES for a in lot['auctions']])
                if is_all_auction_valid and self.check_previous_auction(lot):
                    result = self._create_auction(lot)
                    if result:
                        auction, lot_auction_id = result
                        data = {
                            'auctionID': auction['data']['auctionID'],
                            'status': 'active',
                            'relatedProcessID': auction['data']['id']
                        }
                        self._patch_auction(data, lot['id'], lot_auction_id)
        else:
            self._process_lot_and_assets(
                lot,
                get_next_status(NEXT_STATUS_CHANGE, 'lot', lot['status'], 'finish'),
                get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'finish')
            )

    def get_next_auction(self, lot):
        auctions = filter(lambda a: a['status'] == 'scheduled', lot['auctions'])
        return auctions[0] if auctions else None

    def _dict_from_object(self, keys, obj, auction_index):
        to_patch = {}
        for to_key, from_key in keys.items():
            try:
                value = util.get(obj, from_key.format(auction_index))
            except KeyError:
                continue
            util.new(
                to_patch,
                to_key,
                value
            )
        return to_patch

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _post_auction(self, data, lot_id):
        auction = self.auction_client.create_auction(data)
        logger.info("Successfully created auction {} from lot {})".format(auction['data']['id'], lot_id))
        return auction

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _patch_auction(self, data, lot_id, auction_id):
        auction = self.lots_client.patch_resource_item_subitem(
            resource_item_id=lot_id,
            patch_data={'data': data},
            subitem_name='auctions',
            subitem_id=auction_id
        )
        logger.info("Successfully patched auction {} from lot {})".format(auction_id, lot_id))
        return auction

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _extract_transfer_token(self, lot_id):
        credentials = self.lots_client.extract_credentials(resource_item_id=lot_id)
        logger.info("Successfully extracted tranfer_token from lot {})".format(lot_id))
        return credentials['data']['transfer_token']

    def check_previous_auction(self, lot, status='unsuccessful'):
        for index, auction in enumerate(lot['auctions']):
            if auction['status'] == 'scheduled':
                if index == 0:
                    return True
                previous = lot['auctions'][index - 1]
                return previous['status'] == status
        else:
            return False

    def _create_auction(self, lot):
        auction_from_lot = self.get_next_auction(lot)
        if not auction_from_lot:
            return
        if auction_from_lot['procurementMethodType'] not in self.allowed_pmt:
            logger.warning(
                "Such procurementMethodType is not allowed to create {}. "
                "Allowed procurementMethodType {}".format(auction_from_lot['procurementMethodType'], self.allowed_pmt)
            )
            return
        auction = self._dict_from_object(KEYS_FOR_AUCTION_CREATE, lot, auction_from_lot['tenderAttempts'] - 1)
        lot_documents = deepcopy(lot.get('documents', []))
        for d in lot_documents:
            if d['documentOf'] == 'lot':
                d['relatedItem'] = lot['id']
        auction.setdefault('documents', []).extend(lot_documents)
        auction['status'] = 'pending.activation'
        try:
            auction['transfer_token'] = self._extract_transfer_token(lot['id'])
        except EXCEPTIONS as e:
            message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
            logger.error("Failed to extract transfer token from lot {} ({})".format(lot['id'], message))
            return
        now_date = datetime.now(TZ)
        if auction_from_lot['tenderAttempts'] == 1:
            auction['auctionPeriod'] = auction_from_lot['auctionPeriod']
            start_date = parse_date(auction_from_lot['auctionPeriod']['startDate'])

            re_obj = ACCELERATOR_RE.search(auction.get('procurementMethodDetails', ''))

            if re_obj and 'accelerator' in re_obj.groupdict():
                calc_date = calculate_business_date(
                    start=now_date,
                    delta= timedelta(days=20)/int(re_obj.groupdict()['accelerator']),
                    context=None,
                    working_days=False
                )
            else:
                calc_date = calculate_business_date(
                    start=now_date,
                    delta=timedelta(days=1),
                    context=None,
                    working_days=True
                )
            if start_date <= calc_date:
                auction['auctionPeriod']['startDate'] = calc_date.isoformat()

        else:
            auction['auctionPeriod'] = {
                    'startDate': (now_date + parse_duration(auction_from_lot['tenderingDuration'])).isoformat()
                }
        try:
            auction = self._post_auction({'data': auction}, lot['id'])
            return auction, auction_from_lot['id']
        except EXCEPTIONS as e:
            message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
            logger.error("Failed to create auction from lot {} ({})".format(lot['id'], message))
            return

    def _add_assets_to_lot(self, lot):
        result, patched_assets = self.patch_assets(
            lot,
            get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'pre'),
            lot['id']
        )
        if result is False:
            if patched_assets:
                logger.info("Assets {} will be repatched to 'pending'".format(patched_assets))
                result, _ = self.patch_assets({'assets': patched_assets},
                                              get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'fail'))
                if result is False:
                    log_broken_lot(
                        self.db,
                        logger,
                        self.errors_doc, lot,
                        'patching assets to {}'.format(get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'pre')))
        else:
            result, _ = self.patch_assets(
                lot,
                get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'finish'),
                lot['id']
            )
            if result is False:
                logger.info("Assets {} will be repatched to 'pending'".format(lot['assets']))
                result, _ = self.patch_assets(lot, get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'fail'))
                if result is False:
                    log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching assets to active')
            else:
                asset = self.assets_client.get_asset(lot['assets'][0]).data
                asset_decisions = []

                for dec in deepcopy(asset['decisions']):
                    dec.update(
                        {'relatedItem': asset['id']}
                    )
                    asset_decisions.append(dec)

                to_patch = {l_key: asset.get(a_key) for a_key, l_key in KEYS_FOR_LOKI_PATCH.items()}
                to_patch['decisions'] = lot['decisions'] + asset_decisions

                result = self.patch_lot(
                    lot,
                    get_next_status(NEXT_STATUS_CHANGE, 'lot', lot['status'], 'finish'),
                    to_patch
                )
                if result is False:
                    log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching lot to active.salable')

    def _process_lot_and_assets(self, lot, lot_status, asset_status):
        result, _ = self.patch_assets(lot, asset_status)
        if result:
            logger.info("Assets {} from lot {} will be patched to '{}'".format(lot['assets'], lot['id'], asset_status))
        else:
            logger.warning("Not valid assets {} in lot {}".format(lot['assets'], lot['id']))
        self.patch_lot(lot, lot_status)

    def check_lot(self, lot):
        """
        Makes GET request to openregistry by client, specified in configuration
        file, with lot id from lot object, passed as parameter.

        Args:
            lot: dictionary which contains some fields of lot
                 document from db: id, rev, status, assets, lotID.
        Returns:
            bool: True if request was successful and conditions were
                  satisfied, False otherwise.
        """
        try:
            actual_status = self.lots_client.get_lot(lot['id']).data.status
            logger.info('Successfully got lot {0}'.format(lot['id']))
        except ResourceNotFound as e:
            logger.error('Failed to get lot {0}: {1}'.format(lot['id'], e.message))
            return False
        except RequestFailed as e:
            logger.error('Failed to get lot {0}. Status code: {1}'.format(lot['id'], e.status_code))
            return False
        if lot['status'] != actual_status:
            logger.warning(
                "Lot {0} status ('{1}') already changed to ('{2}')".format(lot['id'], lot['status'], actual_status))
            return False
        if lot['status'] not in HANDLED_STATUSES:
            logger.warning("Lot {0} can not be processed in current status ('{1}')".format(lot['id'], lot['status']))
            return False
        return True

    def check_assets(self, lot, status='pending'):
        """
        Makes GET request to openregistry for every asset id in assets list
        from lot object, passed as parameter, with client specified in
        configuration file.

        Args:
            lot: dictionary which contains some fields of lot
                 document from db: id, rev, status, assets, lotID.
            status (str): status, in which assets are considered
                          as available. Defaults to 'pending'.

        Returns:
            bool: True if request was successful and conditions were
                  satisfied, False otherwise.

        Raises:
            RequestFailed: if RequestFailed was raised during request.
        """
        for asset_id in lot['assets']:
            try:
                asset = self.assets_client.get_asset(asset_id).data
                logger.info('Successfully got asset {}'.format(asset_id))
            except ResourceNotFound as e:
                logger.error('Failed to get asset {0}: {1}'.format(asset_id,
                                                                   e.message))
                return False
            except RequestFailed as e:
                logger.error('Failed to get asset {0}. Status code: {1}'.format(asset_id, e.status_code))
                raise RequestFailed('Failed to get assets')
            if asset.assetType not in self.allowed_asset_types:
                return False
            if asset.get('mode') != lot.get('mode'):
                return False
            related_lot_check = 'relatedLot' in asset and asset.relatedLot != lot['id']
            if related_lot_check or asset.status != status:
                return False
        return True

    def patch_assets(self, lot, status, related_lot=None):
        """
        Makes PATCH request to openregistry for every asset id in assets list
        from lot object, passed as parameter, with client specified in
        configuration file. PATCH request will replace values of fields 'status' and
        'relatedLot' of asset with values passed as parameters 'status' and
        'related_lot' respectively.

        Args:
            lot: dictionary which contains some fields of lot
                 document from db: id, rev, status, assets, lotID.
            status (str): status, assets will be patching to.
            related_lot: id of the lot, which unites assets, that
                         will be patched.

        Returns:
            tuple: (
                bool: True if request was successful and conditions were
                      satisfied, False otherwise.
                list: list with assets, which were successfully patched.
            )
        """
        patched_assets = []
        is_all_patched = True
        patch_data = {"status": status, "relatedLot": related_lot}
        for asset_id in lot['assets']:
            try:
                self._patch_single_asset(asset_id, patch_data)
            except EXCEPTIONS as e:
                is_all_patched = False
                message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
                logger.error("Failed to patch asset {} to {} ({})".format(asset_id, status, message))
            else:
                patched_assets.append(asset_id)
        return is_all_patched, patched_assets

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _patch_single_asset(self, asset_id, patch_data):
        self.assets_client.patch_asset(
            asset_id,
            {"data": patch_data}
        )
        logger.info("Successfully patched asset {} to {}".format(asset_id, patch_data['status']),
                    extra={'MESSAGE_ID': 'patch_asset'})

    def patch_lot(self, lot, status, extras={}):
        """
        Makes PATCH request to openregistry for lot id from lot object,
        passed as parameter, with client specified in configuration file.

        Args:
            lot: dictionary which contains some fields of lot
                 document from db: id, rev, status, assets, lotID.
            status (str): status, lot will be patching to.

        Returns:
            bool: True if request was successful and conditions were
                  satisfied, False otherwise.
        """
        try:
            patch_data = {"status": status}
            if extras:
                patch_data.update(extras)
            self.lots_client.patch_lot(lot['id'], {"data": patch_data})
        except EXCEPTIONS as e:
            message = e.message
            if e.status_code >= 500:
                message = 'Server error: {}'.format(e.status_code)
            logger.error("Failed to patch lot {} to {} ({})".format(lot['id'], status, message))
            return False
        else:
            logger.info("Successfully patched lot {} to {}".format(lot['id'], status),
                        extra={'MESSAGE_ID': 'patch_lot'})
            return True
