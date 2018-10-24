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
from openregistry.concierge.constants import TZ, AUCTION_CREATE_MESSAGE_ID
from openregistry.concierge.loki.utils import (
    calculate_business_date,
    log_assets_message
)
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
            logger.info("Skipping Lot {}".format(lot['id']), extra={'MESSAGE_ID': 'skip_lot'})
            return
        logger.info("Processing Lot {} in status {}".format(lot['id'], lot['status']), extra={'MESSAGE_ID': 'process_loki_lot'})
        if lot['status'] in ['verification']:
            try:
                assets_available = self.check_assets(lot)
            except RequestFailed:
                logger.info("Due to fail in getting assets, Lot {} is skipped".format(lot['id']))
            else:
                if assets_available:
                    result = self._add_assets_to_lot(lot)
                    if result:
                        self.lots_mapping.put(lot['id'], True)
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
                        self.lots_mapping.put(lot['id'], True)
                    elif result is False:
                        self._process_lot_and_assets(
                            lot,
                            'composing',
                            'pending'
                        )
        else:
            result = self._process_lot_and_assets(
                lot,
                get_next_status(NEXT_STATUS_CHANGE, 'lot', lot['status'], 'finish'),
                get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'finish')
            )
            if result:
                self.lots_mapping.put(lot['id'], True)

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
        logger.info(
            "Successfully created auction {} from Lot {})".format(auction['data']['id'], lot_id),
            extra={'MESSAGE_ID': AUCTION_CREATE_MESSAGE_ID}
        )
        return auction

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _patch_auction(self, data, lot_id, auction_id):
        auction = self.lots_client.patch_resource_item_subitem(
            resource_item_id=lot_id,
            patch_data={'data': data},
            subitem_name='auctions',
            subitem_id=auction_id
        )
        logger.info("Successfully patched Lot.auction {} from Lot {})".format(auction_id, lot_id))
        return auction

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _patch_related_process(self, data, lot_id, related_process_id):
        related_process = self.lots_client.patch_resource_item_subitem(
            resource_item_id=lot_id,
            patch_data={'data': data},
            subitem_name='related_processes',
            subitem_id=related_process_id
        )
        logger.info("Successfully patched Lot.relatedProcess {} from Lot {})".format(related_process_id, lot_id))
        return related_process

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _create_asset_related_process(self, asset_id, data):
        related_process = self.assets_client.create_resource_item_subitem(
            resource_item_id=asset_id,
            subitem_obj={'data': data},
            subitem_name='related_processes',
        )
        logger.info("Successfully post Asset.relatedProcess from Asset {})".format(asset_id))
        return related_process

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _remove_asset_lot_related_process(self, asset_id, related_process_id):
        related_process = self.assets_client.delete_resource_item_subitem(
            resource_item_id=asset_id,
            subitem_name='related_processes',
            subitem_id=related_process_id
        )
        logger.info("Successfully remove Asset.relatedProcess from Asset {})".format(asset_id))
        return related_process

    def add_related_process_to_assets(self, lot):
        related_process_type_asset = [rP for rP in lot['relatedProcesses'] if rP['type'] == 'asset']
        patched_rPs = []
        is_all_patched = True
        lot_related_process_data = self.make_lot_related_process(lot)
        for rP in related_process_type_asset:
            try:
                created_rP = self._create_asset_related_process(rP['relatedProcessID'], lot_related_process_data)
                created_rP['asset_parent'] = rP['relatedProcessID']
            except EXCEPTIONS as e:
                is_all_patched = False
                message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
                logger.error(
                    "Failed to add relatedProcess of lot {} in Asset {} ({})".format(
                        lot['id'],
                        rP['relatedProcessID'],
                        message
                    )
                )
            else:
                patched_rPs.append(created_rP)
        return is_all_patched, patched_rPs

    def clean_asset_related_processes(self, lot, assets_rPs):
        is_all_patched = True
        for rP in assets_rPs:
            try:
                self._remove_asset_lot_related_process(rP['asset_parent'], rP['id'])
            except EXCEPTIONS as e:
                is_all_patched = False
                message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
                logger.error("Failed to clean relatedProcess {} in Asset {} ({})".format(rP['id'], lot['id'], message))
        return is_all_patched

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _extract_transfer_token(self, lot_id):
        credentials = self.lots_client.extract_credentials(resource_item_id=lot_id)
        logger.info("Successfully extracted tranfer_token from Lot {})".format(lot_id))
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
            logger.error("Failed to extract transfer token from Lot {} ({})".format(lot['id'], message))
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
            logger.error("Failed to create auction from Lot {} ({})".format(lot['id'], message), extra={'MESSAGE_ID': 'failed_to_create_auction'})
            return False

    def _add_assets_to_lot(self, lot):
        result, patched_assets = self.patch_assets(
            lot,
            get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'pre'),
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
            return False
        else:
            result, _ = self.patch_assets(
                lot,
                get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'finish'),
            )
            if result is False:
                log_assets_message(logger, 'info', "Assets {assets} will be repatched to 'pending'", lot['relatedProcesses'])
                result, _ = self.patch_assets(lot, get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'fail'))
                if result is False:
                    log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching assets to active')
                return False
            else:
                result, patched_rPs = self._patch_lot_asset_related_processes(lot)

                if not result and patched_rPs:
                    lot_with_patched_rPs = {
                        'id': lot['id'],
                        'relatedProcesses': patched_rPs
                    }
                    self._patch_lot_asset_related_processes(lot_with_patched_rPs, cleanup=True)
                    self.patch_assets(lot, 'pending')
                    return False
                elif not result:
                    self.patch_assets(lot, 'pending')
                    return False

                result, patched_rPs = self.add_related_process_to_assets(lot)
                if not result and patched_rPs:
                    self.clean_asset_related_processes(lot, patched_rPs)
                    self._patch_lot_asset_related_processes(lot, cleanup=True)
                    self.patch_assets(lot, 'pending')
                    return False
                elif not result:
                    self._patch_lot_asset_related_processes(lot, cleanup=True)
                    self.patch_assets(lot, 'pending')
                    return False

                asset = self.assets_client.get_asset(lot['relatedProcesses'][0]['relatedProcessID']).data

                to_patch = {l_key: asset.get(a_key) for a_key, l_key in KEYS_FOR_LOKI_PATCH.items()}
                to_patch['decisions'] = deepcopy(lot['decisions'])

                for dec in deepcopy(asset['decisions']):
                    dec.update(
                        {'relatedItem': asset['id']}
                    )
                    to_patch['decisions'].append(dec)

                result = self.patch_lot(
                    lot,
                    get_next_status(NEXT_STATUS_CHANGE, 'lot', lot['status'], 'finish'),
                    to_patch
                )
                if result is False:
                    self._process_lot_and_assets(lot, 'composing', 'pending')
                    self._patch_lot_asset_related_processes(lot, cleanup=True)
                    log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching Lot to pending')
                    return False
                return True

    def _patch_lot_asset_related_processes(self, lot, cleanup=False):
        related_processes = [rP for rP in lot['relatedProcesses'] if rP['type'] == 'asset']
        patched_rPs = []
        is_all_patched = True
        for rP in related_processes:
            asset = self.assets_client.get_asset(rP['relatedProcessID']).data
            if not cleanup:
                data = {'identifier': asset['assetID']}
            else:
                data = {'identifier': ''}
            try:
                self._patch_related_process(data, lot['id'], rP['id'])
            except EXCEPTIONS as e:
                is_all_patched = False
                message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
                logger.error("Failed to patch relatedProcess {} in Lot {} ({})".format(rP['id'], lot['id'], message))
            else:
                patched_rPs.append(rP)
        return is_all_patched, patched_rPs

    def _process_lot_and_assets(self, lot, lot_status, asset_status):
        result, _ = self.patch_assets(lot, asset_status)
        if result:
            msg = "Assets {assets} from Lot {id} will be patched to '{asset_status}'".format(
                assets='{assets}',
                id=lot['id'],
                asset_status=asset_status
            )
            log_assets_message(logger, 'info', msg, lot['relatedProcesses'])
        else:
            msg = "Not valid assets {assets} in Lot {id}".format(assets='{assets}', id=lot['id'])
            log_assets_message(logger, 'warning', msg, lot['relatedProcesses'])
        result = self.patch_lot(lot, lot_status)
        return result

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
            logger.info('Successfully got Lot {0}'.format(lot['id']))
        except ResourceNotFound as e:
            logger.error('Failed to get Lot {0}: {1}'.format(lot['id'], e.message))
            return False
        except RequestFailed as e:
            logger.error('Failed to get Lot {0}. Status code: {1}'.format(lot['id'], e.status_code))
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
        assets = [rP['relatedProcessID'] for rP in lot['relatedProcesses'] if rP['type'] == 'asset']
        for asset_id in assets:
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
            related_lot = self.get_asset_related_lot(asset)
            related_lot_check = related_lot and related_lot.relatedProcessID != lot['id']
            if related_lot_check or asset.status != status:
                return False
        return True

    def get_asset_related_lot(self, asset):
        for rP in asset.get('relatedProcesses', []):
            if rP.type == 'lot':
                return rP

    def make_lot_related_process(self, lot):
        return {
            'type': 'lot',
            'relatedProcessID': lot['id'],
            'identifier': lot['lotID']
        }

    def patch_assets(self, lot, status):
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
        patch_data = {"status": status}
        assets = [rP['relatedProcessID'] for rP in lot['relatedProcesses'] if rP['type'] == 'asset']
        for asset_id in assets:
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
            logger.error("Failed to patch Lot {} to {} ({})".format(lot['id'], status, message))
            return False
        else:
            logger.info("Successfully patched Lot {} to {}".format(lot['id'], status),
                        extra={'MESSAGE_ID': 'patch_lot'})
            return True
