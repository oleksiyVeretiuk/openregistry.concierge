# -*- coding: utf-8 -*-
import logging
import logging.config
from retrying import retry

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
)
from openregistry.concierge.basic.constants import (
    NEXT_STATUS_CHANGE
)

logger = logging.getLogger('openregistry.concierge.worker')

EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)

HANDLED_STATUSES = ('verification', 'recomposed', 'pending.dissolution', 'pending.sold', 'pending.deleted')


class ProcessingBasic(object):

    def __init__(self, config, clients, errors_doc):
        """
        Args:
            config: dictionary with configuration data
        """
        self.allowed_asset_types = []
        self.handled_lot_types = []
        self.config = config

        self._register_allowed_assets()
        self._register_handled_lot_types()

        for key, item in clients.items():
            setattr(self, key, item)
        self.errors_doc = errors_doc

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
        else:
            self._process_lot_and_assets(
                lot,
                get_next_status(NEXT_STATUS_CHANGE, 'lot', lot['status'], 'finish'),
                get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'finish')
            )

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
                        'patching assets to {}'.format(
                            get_next_status(NEXT_STATUS_CHANGE, 'asset', lot['status'], 'pre')
                        ))
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
                result = self.patch_lot(
                    lot,
                    get_next_status(NEXT_STATUS_CHANGE, 'lot', lot['status'], 'finish'),
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
