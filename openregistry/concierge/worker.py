# -*- coding: utf-8 -*-
import argparse
import logging
import logging.config
import os
import time
import yaml
from retrying import retry

from openprocurement_client.resources.lots import LotsClient
from openprocurement_client.resources.assets import AssetsClient
from openprocurement_client.exceptions import (
    Forbidden,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity,
    Conflict,
    PreconditionFailed,
)

from .utils import (
    resolve_broken_lot,
    continuous_changes_feed,
    log_broken_lot,
    prepare_couchdb
)

logger = logging.getLogger(__name__)

EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)

HANDLED_STATUSES = ('verification', 'recomposed', 'pending.dissolution')


def retry_on_error(exception):
    if isinstance(exception, EXCEPTIONS) and (exception.status_code >= 500 or exception.status_code in [409, 412, 429]):
        return True
    return False


class BotWorker(object):
    def __init__(self, config):
        """
        Args:
            config: dictionary with configuration data
        """
        self.config = config
        self.sleep = self.config['time_to_sleep']
        self.lots_client = LotsClient(
            key=self.config['lots']['api']['token'],
            host_url=self.config['lots']['api']['url'],
            api_version=self.config['lots']['api']['version']
        )
        self.assets_client = AssetsClient(
            key=self.config['assets']['api']['token'],
            host_url=self.config['assets']['api']['url'],
            api_version=self.config['assets']['api']['version']
        )
        if self.config['db'].get('login', '') \
                and self.config['db'].get('password', ''):
            db_url = "http://{login}:{password}@{host}:{port}".format(
                **self.config['db']
            )
        else:
            db_url = "http://{host}:{port}".format(**self.config['db'])

        self.db = prepare_couchdb(db_url, self.config['db']['name'], logger, self.config['errors_doc'])
        self.errors_doc = self.db.get(self.config['errors_doc'])
        self.patch_log_doc = self.db.get('patch_requests')

    def run(self):
        """
        Starts an infinite while loop in which lots, received from db,
        are passing to 'process_lots' method for further processing.

        In case if value of 'id' field of received lot matches value of field
        'id' of one on lots, marked as broken and uploaded to db document,
        specified in configuration file, checks value of 'rev' field of both
        lots. 'rev' field specifying version of lot document in db. If lots
        values of 'rev' field is identical (lot have not been changed since
        upload to document and marked as broken), received lot will be skipped
        and not passed to 'process_lots' method. If value of this field is differ,
        field 'resolved' of broken lot in db document will be changed from 'false'
        to 'true' and lot will be passed to 'process_lots' method.

        Returns:
            None
        """
        logger.info("Starting worker")
        while True:
            for lot in self.get_lot():
                broken_lot = self.errors_doc.get(lot['id'], None)
                if broken_lot:
                    if broken_lot['rev'] == lot['rev']:
                        continue
                    else:
                        errors_doc = resolve_broken_lot(self.db, logger, self.errors_doc, lot)
                        self.process_lots(errors_doc[lot['id']])
                else:
                    self.process_lots(lot)
            time.sleep(self.sleep)

    def get_lot(self):
        """
        Receiving lots from db, which are filtered by CouchDB filter
        function specified in the configuration file.

        Returns:
            generator: Generator object with the received lots.
        """
        logger.info('Getting Lots')
        return continuous_changes_feed(
            self.db, logger,
            filter_doc=self.config['db']['filter']
        )

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
        if lot['status'] == 'verification':
            try:
                assets_available = self.check_assets(lot)
            except RequestFailed:
                logger.info("Due to fail in getting assets, lot {} is skipped".format(lot['id']))
            else:
                if assets_available:
                    result, patched_assets = self.patch_assets(lot, 'verification', lot['id'])
                    if result is False:
                        if patched_assets:
                            logger.info("Assets {} will be repatched to 'pending'".format(patched_assets))
                            result, _ = self.patch_assets({'assets': patched_assets}, 'pending')
                            if result is False:
                                log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching assets to verification')
                    else:
                        result, _ = self.patch_assets(lot, 'active', lot['id'])
                        if result is False:
                            logger.info("Assets {} will be repatched to 'pending'".format(lot['assets']))
                            result, _ = self.patch_assets(lot, 'pending')
                            if result is False:
                                log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching assets to active')
                        else:
                            result = self.patch_lot(lot, "active.salable")
                            if result is False:
                                log_broken_lot(self.db, logger, self.errors_doc, lot, 'patching lot to active.salable')
                else:
                    self.patch_lot(lot, "pending")
        elif lot['status'] == 'pending.dissolution':
            result, _ = self.patch_assets(lot, 'pending')
            if result:
                logger.info("Assets {} from lot {} will be patched to 'pending'".format(lot['assets'], lot['id']))
            else:
                logger.warning("Not valid assets {} in lot {}".format(lot['assets'], lot['id']))
            self.patch_lot(lot, 'dissolved')
        elif lot['status'] == 'recomposed':
            result, _ = self.patch_assets(lot, 'pending')
            if result:
                logger.info("Assets {} from lot {} will be patched to 'pending'".format(lot['assets'], lot['id']))
            else:
                logger.warning("Not valid assets {} in lot {}".format(lot['assets'], lot['id']))
            self.patch_lot(lot, 'pending')

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
            logger.error('Falied to get lot {0}: {1}'.format(lot['id'], e.message))
            return False
        except RequestFailed as e:
            logger.error('Falied to get lot {0}. Status code: {1}'.format(lot['id'], e.status_code))
            return False
        if lot.status != actual_status:
            logger.warning("Lot {0} status ('{1}') already changed to ('{3}')".format(lot.id, lot.status, actual_status))
            return False
        if lot.status not in HANDLED_STATUSES:
            logger.warning("Lot {0} can not be processed in current status ('{1}')".format(lot.id, lot.status))
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
                logger.error('Falied to get asset {0}: {1}'.format(asset_id,
                                                                   e.message))
                return False
            except RequestFailed as e:
                logger.error('Falied to get asset {0}. Status code: {1}'.format(asset_id, e.status_code))
                raise RequestFailed('Failed to get assets')
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
        data = {"data": {"status": status, "relatedLot": related_lot}}
        is_all_patched = True
        for asset_id in lot['assets']:
            try:
                self._patch_single_asset(asset_id, data)
            except EXCEPTIONS as e:
                is_all_patched = False
                message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
                logger.error("Failed to patch asset {} to {} ({})".format(asset_id, status, message))
            else:
                patched_assets.append(asset_id)
        return is_all_patched, patched_assets

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _patch_single_asset(self, asset_id, data):
        self.assets_client.patch_asset(asset_id, data)
        logger.info("Successfully patched asset {} to {}".format(asset_id, data['data']['status']),
                    extra={'MESSAGE_ID': 'patch_asset'})
        return True, asset_id

    def patch_lot(self, lot, status):
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
            self.lots_client.patch_lot(lot['id'], {"data": {"status": status}})
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


def main():
    parser = argparse.ArgumentParser(description='---- OpenRegistry Concierge ----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_object:
            config = yaml.load(config_object.read())
        logging.config.dictConfig(config)
        BotWorker(config).run()


if __name__ == "__main__":
    main()
