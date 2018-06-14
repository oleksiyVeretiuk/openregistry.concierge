# -*- coding: utf-8 -*-
import argparse
import logging
import logging.config
import os
import time
import yaml

from openprocurement_client.exceptions import (
    Forbidden,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity,
    Conflict,
    PreconditionFailed,
)

from openregistry.concierge.utils import (
    resolve_broken_lot,
    continuous_changes_feed,
    init_clients
)
from openregistry.concierge.loki.processing import ProcessingLoki
from openregistry.concierge.basic.processing import ProcessingBasic
from openregistry.concierge.constants import (
    DEFAULTS,
)

logger = logging.getLogger(__name__)

EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)

IS_BOT_WORKING = True


class BotWorker(object):
    def __init__(self, config):
        """
        Args:
            config: dictionary with configuration data
        """
        self.lot_type_processing_configurator = {}
        self.config = config

        created_clients = init_clients(config, logger)

        for key, item in created_clients.items():
            setattr(self, key, item)
        self.errors_doc = self.db.get(self.config['errors_doc'])

        if config['lots'].get('loki'):
            process_loki = ProcessingLoki(config['lots']['loki'], created_clients, self.errors_doc)
            self._register_aliases(process_loki)
        if config['lots'].get('basic'):
            process_basic = ProcessingBasic(config['lots']['basic'], created_clients, self.errors_doc)
            self._register_aliases(process_basic)

        self.sleep = self.config['time_to_sleep']
        self.patch_log_doc = self.db.get('patch_requests')

    def _register_aliases(self, processing):
        for lt in processing.handled_lot_types:
            self.lot_type_processing_configurator[lt] = processing

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
        while IS_BOT_WORKING:
            for lot in self.get_lot():
                if lot['lotType'] not in self.lot_type_processing_configurator:
                    logger.warning('Such lotType %s is not supported by this concierge configuration' % lot['lotType'])
                    continue
                broken_lot = self.errors_doc.get(lot['id'], None)
                if broken_lot:
                    if broken_lot['rev'] == lot['rev']:
                        continue
                    else:
                        errors_doc = resolve_broken_lot(self.db, logger, self.errors_doc, lot)
                        self.lot_type_processing_configurator[lot['lotType']].process_lots(errors_doc[lot['id']])
                else:
                    self.lot_type_processing_configurator[lot['lotType']].process_lots(lot)
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


def main():
    parser = argparse.ArgumentParser(description='---- OpenRegistry Concierge ----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    parser.add_argument('-t', dest='check', action='store_const',
                        const=True, default=False,
                        help='Clients check only')
    params = parser.parse_args()
    config = {}
    if os.path.isfile(params.config):
        with open(params.config) as config_object:
            config = yaml.load(config_object.read())
        logging.config.dictConfig(config)
    DEFAULTS.update(config)
    worker = BotWorker(DEFAULTS)
    if params.check:
        exit()
    worker.run()


if __name__ == "__main__":
    main()
