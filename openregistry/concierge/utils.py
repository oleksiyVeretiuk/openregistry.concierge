# -*- coding: utf-8 -*-
from couchdb import Server, Session
from socket import error
from logging import addLevelName, Logger

from openprocurement_client.resources.lots import LotsClient
from openprocurement_client.resources.assets import AssetsClient

from .design import sync_design

CONTINUOUS_CHANGES_FEED_FLAG = True
STATUS_FILTER = """function(doc, req) {
  if(
    doc.status == "verification" || 
    doc.status == "pending.dissolution" || 
    doc.status == "recomposed" || 
    doc.status == "pending.sold" || 
    doc.status == "composing" ||
    doc.status == "pending.deleted") {
        return true;
    }
    return false;
}"""

addLevelName(25, 'CHECK')


def check(self, msg, exc=None, *args, **kwargs):
    self.log(25, msg)
    if exc:
        self.error(exc, exc_info=True)


Logger.check = check


class ConfigError(Exception):
    pass


def prepare_couchdb(couch_url, db_name, logger, errors_doc):
    server = Server(couch_url, session=Session(retry_delays=range(10)))
    try:
        if db_name not in server:
            db = server.create(db_name)
        else:
            db = server[db_name]

        broken_lots = db.get(errors_doc, None)
        if broken_lots is None:
            db[errors_doc] = {}

        prepare_couchdb_filter(db, 'lots', 'status', STATUS_FILTER, logger)

    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    sync_design(db)
    return db


def prepare_couchdb_filter(db, doc, filter_name, filter, logger):
    design_doc = db['_design/{}'.format(doc)]
    if not design_doc.get('filters', ''):
        design_doc['filters'] = {}
    if filter_name not in design_doc['filters']:
        design_doc['filters'][filter_name] = filter
        logger.debug('Successfully created {0}/{1} filter.'.format(doc, filter_name))
    elif design_doc['filters'][filter_name] != filter:
        design_doc['filters'][filter_name] = filter
        logger.debug('Successfully updated {0}/{1} filter.'.format(doc, filter_name))
    else:
        logger.debug('Filter {0}/{1} already exists.'.format(doc, filter_name))
    db.save(design_doc)


def continuous_changes_feed(db, logger, limit=100, filter_doc='lots/status'):

    last_seq_id = 0
    while CONTINUOUS_CHANGES_FEED_FLAG:
        try:
            data = db.changes(include_docs=True, since=last_seq_id, limit=limit, filter=filter_doc)
        except error as e:
            logger.error('Failed to get lots from DB: [Errno {}] {}'.format(e.errno, e.strerror))
            break
        last_seq_id = data['last_seq']
        if len(data['results']) != 0:
            for row in data['results']:
                item = {
                    'id': row['doc']['_id'],
                    'rev': row['doc']['_rev'],
                    'status': row['doc']['status'],
                    'assets': row['doc']['assets'],
                    'lotID': row['doc']['lotID'],
                    'lotType': row['doc']['lotType'],
                    'decisions': row['doc'].get('decisions')
                }
                yield item
        else:
            break


def log_broken_lot(db, logger, doc, lot, message):
    lot['resolved'] = False
    lot['message'] = message
    try:
        doc[lot['id']] = lot
        db.save(doc)
    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    else:
        return doc


def resolve_broken_lot(db, logger, doc, lot):
    try:
        doc[lot['id']]['resolved'] = True
        doc[lot['id']]['rev'] = lot['rev']
        db.save(doc)
    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    else:
        return doc


def init_clients(config, logger):
    clients_from_config = {
        'lots_client': {'section': 'lots', 'client_instance': LotsClient},
        'assets_client': {'section': 'assets', 'client_instance': AssetsClient}
    }
    result = ''
    exceptions = []

    for key, item in clients_from_config.items():
        section = item['section']
        try:
            client = item['client_instance'](
                key=config[section]['api']['token'],
                host_url=config[section]['api']['url'],
                api_version=config[section]['api']['version']
            )
            clients_from_config[key] = client
            result = ('ok', None)
        except Exception as e:
            exceptions.append(e)
            result = ('failed', e)
        logger.check('{} - {}'.format(key, result[0]), result[1])
    try:
        if config['db'].get('login', '') \
                and config['db'].get('password', ''):
            db_url = "http://{login}:{password}@{host}:{port}".format(
                **config['db']
            )
        else:
            db_url = "http://{host}:{port}".format(**config['db'])

        clients_from_config['db'] = prepare_couchdb(db_url, config['db']['name'], logger, config['errors_doc'])
        result = ('ok', None)
    except Exception as e:
        exceptions.append(e)
        result = ('failed', e)
    logger.check('couchdb - {}'.format(result[0]), result[1])

    if exceptions:
        raise exceptions[0]

    return clients_from_config