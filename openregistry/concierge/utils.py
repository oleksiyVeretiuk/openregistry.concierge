# -*- coding: utf-8 -*-
from couchdb import Server, Session
from socket import error
from logging import addLevelName, Logger

from openprocurement_client.resources.lots import LotsClient
from openprocurement_client.resources.auctions import AuctionsClient
from openprocurement_client.resources.assets import AssetsClient
from openprocurement_client.exceptions import (
    Forbidden,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity,
    Conflict,
    PreconditionFailed,
)

from .design import sync_design

CONTINUOUS_CHANGES_FEED_FLAG = True
EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)
STATUS_FILTER = """function(doc, req) {
  if(%s) {
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


def prepare_couchdb(couch_url, db_name, logger, errors_doc, couchdb_filter):
    server = Server(couch_url, session=Session(retry_delays=range(10)))
    try:
        if db_name not in server:
            db = server.create(db_name)
        else:
            db = server[db_name]

        broken_lots = db.get(errors_doc, None)
        if broken_lots is None:
            db[errors_doc] = {}

        prepare_couchdb_filter(db, 'lots', 'status', couchdb_filter, logger)

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
                item = row['doc']
                item.update({
                    'id': row['doc']['_id'],
                    'rev': row['doc']['_rev'],
                })
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


def init_clients(config, logger, couchdb_filter):
    clients_from_config = {
        'lots_client': {'section': 'lots', 'client_instance': LotsClient},
        'assets_client': {'section': 'assets', 'client_instance': AssetsClient},
        'auction_client': {'section': 'assets', 'client_instance': AuctionsClient}
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

        clients_from_config['db'] = prepare_couchdb(db_url, config['db']['name'], logger, config['errors_doc'], couchdb_filter)
        result = ('ok', None)
    except Exception as e:
        exceptions.append(e)
        result = ('failed', e)
    logger.check('couchdb - {}'.format(result[0]), result[1])

    if exceptions:
        raise exceptions[0]

    return clients_from_config


def retry_on_error(exception):
    if isinstance(exception, EXCEPTIONS) and (exception.status_code >= 500 or exception.status_code in [409, 412, 429]):
        return True
    return False


def get_next_status(status_mapping, resource, lotStatus, action):
    return status_mapping[resource][lotStatus][action]


def create_certain_condition(place_to_check, items, condition):
    '''

    :param place_to_check: actually a variable or object in filter that should be checked
    :param items: values to check with value from place_to_check
    :param condition: type of condition to chain checks
    :return: condition in string

    >>> lot_aliases = ['loki', 'anotherLoki']
    >>> create_certain_condition('variable', lot_aliases, '&&')
    '(variable == "loki" && variable == "anotherLoki")'

    '''
    result = ''
    for item in items:
        if result:
            result = result + ' {} '.format(condition)
        result = result + place_to_check + ' == "{}"'.format(item)
    return '({})'.format(result) if result else ''


def create_filter_condition(lot_aliases, handled_statuses):
    '''
    :param lot_aliases: list of lot aliases
    :param handled_statuses: list of status that should be handled for certail lotType
    :return: condition that will be used in filter for couchdb

    >>> lot_aliases = ['loki', 'anotherLoki']
    >>> handled_statuses = ['pending', 'verification']
    >>> create_filter_condition(lot_aliases, handled_statuses)
    '(doc.lotType == "loki" || doc.lotType == "anotherLoki") && (doc.status == "pending" || doc.status == "verification")'
    >>> create_filter_condition(lot_aliases, [])
    '(doc.lotType == "loki" || doc.lotType == "anotherLoki")'
    >>> create_filter_condition([], handled_statuses)
    '(doc.status == "pending" || doc.status == "verification")'

    '''

    conditions = []

    conditions.append(create_certain_condition('doc.lotType', lot_aliases, '||'))
    conditions.append(create_certain_condition('doc.status', handled_statuses, '||'))

    filter_condition = ''

    for condition in conditions:
        if not condition:
            continue

        if filter_condition:
            filter_condition = filter_condition + ' && '

        filter_condition = filter_condition + condition

    return filter_condition
