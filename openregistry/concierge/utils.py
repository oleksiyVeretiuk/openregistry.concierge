# -*- coding: utf-8 -*-
from couchdb import Server, Session
from socket import error

from .design import sync_design

CONTINUOUS_CHANGES_FEED_FLAG = True
STATUS_FILTER = """function(doc, req) {
  if(doc.status == "verification" || doc.status == "pending.dissolution" || doc.status == "recomposed") {return true;}
    return false;
}"""


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
                    'lotID': row['doc']['lotID']
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
