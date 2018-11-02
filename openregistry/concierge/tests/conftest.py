# -*- coding: utf-8 -*-
import couchdb
import logging
import os
import pytest
import uuid

from StringIO import StringIO

from openregistry.concierge.worker import BotWorker, logger as LOGGER


DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = os.environ.get('DB_PORT', '5984')
DB_USER = os.environ.get('DB_USER', '')
DB_PASS = os.environ.get('DB_PASS', '')
TEST_CONFIG = {
    "db": {
        "host": DB_HOST,
        "name": "lots_db_{}".format(uuid.uuid4().hex),
        "port": DB_PORT,
        "login": DB_USER,
        "password": DB_PASS,
        "filter": "lots/status"
    },
    'lots_mapping': {
        'type': 'lazy'
    },
    "errors_doc": "broken_lots",
    "time_to_sleep": 0.0002,
    "lots": {
        "api": {
            "url": "http://192.168.50.9",
            "token": "concierge",
            "version": 0
        },
        "basic": {
            'aliases': ["basic"],
            'assets': {
                "basic": ["basic"],
                "compound": ["compound"],
                "claimRights": ["claimRights"]
            }
        },
        "loki": {
            'aliases': ["loki"],
            'assets': {
                "bounce": ["bounce", "domain"]
            }
        }
    },
    "assets": {
        "api": {
            "url": "http://192.168.50.9",
            "token": "concierge",
            "version": 0
        }
    }
}


@pytest.fixture(scope='function')
def db(request):

    server = couchdb.Server("http://{host}:{port}".format(
        **TEST_CONFIG['db']
    ))
    name = TEST_CONFIG['db']['name']

    def delete():
        del server[name]

    if name in server:
        delete()

    db = server.create(name)
    db['_design/lots'] = {}
    db.save(db['_design/lots'])

    request.addfinalizer(delete)


@pytest.fixture(scope='function')
def bot(mocker, db):
    mocker.patch('openregistry.concierge.utils.LotsClient', autospec=True)
    mocker.patch('openregistry.concierge.utils.AssetsClient', autospec=True)
    mocker.patch('openregistry.concierge.utils.AuctionsClient', autospec=True)
    processing_loki = mocker.patch('openregistry.concierge.worker.ProcessingLoki', autospec=True)
    processing_loki = processing_loki.return_value
    processing_loki.handled_lot_types = ['loki']
    processing_basic = mocker.patch('openregistry.concierge.worker.ProcessingBasic', autospec=True)
    processing_basic = processing_basic.return_value
    processing_basic.handled_lot_types = ['basic']
    return BotWorker(TEST_CONFIG)


class LogInterceptor(object):
    def __init__(self, logger):
        logger.setLevel(logging.INFO)
        self.log_capture_string = StringIO()
        self.test_handler = logging.StreamHandler(self.log_capture_string)
        self.test_handler.setLevel(logging.INFO)
        logger.addHandler(self.test_handler)


@pytest.fixture(scope='function')
def logger():
    return LogInterceptor(LOGGER)


class AlmostAlwaysTrue(object):
    def __init__(self, total_iterations=1):
        self.total_iterations = total_iterations
        self.current_iteration = 0

    def __nonzero__(self):
        if self.current_iteration < self.total_iterations:
            self.current_iteration += 1
            return bool(1)
        return bool(0)


@pytest.fixture(scope='function')
def almost_always_true():
    return AlmostAlwaysTrue
