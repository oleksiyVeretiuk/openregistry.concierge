# -*- coding: utf-8 -*-
import os
from pytz import timezone

DEFAULTS = {
    "db": {
        "host": "127.0.0.1",
        "name": "lots_db",
        "port": "5984",
        "login": "",
        "password": "",
        "filter": "lots/status"
    },
    "errors_doc": "broken_lots",
    "time_to_sleep": 10,
    "lots": {
        "api": {
            "url": "http://0.0.0.0:6543",
            "token": "concierge",
            "version": 0.1
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
            "planned_pmt": [],
            'aliases': ["loki"],
            'assets': {
                "bounce": ["bounce", "domain"]
            }
        }
    },
    "assets": {
        "api": {
            "url": "http://0.0.0.0:6543",
            "token": "concierge",
            "version": 0.1
        }
    },
    "auctions": {
        "api": {
            "url": "http://0.0.0.0:6543",
            "token": "concierge",
            "version": 0.1
        }
    },
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
        "openregistry.concierge.worker": {
            "handlers": ["console"],
            "propagate": "no",
            "level": "DEBUG"
        },
        "": {
            "handlers": ["console"],
            "level": "DEBUG"
        }
    }
}


TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')

AUCTION_CREATE_MESSAGE_ID = 'create_auction'
