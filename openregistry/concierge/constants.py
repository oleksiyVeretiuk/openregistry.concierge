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
        }
    },
    "assets": {
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


BASIC_ASSETS_WORKLOW = {
    'verification': {
        'pre': 'verification',
        'finish': 'active',
        'fail': 'pending'
    },
    'recomposed': {
        'pre': '',
        'finish': 'pending',
        'fail': ''
    },
    'pending.dissolution': {
        'pre': '',
        'finish': 'pending',
        'fail': ''
    },
    'pending.sold': {
        'pre': '',
        'finish': 'complete',
        'fail': ''
    }
}

ASSET_TO_LOT_TYPE = {
    'basic': ['basic', 'compound', 'claimrights'],
    'loki': ['bounce']
}

NEXT_STATUS_CHANGE = {
    'lot': {
        'loki': {
            'composing': {
                'pre': '',
                'finish': 'pending',
                'fail': 'invalid'
            },
        },
        'basic': {
            'verification': {
                'pre': '',
                'finish': 'active.salable',
                'fail': 'pending'
            },
            'recomposed': {
                'pre': '',
                'finish': 'pending',
                'fail': ''
            },
            'pending.dissolution': {
                'pre': '',
                'finish': 'dissolved',
                'fail': ''
            },
            'pending.sold': {
                'pre': '',
                'finish': 'sold',
                'fail': ''
            }
        }
    },
    'asset': {
        'basic': BASIC_ASSETS_WORKLOW,
        'loki': BASIC_ASSETS_WORKLOW,
    }
}