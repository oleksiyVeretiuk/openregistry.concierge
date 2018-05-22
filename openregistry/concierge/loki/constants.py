LOKI_ASSETS_WORKFLOW = {
    'verification': {
        'pre': 'verification',
        'finish': 'active',
        'fail': 'pending'
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
    },
    'pending.deleted': {
        'pre': '',
        'finish': 'pending',
        'fail': ''
    }
}


LOTS_WORKFLOW = {
    'verification': {
        'pre': '',
        'finish': 'pending',
        'fail': 'invalid'
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
    },
    'pending.deleted': {
        'pre': '',
        'finish': 'deleted',
        'fail': ''
    }
}

NEXT_STATUS_CHANGE = {
    'lot': LOTS_WORKFLOW,
    'asset': LOKI_ASSETS_WORKFLOW
}

KEYS_FOR_LOKI_PATCH = {
    'title': 'title',
    'title_ru': 'title_ru',
    'title_en': 'title_en',
    'description': 'description',
    'description_ru': 'description_ru',
    'description_en': 'description_en',
    'assetHolder': 'lotHolder',
    'items': 'items',
    'assetCustodian': 'lotCustodian',
}


ALLOWED_ASSET_TYPES = {
    'bounce'
}