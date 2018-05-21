BASIC_ASSETS_WORKFLOW = {
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
    'pending.deleted': {
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

LOTS_WORKFLOW = {
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
    'pending.deleted': {
        'pre': '',
        'finish': 'deleted',
        'fail': ''
    },
    'pending.sold': {
        'pre': '',
        'finish': 'sold',
        'fail': ''
    }
}


ASSET_TO_LOT_TYPE = {
    'basic', 'compound', 'claimrights'
}
NEXT_STATUS_CHANGE = {
    'lot': LOTS_WORKFLOW,
    'asset': BASIC_ASSETS_WORKFLOW
}