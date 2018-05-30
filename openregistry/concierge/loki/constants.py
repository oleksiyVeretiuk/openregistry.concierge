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

KEYS_FOR_AUCTION_CREATE = {
    'title': 'title',
    'merchandisingObject': 'id',
    'description': 'description',
    'tenderAttempts': 'auctions/{}/tenderAttempts',
    'procuringEntity': 'lotCustodian',
    'items': 'items',
    'auctionPeriod/startDate': 'auctions/{}/auctionPeriod/startDate',
    'value': 'auctions/{}/value',
    'minimalStep': 'auctions/{}/minimalStep',
    'guarantee': 'auctions/{}/guarantee',
    'registrationFee': 'auctions/{}/registrationFee',
    'procurementMethodType': 'auctions/{}/procurementMethodType',
    'documents': 'auctions/{}/documents',
    'bankAccount': 'auctions/{}/bankAccount',
    'auctionParameters': 'auctions/{}/auctionParameters',
}