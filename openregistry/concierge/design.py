# -*- coding: utf-8 -*-
from couchdb.design import ViewDefinition


FIELDS = [
    'status',
    'id',
    'assets',
]


def add_index_options(doc):
    doc['options'] = {'local_seq': True}


def sync_design(db):
    views = [j for i, j in globals().items() if "_view" in i]
    ViewDefinition.sync_many(db, views, callback=add_index_options)


concierge_view = ViewDefinition('lots', 'check_lot', '''function(doc) {
    var statuses = ['waiting', 'pending.dissolution', 'recomposed']
    if(doc.doc_type == 'Lot' && statuses.indexOf(doc.status) != 1) {
        var fields=%s, data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc._local_seq, data);
    }
}''' % FIELDS)
