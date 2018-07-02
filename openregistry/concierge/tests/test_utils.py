# -*- coding: utf-8 -*-
from openregistry.concierge.utils import (
    create_certain_condition,
    create_filter_condition
)


def test_create_filter_condition():
    lot_aliases = ['loki', 'anotherLoki']
    handled_statuses = ['pending', 'verification']

    expected_result = '(doc.lotType == "loki" || doc.lotType == "anotherLoki") && (doc.status == "pending" || doc.status == "verification")'
    result = create_filter_condition(lot_aliases, handled_statuses)
    assert result == expected_result

    expected_result = '(doc.lotType == "loki" || doc.lotType == "anotherLoki")'
    result = create_filter_condition(lot_aliases, [])
    assert result == expected_result

    expected_result = '(doc.status == "pending" || doc.status == "verification")'
    result = create_filter_condition([], handled_statuses)
    assert result == expected_result

    expected_result = ''
    result = create_filter_condition([], [])
    assert result == expected_result


def test_create_certain_condition():
    lot_aliases = ['loki', 'anotherLoki']

    expected_result = '(variable == "loki" && variable == "anotherLoki")'

    result = create_certain_condition('variable', lot_aliases, '&&')

    assert result == expected_result
