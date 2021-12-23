from unittest.mock import patch

import requests.exceptions

from client.__main__ import main
from tests.test_database_request_handler import get_value, insert_value


def test_print_inserted_value_when_get_key(capsys):
    insert_value('panama', 'owl', 'circle')
    capsys.readouterr()
    args = '-H localhost -P 2309 -D panama -G -K owl'.split()
    main(args)
    assert capsys.readouterr().out == 'circle\n'


def test_value_is_changed_on_inserted_value_after_adding():
    insert_value('panama', 'dora', 'snake')
    value = get_value('panama', 'dora')
    assert value == 'snake'
    args = '-H localhost -P 2309 -D panama -A -K dora -V sandwich'.split()
    main(args)
    new_value = get_value('panama', 'dora')
    assert new_value == 'sandwich'


def test_value_is_deleted_after_removing():
    insert_value('panama', 'zebra', 'fox')
    value = get_value('panama', 'zebra')
    assert value == 'fox'
    args = '-H localhost -P 2309 -D panama -R -K zebra'.split()
    main(args)
    new_value = get_value('panama', 'zebra')
    assert new_value == ''


def raise_connection_error():
    raise requests.ConnectionError


@patch('client.__main__.insert_value', lambda *x: raise_connection_error())
def test_print_connection_error_if_can_not_connect_to_server(capsys):
    args = '-H localhost -P 2309 -D panama -A -K lalaland -V flow'.split()
    main(args)
    assert capsys.readouterr().out == \
           'Error: can not connect to localhost:2309.\n'
