from client.__main__ import main
from tests.test_database_request_handler import get_value, insert_value


def test_print_inserted_value_when_get_key(capsys):
    insert_value('panama', 'owl', 'circle')
    capsys.readouterr()
    args = '-H localhost -P 2309 -D panama -G -K owl'.split()
    main(args)
    assert capsys.readouterr().out == 'circle\n'


# def test_value_is_changed_on_inserted_value_after_adding(capsys):
#     value = get_value('panama', 'dora')
#     assert value == ''
#     capsys.readouterr()
#     args = '-H localhost -P 2309 -D panama -A -K dora -V sandwich'.split()
#     main(args)
#     new_value = get_value('panama', 'dora')
#     assert new_value == 'sandwich'


def test_value_is_deleted_after_removing(capsys):
    insert_value('panama', 'queen', 'fox')
    value = get_value('panama', 'queen')
    assert value == 'fox'
    capsys.readouterr()
    args = '-H localhost -P 2309 -D panama -R -K queen'.split()
    main(args)
    new_value = get_value('panama', 'queen')
    assert new_value == ''
