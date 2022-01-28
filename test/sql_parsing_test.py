import unittest

from unittest import TestCase

from sql_parsing import parse_mysql as parse
from sql_parsing import format


class TestMySql(TestCase):
    def test_issue_22(self):
        sql = 'SELECT "fred"'
        result = parse(sql)
        expected = {"select": {"value": {"literal": "fred"}}}
        self.assertEqual(result, expected)
        print(format(result))

    def test_two_tables(self):
        result = parse("SELECT * from XYZZY, ABC")
        expected = {"from": ["XYZZY", "ABC"], "select": "*"}
        self.assertEqual(result, expected)
        print(format(result))

    def test_dot_table_name(self):
        result = parse("select * from SYS.XYZZY")
        expected = {"from": "SYS.XYZZY", "select": "*"}
        self.assertEqual(result, expected)
        print(format(result))

    def test_select_one_column(self):
        result = parse("Select A from dual")
        expected = {"select": {"value": "A"}, "from": "dual"}
        self.assertEqual(result, expected)
        print(format(result))

    def test_select_quote(self):
        result = parse("Select '''' from dual")
        expected = {"select": {"value": {"literal": "'"}}, "from": "dual"}
        self.assertEqual(result, expected)
        print(format(result))

    def test_select_quoted_name(self):
        # fixme
        result = parse('Select a "@*#&", b as test."g.g".c from dual')
        expected = {
            "select": [
                {"name": "@*#&", "value": "a"},
                {"name": "test.g\\.g.c", "value": "b"},
            ],
            "from": "dual",
        }
        self.assertEqual(result, expected)
        print(format(result))

    def test_select_expression(self):
        #                         1         2         3         4         5         6
        #               0123456789012345678901234567890123456789012345678901234567890123456789
        result = parse("SELECT a + b/2 + 45*c + (2/d) from dual")
        expected = {'select':
                        {'value':
                             {'add': ['a', {'div': ['b', 2]},
                             {'mul': [45, 'c']},
                             {'div': [2, 'd']}]}}, 'from': 'dual'}
        self.assertEqual(result, expected)
        print(format(result))

    def test_select_underscore_name(self):
        #                         1         2         3         4         5         6
        #               0123456789012345678901234567890123456789012345678901234567890123456789
        result = parse("select _id from dual")
        expected = {"select": {"value": "_id"}, "from": "dual"}
        self.assertEqual(result, expected)
        print(format(result))

    def test_select_dots_names(self):
        #                         1         2         3         4         5         6
        #               0123456789012345678901234567890123456789012345678901234567890123456789
        result = parse("select a.b.c._d from dual")
        expected = {"select": {"value": "a.b.c._d"}, "from": "dual"}
        self.assertEqual(result, expected)
        print(format(result))

    def test_select_many_column(self):
        result = parse("Select a, b, c from dual")
        expected = {
            "select": [{"value": "a"}, {"value": "b"}, {"value": "c"}],
            "from": "dual",
        }
        self.assertEqual(result, expected)
        print(format(result))

    def test_bad_select1(self):
        with self.assertRaises(Exception):
            # was 'Expecting select'
            parse("se1ect A, B, C from dual")

    def test_bad_select2(self):
        with self.assertRaises(Exception):
            # was 'Expecting {{expression1 + [{[as] + column_name1}]}'
            parse("Select &&& FROM dual")

    def test_incomplete1(self):
        with self.assertRaises(Exception):
            # was 'Expecting {{expression1 + [{[as] + column_name1}]}}'
            parse("SELECT")

    def test_where_neq(self):
        #                         1         2         3         4         5         6
        #               0123456789012345678901234567890123456789012345678901234567890123456789
        result = parse("SELECT * FROM dual WHERE a<>'test'")
        expected = {
            "from": "dual",
            "select": "*",
            "where": {'neq': ['a', {'literal': 'test'}]},
        }
        self.assertEqual(result, expected)

    def test_where_like(self):
        #                         1         2         3         4         5         6
        #               0123456789012345678901234567890123456789012345678901234567890123456789
        result = parse("SELECT a , b FROM dual where a like '%test%'")
        expected = {'select': [{'value': 'a'}, {'value': 'b'}],
                    'from': 'dual',
                    'where': {'like': ['a', {'literal': '%test%'}]}}
        print(format(result))
        self.assertEqual(result, expected)
        print(format(result))

    def test_order_by(self):
        result = parse("INSERT INTO runoob_tbl (runoob_title, "
                       "runoob_author, submission_date) VALUES ('MySQL', 'zzz', NOW());")
        expected = {'columns': ['runoob_title', 'runoob_author', 'submission_date'],
                    'query': {
                        'select': [{'value': {'literal': 'MySQL'}}, {'value': {'literal': 'zzz'}},
                                   {'value': {'now': {}}}]},
                    'insert': 'runoob_tbl'}
        self.assertEqual(result, expected)
        # print(format(result))

    def test_issue_64_table(self):
        sql = """INSERT INTO tab (name, a, b) VALUES (42, '6736w', 89) """
        result = parse(sql)
        print(result)
        expected = {'columns': ['name', 'a', 'b'],
                    'query':
                        {'select': [{'value': 42}, {'value': {'literal': '6736w'}}, {'value': 89}]},
                    'insert': 'tab'}
        print(format(result))
        self.assertEqual(result, expected)

    def test_issue_64_more_values(self):
        # FROM https://www.freecodecamp.org/news/sql-insert-and-insert-into-statements-with-example-syntax/
        sql = """INSERT INTO Person(Id, Name, DateOfBirth, Gender)
            VALUES (1, 'John Lennon', '1940-10-09', 'M'), (2, 'Paul McCartney', '1942-06-18', 'M'),
            (3, 'George Harrison', '1943-02-25', 'M'), (4, 'Ringo Starr', '1940-07-07', 'M')"""
        result = parse(sql)
        expected = {
            "insert": "Person",
            "values": [
                {
                    "DateOfBirth": "1940-10-09",
                    "Gender": "M",
                    "Id": 1,
                    "Name": "John Lennon",
                },
                {
                    "DateOfBirth": "1942-06-18",
                    "Gender": "M",
                    "Id": 2,
                    "Name": "Paul McCartney",
                },
                {
                    "DateOfBirth": "1943-02-25",
                    "Gender": "M",
                    "Id": 3,
                    "Name": "George Harrison",
                },
                {
                    "DateOfBirth": "1940-07-07",
                    "Gender": "M",
                    "Id": 4,
                    "Name": "Ringo Starr",
                },
            ],
        }
        print(format(result))
        self.assertEqual(result, expected)

    def test_max(self):
        result = parse("select max(a) from b")
        expected = {'select': {'value': {'max': 'a'}},
                    'from': 'b'}
        print(format(result))
        self.assertEqual(result, expected)

    def test_limit(self):
        result = parse("select max(a) from b limit 5")
        print(result)

    def test_create(self):
        result = parse("create table v(id varchar(20), id1 int)")
        expected = {'create table':
                        {'name': 'v',
                         'columns': [{'name': 'id', 'type': {'varchar': 20}}, {'name': 'id1', 'type': {'int': {}}}]}}
        self.assertEqual(result, expected)

    def test_insert(self):
        result = parse("")


if __name__ == '__main__':
    unittest.main()
