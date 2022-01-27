import unittest
from src.lex_module.sql_metadata.parser import Parser


class MyTestCase(unittest.TestCase):

    def test_ply_select(self):
        query = 'SELECT * FROM STUDENT;'
        parser = Parser(query)
        print(parser.query_type)
        self.assertEqual(True, True)  # add assertion here

    def test_ply_insert(self):
        query = 'insert into test(no) values("select");'
        parser = Parser(query)
        print(parser.query_type)
        self.assertEqual(True, True)  # add assertion here

    def test_ply_create(self):
        query = 'create table (no varchar(30));'
        parser = Parser(query)
        print(parser.query_type)
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
