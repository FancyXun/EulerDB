import unittest
from sql_yacc import sql_parse


class MyTestCase(unittest.TestCase):
    def test_ply_select(self):
        query = 'select * from test;'
        ast = sql_parse(query)
        print(ast)
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
