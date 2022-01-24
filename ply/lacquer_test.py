import unittest
from ply.lacquer.parsers import parser


class MyTestCase(unittest.TestCase):
    def test_select(self):
        query = 'SELECT * FROM STUDENT'
        ast = parser.parse(query)
        print(ast)
        self.assertEqual(True, True)  # add assertion here

    def test_insert(self):
        query = 'insert into test(sno) values("select1");'
        ast = parser.parse(query)
        print(ast)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
