import unittest
from crypto.ope.ope import OPE, ValueRange


class OPETest(unittest.TestCase):
    def test_encrypt(self):
        random_key = OPE.generate_key()
        cipher = OPE(random_key)
        print(cipher.encrypt(1000000000000000000000), cipher.encrypt(200000000000000000000010), cipher.encrypt(3098000000000000000000008000))
        self.assertLess(cipher.encrypt(1000), cipher.encrypt(2000))
        self.assertLess(cipher.encrypt(2000), cipher.encrypt(3000))


if __name__ == '__main__':
    unittest.main()
