import unittest
from src.crypto.encrypt import OPECipher


class OPETest(unittest.TestCase):
    def test_encrypt(self):
        cipher = OPECipher()
        self.assertLess(cipher.encrypt(10000), cipher.encrypt(20000))
        self.assertLess(cipher.encrypt(2000), cipher.encrypt(3000))


if __name__ == '__main__':
    unittest.main()
