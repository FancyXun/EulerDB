# Lint as: python3
# Copyright 2018, The TensorFlow Federated Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from base64 import b64decode
from base64 import b64encode
from hashlib import sha256
from functools import reduce
import random

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet
from gmssl.sm4 import CryptSM4, SM4_ENCRYPT, SM4_DECRYPT
from scheduler.crypto.ope.ope import OPE
from phe import paillier

BLOCK_SIZE = 16
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpad = lambda s: s[:-ord(s[len(s) - 1:])]


def stretch(val):
    # piecewise linear function [10**15, 10**42, 10**56, 10**65], [10**15, 10**19, 10**22, 10**24]
    if val < 0:
        return -stretch(-val)
    if 10**15 >= val >= 0:
        return val
    if 10**42 >= val > 10**15:
        return (val-10**15)/(10**42-10**15)*(10**19-10**15)+10**15
    if 10**56 >= val > 10**42:
        return (val-10**42)/(10**56-10**42)*(10**22-10**19)+10**19
    if 10**65 >= val > 10**56:
        return (val-10**56)/(10**65-10**56)*(10**24-10**22)+10**22


def inverse_stretch(val):
    # piecewise linear function [10**15, 10**19, 10**22, 10**24], [10**15, 10**42, 10**56, 10**65]
    if val < 0:
        return -stretch(-val)
    if 10**15 >= val >= 0:
        return val
    if 10**19 >= val > 10**15:
        return (val-10**15)/(10**19-10**15)*(10**42-10**15)+10**15
    if 10**22 >= val > 10**19:
        return (val-10**19)/(10**22-10**19)*(10**56-10**42)+10**42
    if 10**24 >= val > 10**22:
        return (val-10**22)/(10**24-10**22)*(10**65-10**56)+10**56


def encode(val, val_type):
    if val_type == 'float':
        return int(val * 10 ** 12)
    if val_type == 'double':
        val = stretch(val)
        return int(val * 10 ** 12)
    return val


def decode(val, val_type):
    if val_type in ['int', 'float', 'double']:
        try:
            val = eval(val)
        except:
            val = val
    if val_type == 'int':
        return val
    if val_type == 'float':
        return val / 10 ** 12
    if val_type == 'double':
        val = inverse_stretch(val)
        return val / 10 ** 12
    return val


class AESCrypto(object):
    AES_CBC_KEY = b'a\x14\x04.\x8a\xa2a\xec,\xf1\x07\xc2l\x19|`g\xae\xba\tl\xc4\xa7\xac$\x11\xef\x0f\xeaN\x01\xcf'
    AES_CBC_IV = b'\xd3|\xf6(\xc3\x15\x08\xeaq\xc4}\xbf\xc3\x95\\{'

    @classmethod
    def encrypt(cls, data, mode='cbc'):
        func_name = '{}_encrypt'.format(mode)
        func = getattr(cls, func_name)
        return func(data)

    @classmethod
    def decrypt(cls, data, mode='cbc'):
        func_name = '{}_decrypt'.format(mode)
        func = getattr(cls, func_name)
        return func(data)

    @staticmethod
    def pkcs7_padding(data):
        if not isinstance(data, bytes):
            data = data.encode()

        padder = padding.PKCS7(algorithms.AES.block_size).padder()

        padded_data = padder.update(data) + padder.finalize()

        return padded_data

    @classmethod
    def cbc_encrypt(cls, data):
        if not isinstance(data, bytes):
            data = data.encode()

        cipher = Cipher(algorithms.AES(cls.AES_CBC_KEY),
                        modes.CBC(cls.AES_CBC_IV),
                        backend=default_backend())
        encryptor = cipher.encryptor()

        padded_data = encryptor.update(cls.pkcs7_padding(data))

        return padded_data

    @classmethod
    def cbc_decrypt(cls, data):
        if not isinstance(data, bytes):
            data = data.encode()

        cipher = Cipher(algorithms.AES(cls.AES_CBC_KEY),
                        modes.CBC(cls.AES_CBC_IV),
                        backend=default_backend())
        decryptor = cipher.decryptor()

        uppaded_data = cls.pkcs7_unpadding(decryptor.update(data))

        uppaded_data = uppaded_data.decode()
        return uppaded_data

    @staticmethod
    def pkcs7_unpadding(padded_data):
        unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
        data = unpadder.update(padded_data)

        try:
            uppadded_data = data + unpadder.finalize()
        except ValueError:
            raise Exception('Invalid encrypted information!')
        else:
            return uppadded_data


class AESCipher:
    input = 'STRING'
    output = 'STRING'

    def __init__(self, key):
        self.key = bytes.fromhex(sha256(key.encode('utf8')).hexdigest())

    @staticmethod
    def pad(plain_text):
        if len(plain_text) % BLOCK_SIZE != 0:
            add = BLOCK_SIZE - (len(plain_text) % BLOCK_SIZE)
        else:
            add = 0

        return plain_text + ("\0".encode() * add)

    @staticmethod
    def unpad(plain_text):
        return plain_text.decode('utf8').rstrip("\0")

    def encrypt(self, raw):
        raw = self.pad(raw.encode('utf8'))
        aes = Cipher(algorithms.AES(self.key), modes.ECB())
        cipher = aes.encryptor()
        c = cipher.update(raw) + cipher.finalize()
        return b64encode(c).decode('utf8')

    def decrypt(self, enc):
        enc = b64decode(enc)
        aes = Cipher(algorithms.AES(self.key), modes.ECB())
        cipher = aes.decryptor()
        m = self.unpad(cipher.update(enc) + cipher.finalize())
        return m


class SM4CipherBytes:

    def __init__(self, key):
        self.key = bytes.fromhex(sha256(key.encode('utf8')).hexdigest())
        self.crypt_sm4 = CryptSM4()

    def encrypt(self, raw):
        if isinstance(raw, bytes):
            pass
        else:
            raw = str(raw)
            raw = raw.encode('utf-8')
        self.crypt_sm4.set_key(self.key, SM4_ENCRYPT)
        cipher_text = self.crypt_sm4.crypt_ecb(raw)
        return cipher_text.hex()

    def decrypt(self, enc):
        if isinstance(enc, bytes):
            pass
        else:
            enc = bytes.fromhex(enc)
        self.crypt_sm4.set_key(self.key, SM4_DECRYPT)
        text = self.crypt_sm4.crypt_ecb(enc)
        return text.decode()


class AESCipherBytes:

    def __init__(self, key):
        self.key = bytes.fromhex(sha256(key).hexdigest())

    def encrypt(self, raw):
        if isinstance(raw, bytes):
            raw = b64encode(raw).decode('utf8')
        raw = pad(raw).encode('utf8')
        aes = Cipher(algorithms.AES(self.key), modes.ECB())
        cipher = aes.encryptor()
        c = cipher.update(raw) + cipher.finalize()
        return c

    def decrypt(self, enc):
        aes = Cipher(algorithms.AES(self.key), modes.ECB())
        cipher = aes.decryptor()
        m = unpad(cipher.update(enc) + cipher.finalize())
        return b64decode(m)


def key_generator(key_size=32, rand_seed=None):
    str_choice = "1234567890abcdef"
    random.seed(rand_seed)
    sa = []
    for i in range(key_size):
        sa.append(random.choice(str_choice))
    key = (''.join(sa)).encode()
    return key


class FernetCipher:
    input = 'STRING'
    output = 'STRING'

    def __init__(self):
        self.cipher = Fernet(b'AvA1crC_CVMQe4LskY6w7J7R3BmemH02A7utNimOHCQ=')

    def encrypt(self, raw):
        return self.cipher.encrypt(raw.encode('utf-8')).decode("utf-8")

    def decrypt(self, enc):
        return self.cipher.decrypt(enc.encode('utf-8')).decode("utf-8")


class OPECipher:
    input = 'INT'
    output = 'BIGINT'

    def __init__(self, key):
        self.key = bytes.fromhex(sha256(key.encode('utf8')).hexdigest())
        self.cipher = OPE(self.key)

    def encrypt(self, raw):
        return self.cipher.encrypt(int(raw))

    def decrypt(self, enc):
        return self.cipher.decrypt(int(enc))


class FuzzyCipher:
    input = 'STRING'
    output = 'STRING'

    def __init__(self, key):
        self.cipher = AESCipher(key)

    def encrypt(self, raw):
        result = ""
        if raw.isalpha() or raw.isalnum() or raw.isdigit():
            if len(raw) < 3:
                result = self.cipher.encrypt(str(raw))
            else:
                for i in range(len(raw)-2):
                    result = result + self.cipher.encrypt(str(raw)[i: i+3])
        else:
            if len(raw) < 2:
                result = self.cipher.encrypt(str(raw))
            else:
                for i in range(len(raw)-1):
                    result = result + self.cipher.encrypt(str(raw)[i: i+2])

        return result


class HomomorphicCipher:
    input = 'INT'
    output = 'STRING'

    def __init__(self, homomorphic_key):
        if homomorphic_key:
            self.p, self.q = homomorphic_key
            self.n = self.p * self.q
            self.pk = paillier.PaillierPublicKey(int(self.n))
            self.sk = paillier.PaillierPrivateKey(self.pk, int(self.p), int(self.q))
            self.precision = None

    def encrypt(self, raw):
        return str(self.pk.encrypt(int(raw), self.precision).ciphertext(False))

    def decrypt(self, enc):
        enc_list = str(enc).split(',')
        enc_value_list, agg_type = enc_list[:-1], enc_list[-1]
        n_square = self.pk.nsquare
        if agg_type == 'SUM':
            sum_enc_res = reduce(lambda x, y: (x * y) % n_square, map(lambda x: int(x), enc_value_list), 1)
            enc_number = paillier.EncryptedNumber(self.pk, int(sum_enc_res))
            return self.sk.decrypt(enc_number)
        elif agg_type == 'AVG':
            sum_enc_res = reduce(lambda x, y: (x * y) % n_square, map(lambda x: int(x), enc_value_list), 1)
            enc_number = paillier.EncryptedNumber(self.pk, int(sum_enc_res))
            return self.sk.decrypt(enc_number) / len(enc_value_list)
        num = 1
        if ',' in str(enc):
            enc, num = enc.split(',')
        enc_number = paillier.EncryptedNumber(self.pk, int(enc))
        if num == 1:
            return int(self.sk.decrypt(enc_number))
        return round(self.sk.decrypt(enc_number) / int(num), 4)


if __name__ == '__main__':
    text = 12345678
    key = "abcdefghijklmnopqrstuvwxyz@#$%^&*()points"
    homo_key = [787659527, 1023624989]
    sm4 = SM4CipherBytes(key)
    ope = OPECipher(key)
    aes = AESCipher(key)
    fuzzy = FuzzyCipher(key)
    homo = HomomorphicCipher(homo_key)
    print(homo.decrypt(homo.encrypt(text)))
    print('sm4', sm4.encrypt(text))

    print('sm4', sm4.decrypt(sm4.encrypt(text)))

    ope_text = ope.encrypt(text)
    aes_text = aes.encrypt(str(text))
    print(ope_text)
    print(aes_text)
    print(ope.decrypt(ope_text))
    print(aes.decrypt(aes_text))
    print("-"*200)
    text1 = '345sfdfsytd6twet6wte6wt6ewtetw'
    text2 = 23143241241324
    text3 = '中文'
    aes_text = aes.encrypt(str(text1))
    print(aes_text)
    print(aes.decrypt(aes_text))
    aes_text = aes.encrypt(str(text2))
    print(aes_text)
    print(aes.decrypt(aes_text))
    aes_text = aes.encrypt(str(text3))
    print(aes_text)
    print(aes.decrypt(aes_text))
