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

    def encrypt(self, raw):
        raw = pad(raw).encode('utf8')
        aes = Cipher(algorithms.AES(self.key), modes.ECB())
        cipher = aes.encryptor()
        c = cipher.update(raw) + cipher.finalize()
        return b64encode(c).decode('utf8')

    def decrypt(self, enc):
        enc = b64decode(enc)
        aes = Cipher(algorithms.AES(self.key), modes.ECB())
        cipher = aes.decryptor()
        m = unpad(cipher.update(enc) + cipher.finalize())
        return m.decode('utf8')


class SM4CipherBytes:

    def __init__(self, key):
        self.key = bytes.fromhex(key.decode())
        self.crypt_sm4 = CryptSM4()

    def encrypt(self, raw):
        if isinstance(raw, bytes):
            pass
        else:
            raw = raw.encode('utf-8')
        self.crypt_sm4.set_key(self.key, SM4_ENCRYPT)
        cipher_text = self.crypt_sm4.crypt_ecb(raw)
        return cipher_text

    def decrypt(self, enc):
        if isinstance(enc, bytes):
            pass
        else:
            enc = enc.encode('utf-8')
        self.crypt_sm4.set_key(self.key, SM4_DECRYPT)
        text = self.crypt_sm4.crypt_ecb(enc)
        return text


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

    def __init__(self):
        self.cipher = OPE(b'ZvtU2iyA0E5byRp6YMxbsoEnk1vKPxtm6IcLt4ZxuK0=')

    def encrypt(self, raw):
        return self.cipher.encrypt(int(raw))

    def decrypt(self, enc):
        return self.cipher.decrypt(int(enc))


class FuzzyCipher:
    input = 'STRING'
    output = 'STRING'

    def __init__(self):
        pass

    @staticmethod
    def encrypt(raw):
        result = ""
        if raw.isalpha() or raw.isalnum() or raw.isdigit():
            assert len(raw) >= 3
            for i in range(len(raw)-2):
                result = result + AESCipher("points").encrypt(str(raw)[i: i+3])
        else:
            assert len(raw) >= 2
            for i in range(len(raw)-1):
                result = result + AESCipher("points").encrypt(str(raw)[i: i+2])

        return result


class PAILLIERCipher:
    input = 'INT'
    output = 'STRING'

    def __init__(self, n=11214985453562592643, p=2670330847, q=4199848669, precision=None):
        self.pk = paillier.PaillierPublicKey(n)
        self.sk = paillier.PaillierPrivateKey(self.pk, p, q)
        self.precision = precision

    def encrypt(self, raw):
        return str(self.pk.encrypt(int(raw), self.precision).ciphertext(False))

    def decrypt(self, enc):
        enc_list = str(enc).split(',')
        enc_value_list, agg_type = enc_list[:-1], enc_list[-1]
        print(agg_type)
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
        return self.sk.decrypt(enc_number) / int(num)
