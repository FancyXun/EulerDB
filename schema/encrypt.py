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
import random

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.fernet import Fernet
from gmssl.sm4 import CryptSM4, SM4_ENCRYPT, SM4_DECRYPT

BLOCK_SIZE = 16
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpad = lambda s: s[:-ord(s[len(s) - 1:])]


class AESCipher:

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
    def __init__(self):
        self.cipher = Fernet(b'AvA1crC_CVMQe4LskY6w7J7R3BmemH02A7utNimOHCQ=')

    def encrypt(self, raw):
        return self.cipher.encrypt(raw.encode('utf-8')).decode("utf-8")

    def decrypt(self, enc):
        return self.cipher.decrypt(enc.encode('utf-8')).decode("utf-8")


fernetCipher = FernetCipher()



