import re
import codecs
import hashlib
from collections import defaultdict
from gmpy2 import invert
from scheduler.crypto.encrypt import AESCipher


mod_p = 74253941


class OXT:

    @staticmethod
    def setup_edb(inverted_index_dict, keys):
        k_s, k_x, k_i, k_z, k_t = keys
        t_dict = dict()
        x_set = set()
        for word in inverted_index_dict.keys():
            t = []
            k_e = AESCipher(str(OXTRandom.prf(k_s, word)))
            # c = 0
            for ind in inverted_index_dict[word]:
                # x_ind = OXTRandom.prf_ddh(k_i, ind)
                # z = OXTRandom.prf_p(k_z, word | c)
                # z = OXTRandom.hash(word)*OXTRandom.prf_ddh(k_z, r)
                x_ind = OXTRandom.prf(k_i, ind) % mod_p
                z = OXTRandom.prf(k_z, word) % mod_p
                y = x_ind * OXTRandom.inverse(z) % mod_p
                # g = 2
                # c += 1
                x_set.add(OXTRandom.prf(k_x, word) * x_ind % mod_p)

                e = k_e.encrypt(str(ind))
                t.append((e, y))
            # stag_w = OXTRandom.lsh(word) * OXTRandom.prf_ddh(k_t, 1)
            stag_w = OXTRandom.prf(k_t, word)
            t_dict[stag_w] = t

        return t_dict, x_set, keys

    @staticmethod
    def build_index(least_frequent_word, rest_words, k_x, k_t, k_z):
        # g = 2
        # stag = OXTRandom.lsh(least_frequent_word) * OXTRandom.prf_ddh(k_t, 1)
        stag = OXTRandom.prf(k_t, least_frequent_word)
        # z = OXTRandom.lsh(least_frequent_word) * OXTRandom.prf_ddh(k_z, 1)
        z = OXTRandom.prf(k_z, least_frequent_word) % mod_p
        # x_tokens = [g ** (z * OXTRandom.prf_p(k_x, i, mod_p) % mod_p) for i in rest_words]
        x_tokens = [z * OXTRandom.prf(k_x, i) % mod_p for i in rest_words]
        return stag, x_tokens

    @staticmethod
    def search_encryption(stag, x_tokens, t_set, x_set):
        t = t_set[stag]
        res_ind = []
        for enc_ind, y in t:
            for j in x_tokens:
                if j * y % mod_p not in x_set:
                    break
            else:
                res_ind.append(enc_ind)
        return res_ind


class OXTRandom:

    @staticmethod
    def gen_key(bit_size):
        return

    @staticmethod
    def prf(key, word):
        return int(hashlib.md5(bytes(codecs.encode(str(key)))+bytes(codecs.encode(str(word)))).hexdigest(), 16)

    @staticmethod
    def hash(x, p=74253941):
        return int(hashlib.md5(bytes(x)).hexdigest(), 16) % p

    @classmethod
    def prf_ddh(cls, k, x, p=74253941):
        return cls.hash(x) ** k % p

    @staticmethod
    def inverse(z, p=74253941):
        return int(invert(z, p))


class RawOXT:

    @staticmethod
    def encode_words(words):
        if isinstance(words, list):
            return [int(codecs.encode(w, 'utf-8').hex(), 16) for w in words]
        return int(codecs.encode(words, 'utf-8').hex(), 16)

    @classmethod
    def build_forward_index(cls, text_list, ind_list=None):
        ind_list = [i+1 for i in range(len(text_list))] if not ind_list else ind_list
        forward_index = dict()
        for ind, text in zip(ind_list, text_list):
            forward_index[ind] = cls.split_raw_text_into_keywords(text)
        return forward_index

    @staticmethod
    def build_inverted_index(forward_index):
        inverted_index = defaultdict(set)
        for ind, keywords in forward_index.items():
            for keyword in keywords:
                inverted_index[keyword].add(ind)
        return inverted_index

    @classmethod
    def split_raw_text_into_keywords(cls, raw_text):
        x = re.split(r'[\ \,\.]{1,}', raw_text+' ')[:-1]
        res = set()
        for i in x:
            res.add(cls.encode_words(i))
        return res

    @staticmethod
    def search_keyword(word_first, rest_word_set, forward_index, inverted_index):
        ind_list = inverted_index[word_first]
        search_ind_list = []
        for ind in ind_list:
            # can use bloom filter to decide whether it is a subset
            if rest_word_set.issubset(forward_index[ind]):
                search_ind_list.append(ind)
        return search_ind_list


if __name__ == '__main__':
    files = ['some people like coke', 'some guys like sprite', 'some dudes like fanta',
             'others hate coke', 'others hate sprite']

    fwd_index = RawOXT.build_forward_index(files)
    inv_index = RawOXT.build_inverted_index(fwd_index)
    search_word = ['coke', 'some', 'like']
    encode_word = RawOXT.encode_words(search_word)
    ind_res = RawOXT.search_keyword(encode_word[0], set(encode_word[1:]), fwd_index, inv_index)
    origin_res = [files[i-1] for i in ind_res]
    print(origin_res)

    use_keys = list('abcde')
    t_dict, set_x, use_keys = OXT.setup_edb(inv_index, use_keys)
    key_s, key_x, key_i, key_z, key_t = use_keys
    stag, enc_search_tokens = OXT.build_index(encode_word[0], set(encode_word[1:]), key_x, key_t, key_z)
    res_enc_ind = OXT.search_encryption(stag, enc_search_tokens, t_dict, set_x)
    w_ke = AESCipher(str(OXTRandom.prf(key_s, encode_word[0])))
    ind_res = [int(w_ke.decrypt(i))-1 for i in res_enc_ind]
    print([files[i] for i in ind_res])


















