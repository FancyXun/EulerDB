import json
import os

from scheduler.crypto import encrypt
from sql_parsing import parse

META_PATH = 'data/meta.json'

ENCRYPT_SQL_TYPE = {
    "INT":
        {
            "OPE": "BIGINT",
            "SYMMETRIC": "VARCHAR(300)"
        },
    "VARCHAR":
        {
            "SYMMETRIC": 20
        }
}

FUZZY_TYPE = 'VARCHAR(2000)'

ARITHMETIC_TYPE = 'VARCHAR(2000)'

CIPHERS = {
    "VARCHAR": encrypt.AESCipher("points"),
    "INT": [encrypt.OPECipher(), encrypt.AESCipher("points")]
}

CIPHERS_META = {
    "OPE": encrypt.OPECipher(),
    "SYMMETRIC": encrypt.AESCipher("points"),
    "FUZZY": encrypt.FuzzyCipher()
}

FUNC_CIPHERS = {
    "max": "OPE",
    "min": "OPE",
    "sum": "ARITHMETIC",
    "avg": "ARITHMETIC"
}


class Delta(object):
    __instance = None
    meta = None
    table_json = None

    def __new__(cls, *args, **kwargs):
        if Delta.__instance is None:
            Delta.__instance = object.__new__(cls, *args, **kwargs)
            cls.meta = cls.load_delta()
        return Delta.__instance

    def update_delta(self, db_name, table_meta):
        if self.meta:
            if db_name not in self.meta.keys():
                self.meta.update({db_name: table_meta})
            else:
                self.meta[db_name].update(table_meta)

        else:
            self.meta = {
                db_name: table_meta
            }
        return self.meta

    def delete_delta(self):
        pass

    def save_delta(self):
        with open(META_PATH, "w") as f:
            json.dump(self.meta, f)

    @staticmethod
    def load_delta():
        if not os.path.exists(META_PATH):
            return {}
        with open(META_PATH, "r") as f:
            meta = json.load(f)
        return meta

    @staticmethod
    def add_ciphers_meta(insert_table_meta=None):
        if insert_table_meta:
            n = insert_table_meta["paillier public key"][0]
            p, q = insert_table_meta["Paillier private key"]
            precision = insert_table_meta["precision"]
            res = {
                "ARITHMETIC": encrypt.PAILLIERCipher(int(n), int(p), int(q), precision)
            }
            return res
        return {}

    @staticmethod
    def create_paillier_sum_procedure(cursor, feature_name, table_name):
        n_square = CIPHERS_META['ARITHMETIC'].pk.nsquare
        drop_procedure = "drop procedure if exists paillierSum"
        cursor.execute(drop_procedure)
        create_procedure = f"CREATE PROCEDURE `paillierSum`(IN nSquare bigint, OUT sum{feature_name} bigint, OUT num{feature_name} int)" \
                         f"BEGIN DECLARE done BOOLEAN DEFAULT 0; DECLARE o bigint; DECLARE enc_data CURSOR FOR SELECT {feature_name} from {table_name};DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1;set sum{feature_name}=1;set num{feature_name}=0;OPEN enc_data;fetch_loop: LOOP FETCH enc_data INTO o;IF done THEN LEAVE fetch_loop; END IF; set sum{feature_name} = (sum{feature_name}*o)%nSquare; set num{feature_name}= num{feature_name}+1; END LOOP; CLOSE enc_data;" \
                         "END"
        cursor.execute(create_procedure)
        # n_square = '2213984809'
        set_query = "SET @nSquare = {};".format(n_square)
        call_query = "CALL `paillierSum`(@nSquare, @sum{}, @num{});".format(feature_name, feature_name)
        cursor.execute(set_query)
        cursor.execute(call_query)

    @staticmethod
    def get_paillier_n_square(origin_query, db):
        origin_table = parse(origin_query)['from']
        table_meta = Delta.load_delta()[db][origin_table]
        return table_meta["paillier public key"][1]

    @classmethod
    def get_paillier_procedure_info(cls):
        sum_feature_name_list = []
        avg_feature_name_list = []
        json = cls.table_json
        for value in json['select']:
            if isinstance(value, dict):
                try:
                    if value.get('sum', '').endswith('ARITHMETIC'):
                        sum_feature_name_list.append(value['sum'])
                    if value.get('avg', '').endswith('ARITHMETIC'):
                        avg_feature_name_list.append(value['avg'])
                except:
                    pass
        need_paillier_procedure = len(sum_feature_name_list) + len(avg_feature_name_list)
        return sum_feature_name_list, avg_feature_name_list, need_paillier_procedure

    @staticmethod
    def modify_sum_query(enc_query, feature_name):
        return enc_query.replace('SUM({})'.format(feature_name), '@sum{}'.format(feature_name))

    @staticmethod
    def modify_avg_query(enc_query, feature_name):
        return enc_query.replace('AVG({})'.format(feature_name), "concat(@sum{}, ',',@num{})".format(feature_name,
                                                                                                     feature_name))
