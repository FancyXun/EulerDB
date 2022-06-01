import os
import json
import multiprocessing
import numpy as np
import pandas as pd
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from scheduler.crypto import encrypt


SPARK_SUPPORT_ENVIRON_NAME = 'SPARK_CONFIG'
CPU_NUM = 20


class PoolSupport:

    @staticmethod
    def pool_run(fun, iterable_obj, cpu_num=20):
        p = multiprocessing.Pool(processes=cpu_num)
        total_res = []
        for i in iterable_obj:
            res = p.apply_async(fun, (i,))
            total_res.append(res)
        p.close()
        p.join()
        return [i.get() for i in total_res]


class SparkSupport:

    @staticmethod
    def get_spark_sc_from_spark_conf(spark_conf, app_name):
        conf = SparkConf().setAppName(spark_conf.get('app_name', app_name))
        conf.set("spark.driver.memory", str(spark_conf.get("spark.driver.memory", "30g")))
        conf.set("spark.driver.maxResultSize", str(spark_conf.get("spark.driver.maxResultSize", "10g")))
        conf.set("spark.rpc.message.maxSize", str(spark_conf.get("spark.rpc.message.maxSize", "512")))
        conf.set("spark.executor.memory", str(spark_conf.get("spark.executor.memory", "2g")))
        conf.set("spark.executor.cores", str(spark_conf.get("spark.executor.cores", "1")))
        conf.set("spark.cores.max", str(spark_conf.get("spark.cores.max", "60")))
        conf.set("spark.executor.instances", str(spark_conf.get("spark.executor.instances", "60")))
        master = spark_conf.get("master", "spark://192.168.51.154:7077")
        return SparkContext(master=master, conf=conf)

    @classmethod
    def cal_fun_support_spark(cls, data, map_fun, num_slice=100, reduce_fun=None, enable_spark=True, app_name='task'):
        if not enable_spark:
            return map_fun(data)
        spark_conf = json.loads(os.environ.get(SPARK_SUPPORT_ENVIRON_NAME, json.dumps({})))
        sc = cls.get_spark_sc_from_spark_conf(spark_conf, app_name)
        num_slice = min(num_slice, len(data))
        try:
            data_rdd = sc.parallelize(data, num_slice)
            res_rdd = data_rdd.mapPartitions(map_fun)
            if reduce_fun:
                res = res_rdd.reduce(reduce_fun)
            else:
                res = res_rdd.collect()
            sc.stop()
            return res
        except Exception as e:
            sc.stop()
            raise e


class OfflineDecryption:

    @classmethod
    def decrypt_file(cls, csv_path, table, db_meta, states, decrypt_type='single', num=10**5):
        df = pd.read_csv(csv_path)
        columns = df.columns()
        res = []
        data_num = df.shape[0]
        decrypt_dict = {'single': cls.single_decrypt,
                        'pool': cls.pool_decrypt,
                        'distributed': cls.distributed_decrypt}
        if data_num < num:
            decrypt_type = 'single' if decrypt_type=='distributed' else decrypt_type
        decrypt = decrypt_dict[decrypt_type]
        for i, state in zip(columns, states):
            data_type = db_meta[table]['columns'][i]['type']
            res.append(decrypt(df[i], cls.get_feature_key(i, table, db_meta, state), data_type))
        return res

    @staticmethod
    def distributed_decrypt(data, key, data_type):
        if os.environ.get(SPARK_SUPPORT_ENVIRON_NAME):
            try:
                def decrypt_piece(x):
                    x = np.array(list(x))
                    return [encrypt.decode(key.decrypt(i), data_type) for i in x]

                res = np.array(SparkSupport.cal_fun_support_spark(data, decrypt_piece, app_name='decrypt_data'))
                return res
            except Exception as e:
                print(e)
                print('decrypt data in local')
                res = np.array([key.decrypt(i) for i in data])
                return res
        return np.array([key.decrypt(i) for i in data])

    @staticmethod
    def pool_decrypt(data, key, data_type):
        def decrypt(i):
            return encrypt.decode(key.decrypt(i), data_type)
        return np.array(PoolSupport.pool_run(decrypt, data, cpu_num=CPU_NUM))

    @staticmethod
    def single_decrypt(data, key, data_type):
        return np.array([encrypt.decode(key.decrypt(i), data_type) for i in data])

    @staticmethod
    def get_feature_key(col_name, table, db_meta, state):
        key = db_meta[table]['columns'][col_name]['key']
        homo_key = db_meta[table]['columns'][col_name].get('homomorphic_key')
        decrypter = {"symmetric": encrypt.AESCipher(key),
                     "order-preserving": encrypt.OPECipher(key),
                     "arithmetic": encrypt.HomomorphicCipher(homo_key)}
        return decrypter[state]