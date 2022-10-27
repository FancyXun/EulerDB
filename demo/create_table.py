import json
import requests

# 后台数据库信息
db_host = '127.0.0.1'
db = 'points'
user = 'root'
password = 'root'
port = 3306

db_info = {
    'host': db_host, 'db': db,
    'user': user, 'password': password,
    'port': port
}

# 表名
table = "test_points"
sql = 'create table if not exists {}(' \
      'id_card varchar(100), ' \
      'sentences varchar(100), ' \
      'age int, ' \
      'score int, ' \
      'comments varchar(100)) '.format(table)

# 加密信息列
encrypted_columns = {
    "id_card": {
        "fuzzy": True,
        "key": "abcdefgpoints"
    },
    "sentences": {
        "fuzzy": True,
        "key": "abcdefghijklmn"
    },
    "age": {
        "fuzzy": False,
        "key": "abcdefgopqrst",
    }
}
# 加密信息和sql放入词典
db_info['encrypted_columns'] = encrypted_columns
db_info['query'] = sql
# 代理地址
requests.post('http://localhost:8888/query', json.dumps(db_info))