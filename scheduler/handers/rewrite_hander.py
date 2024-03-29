import copy

from sql_parsing import parse_mysql as parse
from sql_parsing import format, parse_alter_key, format_alter_key

from scheduler.handers.table import rewrite_table
from scheduler.handers.clause.sql_clause import Clause
from scheduler.handers.clause import clause
from scheduler.handers.clause import table_key

from scheduler.schema.metadata import Delta


class Rewriter(Clause):
    def __init__(self, db, encrypted_cols=None):
        super().__init__(db, encrypted_cols)
        self.limit = float('-inf')

    def rewrite_query(self, query):
        """

        """
        table = None
        self.origin_query = query
        if self.encrypted_cols is not None:
            return rewrite_table(self.db, self.db_meta, query, self.encrypted_cols)
        alter_key_json = parse_alter_key(query)
        json = parse(query) if not alter_key_json else alter_key_json
        # if 'limit' in json.keys():
        #     self.limit = abs(int(json['limit']))
        source_json = copy.deepcopy(json)
        for key in table_key:
            if key in json.keys():
                table = copy.deepcopy(json[key])
                break
        else:
            raise Exception("no table found in sql {}".format(self.origin_query))

        def __inner__(inner_json, inner_key, _func):
            if isinstance(inner_json, dict):
                if isinstance(inner_json[inner_key], dict):
                    for _k, _v in inner_json[inner_key].items():
                        tmp = __inner__(_v, _k, _func)
                        inner_json[inner_key][_k] = tmp
                    return inner_json
            else:
                return func[inner_key].rewrite(inner_json, table, json=source_json)

        for key in clause.keys():
            if key not in json.keys():
                continue
            # todo
            func_key = 'select' if key == 'select_distinct' else key
            if key == 'from':
                # 可能是嵌套的select
                if isinstance(json[key], list):
                    for i in range(len(json[key])):
                        if 'value' in json[key][i]:
                            json[key][i]['value'] = self.rewrite_clause(json[key][i]['value'])
                        else:
                            json[key][i] = self.__getattribute__(func_key).rewrite(json[key], table, json=source_json)[i]
                            print(json[key][i])
                    continue
                if 'value' in json[key]:
                    json[key]['value'] = self.rewrite_clause(json[key]['value'])
                    continue
            func = self.__getattribute__(func_key)
            if isinstance(func, dict):
                json = __inner__(json, key, func)
            else:
                json[key] = func.rewrite(json[key], table, json=source_json)

        # record json in Delta
        Delta.table_json = json
        if 'alter' in json:
            return format_alter_key(json), table
        return format(json), table

    def rewrite_clause(self, json):
        source_json = copy.deepcopy(json)
        if isinstance(json, str):
            return self.db_meta[json]['anonymous']
        for key in table_key:
            if key in json.keys():
                table = json[key]
                break
        else:
            raise Exception("no table found in sql {}".format(self.origin_query))

        def __inner__(inner_json, inner_key, _func):
            if isinstance(inner_json, dict):
                if isinstance(inner_json[inner_key], dict):
                    for _k, _v in inner_json[inner_key].items():
                        tmp = __inner__(_v, _k, _func)
                        inner_json[inner_key][_k] = tmp
                    return inner_json
            else:
                return func[inner_key].rewrite(inner_json, table, json=source_json)

        for key in clause.keys():
            if key not in json.keys():
                continue
            # todo
            func_key = 'select' if key == 'select_distinct' else key
            if key == 'from':
                # 可能是嵌套的select
                if 'value' in json[key]:
                    json[key]['value'] = self.rewrite_clause(json[key]['value'])
                    continue
            func = self.__getattribute__(func_key)
            if isinstance(func, dict):
                json = __inner__(json, key, func)
            else:
                json[key] = func.rewrite(json[key], table, json=source_json)
        return json


if __name__ == '__main__':
    str_db = '127.0.0.1:3306/points'
    enc_cols = {}
    sql_list = [
        # "CREATE TABLE `user_info`  (\
        #     `id` bigint(11) NOT NULL AUTO_INCREMENT COMMENT '主键',\
        #     `user_name` varchar(255) "
        #     # "CHARACTER SET utf8 "
        #     "COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '姓名',\
        #     `sex` int(1) NULL DEFAULT NULL COMMENT '性别',\
        #     `create_date` "
        #     "timestamp "
        #     "NULL DEFAULT CURRENT_TIMESTAMP "
        #     # "ON UPDATE CURRENT_TIMESTAMP(0) "
        #     "COMMENT '创建日期',\
        #     PRIMARY KEY (`id`) "
        #     # "USING BTREE\
        # ") ENGINE = InnoDB "
        # "AUTO_INCREMENT = 5 "
        # # "CHARACTER SET = utf8mb4 "
        # "COLLATE = utf8mb4_0900_ai_ci "
        # "COMMENT = '用户信息表' "
        # # "ROW_FORMAT = Dynamic"
        # ";",
        'select * from tt'
    ]
    enc_cols = [None]*len(sql_list)
    enc_cols[0] = []
    rewriter = Rewriter(str_db, enc_cols)
    for i in range(len(sql_list)):
        rewriter = Rewriter(str_db, enc_cols[i])
        res = rewriter.rewrite_query(sql_list[i])
        print(res[0])

