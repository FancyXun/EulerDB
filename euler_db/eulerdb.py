from scheduler.backend import execution_context


def enc_sql(query_info, query):
    result = execution_context.encrypt_sql(query_info, query)
    return result


if __name__ == '__main__':
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
    sql = 'select * from {} where id_card = "310310314" and sentences = "杭州光之树大华公司中国集团上海腾讯有限" from test'
    enc_sql(db_info, sql)

