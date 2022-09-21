from scheduler.handers.rewrite_hander import Rewriter


def sql_enc(sql, db):
    # sql = Rewriter(db).rewrite_query(sql)
    return sql