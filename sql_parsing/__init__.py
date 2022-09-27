# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import re
import json
from threading import Lock

from sql_parsing.sql_parser import scrub
from sql_parsing.utils import ansi_string, simple_op, normal_op

parse_locker = Lock()  # ENSURE ONLY ONE PARSING AT A TIME
common_parser = None
mysql_parser = None
sqlserver_parser = None

SQL_NULL = {"null": {}}


def parse(sql, null=SQL_NULL, calls=simple_op):
    """
    :param sql: String of SQL
    :param null: What value to use as NULL (default is the null function `{"null":{}}`)
    :return: parse tree
    """
    global common_parser

    with parse_locker:
        if not common_parser:
            common_parser = sql_parser.common_parser()
        result = _parse(common_parser, sql, null, calls)
        return result


def parse_mysql(sql, null=SQL_NULL, calls=simple_op):
    """
    PARSE MySQL ASSUME DOUBLE QUOTED STRINGS ARE LITERALS
    :param sql: String of SQL
    :param null: What value to use as NULL (default is the null function `{"null":{}}`)
    :return: parse tree
    """
    global mysql_parser

    with parse_locker:
        if not mysql_parser:
            mysql_parser = sql_parser.mysql_parser()
        return _parse(mysql_parser, sql, null, calls)


def parse_sqlserver(sql, null=SQL_NULL, calls=simple_op):
    """
    PARSE MySQL ASSUME DOUBLE QUOTED STRINGS ARE LITERALS
    :param sql: String of SQL
    :param null: What value to use as NULL (default is the null function `{"null":{}}`)
    :return: parse tree
    """
    global sqlserver_parser

    with parse_locker:
        if not sqlserver_parser:
            sqlserver_parser = sql_parser.sqlserver_parser()
        return _parse(sqlserver_parser, sql, null, calls)


parse_bigquery = parse_mysql


def _parse(parser, sql, null, calls):
    utils.null_locations = []
    utils.scrub_op = calls
    sql = sql.rstrip().rstrip(";")
    parse_result = parser.parse_string(sql, parse_all=True)
    output = scrub(parse_result)
    for o, n in utils.null_locations:
        o[n] = null
    return output


def format(json, **kwargs):
    from sql_parsing.formatting import Formatter

    return Formatter(**kwargs).dispatch(json)


def parse_alter_key(sql):
    add_primary_key_pattern = re.compile(r'[ \t\n\r]*'
                                         r'[aA][lL][tT][eE][rR][ \t\n\r]*'
                                         r'[tT][aA][bB][lL][eE][ \t\n\r]*'
                                         r'(`|\b)(\w*)\1[ \t\n\r]*'
                                         r'[aA][dD][dD][ \t\n\r]*'
                                         r'[pP][rR][iI][mM][aA][rR][yY][ \t\n\r]*'
                                         r'[kK][eE][yY][ \t\n\r]*'
                                         r'\([ \t\n\r]*(\w*)[ \t\n\r]*\)[ \t\n\r]*;?$')
    add_primary_key = add_primary_key_pattern.match(sql)
    if add_primary_key:
        return {'alter': add_primary_key.group(2),
                'add primary key': [{'value': i} for i in add_primary_key.group(3).split(',')]}
    drop_primary_key_pattern = re.compile(r'[ \t\n\r]*'
                                          r'[aA][lL][tT][eE][rR][ \t\n\r]*'
                                          r'[tT][aA][bB][lL][eE][ \t\n\r]*'
                                          r'(`|\b)(\w*)\1[ \t\n\r]*'
                                          r'[dD][rR][oO][pP][ \t\n\r]*'
                                          r'[pP][rR][iI][mM][aA][rR][yY][ \t\n\r]*'
                                          r'[kK][eE][yY][ \t\n\r]*;?$')
    drop_primary_key = drop_primary_key_pattern.match(sql)
    if drop_primary_key:
        return {'alter': drop_primary_key.group(2), 'drop primary key': True}
    add_foreign_key_pattern = re.compile(r'[ \t\n\r]*'
                                         r'[aA][lL][tT][eE][rR][ \t\n\r]*'
                                         r'[tT][aA][bB][lL][eE][ \t\n\r]*'
                                         r'(`|\b)(\w*)\1[ \t\n\r]*'
                                         r'[aA][dD][dD][ \t\n\r]*'
                                         r'[cC][oO][nN][sS][tT][rR][aA][iI][nN][tT][ \t\n\r]*'
                                         r'(\w*)[ \t\n\r]*'
                                         r'[fF][oO][rR][eE][iI][gG][nN][ \t\n\r]*'
                                         r'[kK][eE][yY][ \t\n\r]*'
                                         r'\([ \t\n\r]*(\w*)[ \t\n\r]*\)[ \t\n\r]*'
                                         r'[rR][eE][fF][eE][rR][eE][nN][cC][eE][sS][ \t\n\r]*'
                                         r'(\w*)[ \t\n\r]*'
                                         r'\([ \t\n\r]*(\w*)[ \t\n\r]*\)[ \t\n\r]*;?$')
    add_foreign_key = add_foreign_key_pattern.match(sql)
    if add_foreign_key:
        return {'alter': add_foreign_key.group(2), 'add constraint': add_foreign_key.group(3),
                'add foreign key': [{'value': i} for i in add_foreign_key.group(4).split(',')],
                'reference_table': add_foreign_key.group(5),
                'reference_feature': [{'value': i} for i in add_foreign_key.group(6).split(',')]}
    drop_foreign_key_pattern = re.compile(r'[ \t\n\r]*'
                                          r'[aA][lL][tT][eE][rR][ \t\n\r]*'
                                          r'[tT][aA][bB][lL][eE][ \t\n\r]*'
                                          r'(`|\b)(\w*)\1[ \t\n\r]*'
                                          r'[dD][rR][oO][pP][ \t\n\r]*'
                                          r'[fF][oO][rR][eE][iI][gG][nN][ \t\n\r]*'
                                          r'[kK][eE][yY][ \t\n\r]*'
                                          r'(\w*)[ \t\n\r]*;?$')
    drop_foreign_key = drop_foreign_key_pattern.match(sql)
    if drop_foreign_key:
        return {'alter': drop_foreign_key.group(2), 'drop foreign key': drop_foreign_key.group(3)}
    add_index_pattern = re.compile(r'[ \t\n\r]*'
                                   r'[aA][lL][tT][eE][rR][ \t\n\r]*'
                                   r'[tT][aA][bB][lL][eE][ \t\n\r]*'
                                   r'(`|\b)(\w*)\1[ \t\n\r]*'
                                   r'[aA][dD][dD][ \t\n\r]*'
                                   r'[iI][nN][dD][eE][xX][ \t\n\r]*'
                                   r'(\w*)[ \t\n\r]*'
                                   r'\([ \t\n\r]*(\w*)[ \t\n\r]*\)[ \t\n\r]*;?$')
    add_index = add_index_pattern.match(sql)
    if add_index:
        return {'alter': add_index.group(2),
                'add index': add_index.group(3),
                'add index feature': [{'value': i} for i in add_index.group(4).split(',')]}

    add_fulltext_pattern = re.compile(r'[ \t\n\r]*'
                                      r'[aA][lL][tT][eE][rR][ \t\n\r]*'
                                      r'[tT][aA][bB][lL][eE][ \t\n\r]*'
                                      r'(`|\b)(\w*)\1[ \t\n\r]*'
                                      r'[aA][dD][dD][ \t\n\r]*'
                                      r'[fF][uU][lL][lL][tT][eE][xX][tT][ \t\n\r]*'
                                      r'(\w*)[ \t\n\r]*'
                                      r'\([ \t\n\r]*(\w*)[ \t\n\r]*\)[ \t\n\r]*;?$')
    add_fulltext = add_fulltext_pattern.match(sql)
    if add_fulltext:
        return {'alter': add_fulltext.group(2),
                'add fulltext': add_fulltext.group(3),
                'add fulltext feature': [{'value': i} for i in add_fulltext.group(4).split(',')]}

    add_unique_pattern = re.compile(r'[ \t\n\r]*'
                                    r'[aA][lL][tT][eE][rR][ \t\n\r]*'
                                    r'[tT][aA][bB][lL][eE][ \t\n\r]*'
                                    r'(`|\b)(\w*)\1[ \t\n\r]*'
                                    r'[aA][dD][dD][ \t\n\r]*'
                                    r'[uU][nN][iI][qQ][uU][eE][ \t\n\r]*'
                                    r'(\w*)[ \t\n\r]*'
                                    r'\([ \t\n\r]*(\w*)[ \t\n\r]*\)[ \t\n\r]*;?$')
    add_unique = add_unique_pattern.match(sql)
    if add_unique:
        return {'alter': add_unique.group(2),
                'add unique': add_unique.group(3),
                'add unique feature': [{'value': i} for i in add_unique.group(4).split(',')]}

    drop_index_pattern = re.compile(r'[ \t\n\r]*'
                                    r'[aA][lL][tT][eE][rR][ \t\n\r]*'
                                    r'[tT][aA][bB][lL][eE][ \t\n\r]*'
                                    r'(`|\b)(\w*)\1[ \t\n\r]*'
                                    r'[dD][rR][oO][pP][ \t\n\r]*'
                                    r'[iI][nN][dD][eE][xX][ \t\n\r]*'
                                    r'(\w*)[ \t\n\r]*;?$')
    drop_index = drop_index_pattern.match(sql)
    if drop_index:
        return {'alter': drop_index.group(2), 'drop index': drop_index.group(3)}

    return {}


def format_alter_key(json):
    if json.get('add primary key'):
        return f"alter table {json['alter']} " \
               f"add primary key({','.join([i['value'] for i in json['add primary key']])});"
    if json.get('drop primary key'):
        return f"alter table {json['alter']} drop primary key;"
    if json.get('add foreign key'):
        return f"alter table {json['alter']} add constraint {json['add constraint']} " \
               f"foreign key({','.join([i['value'] for i in json['add foreign key']])}) " \
               f"REFERENCES {json['reference_table']}" \
               f"({','.join([i['value'] for i in json['reference_feature']])});"
    if json.get('drop foreign key'):
        return f"alter table {json['alter']} drop foreign key {json['drop foreign key']};"
    if json.get('add index'):
        return f"alter table {json['alter']} " \
               f"add index {json['add index']}({','.join([i['value'] for i in json['add index feature']])});"
    if json.get('add unique'):
        return f"alter table {json['alter']} " \
               f"add unique {json['add unique']}({','.join([i['value'] for i in json['add unique feature']])});"
    if json.get('add fulltext'):
        return f"alter table {json['alter']} " \
               f"add fulltext {json['add fulltext']}({','.join([i['value'] for i in json['add fulltext feature']])});"
    if json.get('drop index'):
        return f"alter table {json['alter']} drop index {json['drop index']};"


_ = json.dumps

__all__ = ["parse", "format", "parse_mysql", "parse_bigquery", "normal_op", "simple_op"]
