from scheduler.handers.clause.sql_where import SQLWhere
from scheduler.handers.clause.sql_set import SQLSet
from scheduler.handers.clause.sql_select import SQLSelect
from scheduler.handers.clause.sql_from import SQLFrom
from scheduler.handers.clause.anonymous_table import AnonymousTable
from scheduler.handers.clause.function import OrderBy
from scheduler.handers.clause.sql_columns import SQLColumns
from scheduler.handers.clause.sql_values import SQLValues
from scheduler.handers.clause.sql_drop import SQLDrop

clause = {
    'insert': AnonymousTable,
    'delete': AnonymousTable,
    'update': AnonymousTable,
    'select': SQLSelect,
    'select_distinct': SQLSelect,
    'from': SQLFrom,
    'where': SQLWhere,
    'set': SQLSet,
    'orderby': OrderBy,
    'columns': SQLColumns,
    'drop': SQLDrop,
    'query': {'select': SQLValues}
}

table_key = [
    'insert',
    'from',
    'select_distinct',
    'update',
    'delete',
    'drop']


