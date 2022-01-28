#
# Copyright (C) 2009-2020 the parse authors and contributors
# <see AUTHORS file>
#
# This module is part of python-parse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

"""filter"""

from scheduler.compile.parse import lexer
from scheduler.compile.parse.engine import grouping
from scheduler.compile.parse.engine.statement_splitter import StatementSplitter


class FilterStack:
    def __init__(self):
        self.preprocess = []
        self.stmtprocess = []
        self.postprocess = []
        self._grouping = False

    def enable_grouping(self):
        self._grouping = True

    def run(self, sql, encoding=None):
        stream = lexer.tokenize(sql, encoding)
        # Process token stream
        for filter_ in self.preprocess:
            stream = filter_.process(stream)

        stream = StatementSplitter().process(stream)

        # Output: Stream processed Statements
        for stmt in stream:
            if self._grouping:
                stmt = grouping.group(stmt)

            for filter_ in self.stmtprocess:
                filter_.process(stmt)

            for filter_ in self.postprocess:
                stmt = filter_.process(stmt)

            yield stmt
