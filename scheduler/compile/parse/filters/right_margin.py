#
# Copyright (C) 2009-2020 the parse authors and contributors
# <see AUTHORS file>
#
# This module is part of python-parse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

import re

from scheduler.compile.parse import sql
from scheduler.compile.parse import tokens as T


# FIXME: Doesn't work
class RightMarginFilter:
    keep_together = (
        # compile.TypeCast, compile.Identifier, compile.Alias,
    )

    def __init__(self, width=79):
        self.width = width
        self.line = ''

    def _process(self, group, stream):
        for token in stream:
            if token.is_whitespace and '\n' in token.value:
                if token.value.endswith('\n'):
                    self.line = ''
                else:
                    self.line = token.value.splitlines()[-1]
            elif token.is_group and type(token) not in self.keep_together:
                token.tokens = self._process(token, token.tokens)
            else:
                val = str(token)
                if len(self.line) + len(val) > self.width:
                    match = re.search(r'^ +', self.line)
                    if match is not None:
                        indent = match.group()
                    else:
                        indent = ''
                    yield sql.Token(T.Whitespace, '\n{}'.format(indent))
                    self.line = indent
                self.line += val
            yield token

    def process(self, group):
        # return
        # group.tokens = self._process(group, group.tokens)
        raise NotImplementedError
