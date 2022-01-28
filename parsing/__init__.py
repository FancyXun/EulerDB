# encoding: utf-8

# ORIGINALLY COPIED FROM pyparsing UNDER THE MIT LICENCE

# module pyparsing.py
#
# Copyright (c) 2003-2019  Paul T. McGuire
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
from parsing import whitespaces
from parsing.core import ParserElement, _PendingSkip
from parsing.enhancement import *
from parsing.exceptions import (
    ParseException,
    ParseException,
    ParseSyntaxException,
    RecursiveGrammarException,
)
from parsing.expressions import And, MatchAll, MatchFirst, Or, ParseExpression
from parsing.whitespaces import Whitespace

whitespaces.NO_WHITESPACE = Whitespace("").use()
whitespaces.STANDARD_WHITESPACE = Whitespace().use()

from parsing.infix import LEFT_ASSOC, RIGHT_ASSOC, infix_notation
from parsing.regex import Regex
from parsing.tokens import *

__all__ = [
    "And",
    "AnyChar",
    "CaselessKeyword",
    "CaselessLiteral",
    "CharsNotIn",
    "Combine",
    "Dict",
    "MatchAll",
    "Empty",
    "FollowedBy",
    "Forward",
    "Group",
    "Keyword",
    "LineEnd",
    "LineStart",
    "Literal",
    "LookAhead",
    "LookBehind",
    "PrecededBy",
    "Many",
    "MatchFirst",
    "NoMatch",
    "NotAny",
    "OneOrMore",
    "Optional",
    "Or",
    "ParseException",
    "ParseEnhancement",
    "ParseException",
    "ParseExpression",
    "ParseResults",
    "ParseSyntaxException",
    "ParserElement",
    "RecursiveGrammarException",
    "Regex",
    "SkipTo",
    "StringEnd",
    "StringStart",
    "Suppress",
    "Token",
    "TokenConverter",
    "White",
    "Word",
    "WordEnd",
    "WordStart",
    "ZeroOrMore",
    "Char",
    "LEFT_ASSOC",
    "RIGHT_ASSOC",
    "infix_notation",
    "CloseMatch",
]
