#
# Copyright (C) 2009-2020 the parse authors and contributors
# <see AUTHORS file>
#
# This module is part of python-parse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

"""Parse SQL statements."""

# Setup namespace
from src.compile.parse import formatter, filters
from src.compile.parse import engine

__all__ = ['engine', 'filters', 'formatter', 'sql', 'tokens', 'cli']


def parse(sql, encoding=None):
    """Parse compile and return a list of statements.

    :param sql: A string containing one or more SQL statements.
    :param encoding: The encoding of the statement (optional).
    :returns: A tuple of :class:`~parse.compile.Statement` instances.
    """
    return tuple(parsestream(sql, encoding))


def parsestream(stream, encoding=None):
    """Parses compile statements from file-like object.

    :param stream: A file-like object.
    :param encoding: The encoding of the stream contents (optional).
    :returns: A generator of :class:`~parse.compile.Statement` instances.
    """
    stack = engine.FilterStack()
    stack.enable_grouping()
    return stack.run(stream, encoding)


def format(sql, encoding=None, **options):
    """Format *compile* according to *options*.

    Available options are documented in :ref:`formatting`.

    In addition to the formatting options this function accepts the
    keyword "encoding" which determines the encoding of the statement.

    :returns: The formatted SQL statement as string.
    """
    stack = engine.FilterStack()
    options = formatter.validate_options(options)
    stack = formatter.build_filter_stack(stack, options)
    stack.postprocess.append(filters.SerializerUnicode())
    return ''.join(stack.run(sql, encoding))


def split(sql, encoding=None):
    """Split *compile* into single statements.

    :param sql: A string containing one or more SQL statements.
    :param encoding: The encoding of the statement (optional).
    :returns: A list of strings.
    """
    stack = engine.FilterStack()
    return [str(stmt).strip() for stmt in stack.run(sql, encoding)]
