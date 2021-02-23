#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

# -*- python -*-

"""This module defines the ReadProxy class."""

from .persistence import ReadProxyBase


class ReadProxy(ReadProxyBase):
    """ReadProxy provides a lazy-loading object that is initialized by a
    callback function set in ReadProxy's constructor.  Adapted from
    peak.util.proxies.LazyProxy, which was written by Phillip J. Eby
    (peak@eby-sarna.com)."""

    __slots__ = ('__cache__', '__callback__')

    def __init__(self, func):
        ReadProxyBase.__init__(self)
        set_callback(self, func)

    def __getattr__(self, attr):
        getattr(self.__subject__, attr)

    def __setattr__(self, attr, val):
        setattr(self.__subject__, attr, val)

    def __delattr__(self, attr):
        delattr(self.__subject__, attr)

    def __bool__(self):
        return bool(self.__subject__)

    def __getitem__(self, arg):
        return self.__subject__[arg]

    def __setitem__(self, arg, val):
        self.__subject__[arg] = val

    def __delitem__(self, arg):
        del self.__subject__[arg]

    def __getslice__(self, i, j):
        return self.__subject__[i:j]

    def __setslice__(self, i, j, val):
        self.__subject__[i:j] = val

    def __delslice__(self, i, j):
        del self.__subject__[i:j]

    def __contains__(self, ob):
        return ob in self.__subject__

    for name in 'repr str hash len abs complex int long float iter oct hex'.split():
        exec("def __%s__(self): return %s(self.__subject__)" % (name, name))

    for name in 'cmp', 'coerce', 'divmod':
        exec("def __%s__(self, ob): return %s(self.__subject__, ob)" % (name,
                                                                        name))

    for name, op in [
        ('lt', '<'), ('gt', '>'), ('le', '<='), ('ge', '>='),
        ('eq', '=='), ('ne', '!=')
    ]:
        exec("def __%s__(self, ob): return self.__subject__ %s ob" % (name, op))

    for name, op in [('neg', '-'), ('pos', '+'), ('invert', '~')]:
        exec("def __%s__(self): return %s self.__subject__" % (name, op))

    for name, op in [
        ('or', '|'), ('and', '&'), ('xor', '^'),
        ('lshift', '<<'), ('rshift', '>>'),
        ('add', '+'), ('sub', '-'), ('mul', '*'), ('div', '/'),
        ('mod', '%'), ('truediv', '/'), ('floordiv', '//')
    ]:
        exec((
            "def __%(name)s__(self,ob):\n"
            "    return self.__subject__ %(op)s ob\n"
            "\n"
            "def __r%(name)s__(self,ob):\n"
            "    return ob %(op)s self.__subject__\n"
            "\n"
            "def __i%(name)s__(self,ob):\n"
            "    self.__subject__ %(op)s=ob\n"
            "    return self\n"
        ) % locals())

    del name, op

    # Oddball signatures

    def __rdivmod__(self, ob):
        return divmod(ob, self.__subject__)

    def __pow__(self, *args):
        return pow(self.__subject__, *args)

    def __ipow__(self, ob):
        self.__subject__ **= ob
        return self

    def __rpow__(self, ob):
        return pow(ob, self.__subject__)


get_callback = ReadProxy.__callback__.__get__
set_callback = ReadProxy.__callback__.__set__
get_cache = ReadProxy.__cache__.__get__
set_cache = ReadProxy.__cache__.__set__


def _subject(self, get_cache=get_cache, set_cache=set_cache):
    try:
        return get_cache(self)
    except AttributeError:
        set_cache(self, get_callback(self)())
        return get_cache(self)


ReadProxy.__subject__ = property(_subject, set_cache)
del _subject
