#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2016 LSST Corporation.
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
from past.builtins import basestring

try:
    from collections.abc import Sequence, Set, Mapping
except ImportError:
    from collections import Sequence, Set, Mapping


# -*- python -*-

def listify(x):
    """Takes any object and puts that whole object in a list:
    - strings will be made into a single element in the list
    - tuples will be converted to list
    - lists will remain as lists
    - None will be made into an empty list
    """
    if x is None:
        x = []
    elif isinstance(x, basestring):
        x = [x]
    elif isinstance(x, dict):
        x = [x]
    elif hasattr(x, '__iter__'):
        x = list(x)
    else:
        x = [x]
    return x


def iterify(x):
    """Takes any object. Returns it if it is iterable. If it
    is not iterable it puts the object in a list and returns
    the list. None will return an empty list. If a new list
    is always required use listify(). Strings will be placed
    in a list with a single element.
    """
    if x is None:
        x = []
    elif isinstance(x, basestring):
        x = [x]
    elif hasattr(x, '__iter__'):
        pass
    else:
        x = [x]
    return x


def sequencify(x):
    """Takes an object, if it is a sequence return it,
    else put it in a tuple. Strings are not sequences.
    If x is a dict, returns a sorted tuple of keys."""
    if isinstance(x, (Sequence, Set)) and not isinstance(x, basestring):
        pass
    elif isinstance(x, Mapping):
        x = tuple(sorted(x.keys()))
    else:
        x = (x, )
    return x


def setify(x):
    """Take an object x and return it in a set.

    If x is a container, will create a set from the contents of the container.
    If x is an object, will create a set with a single item in it.
    If x is a string, will treat the string as a single object (i.e. not as a list of chars)"""
    if x is None:
        x = set()

    # Here we have to explicity for strings because the set initializer will use each character in a string as
    # a separate element. We cannot use the braces initialization because x might be a list, and we do not
    # want the list to be an item; we want each item in the list to be represented by an item in the set.
    # Then, we have to fall back to braces init because if the item is NOT a list then the set initializer
    # won't take it.
    if isinstance(x, basestring):
        x = set([x])
    else:
        try:
            x = set(x)
        except TypeError:
            x = set([x])
    return x


def doImport(pythonType):
    """Import a python object given an importable string"""
    try:
        if not isinstance(pythonType, basestring):
            raise TypeError("Unhandled type of pythonType, val:%s" % pythonType)
        # import this pythonType dynamically
        pythonTypeTokenList = pythonType.split('.')
        importClassString = pythonTypeTokenList.pop()
        importClassString = importClassString.strip()
        importPackage = ".".join(pythonTypeTokenList)
        importType = __import__(importPackage, globals(), locals(), [importClassString], 0)
        pythonType = getattr(importType, importClassString)
        return pythonType
    except ImportError:
        pass
    # maybe python type is a member function, in the form: path.to.object.Class.funcname
    pythonTypeTokenList = pythonType.split('.')
    importClassString = '.'.join(pythonTypeTokenList[0:-1])
    importedClass = doImport(importClassString)
    pythonType = getattr(importedClass, pythonTypeTokenList[-1])
    return pythonType
