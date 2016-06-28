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
    if not hasattr(x, '__iter__'):
        x = [x]
    else:
        x = list(x)
    return x


def setify(x):
    """Take an object x and return it in a set. 

    If x is a container, will create a set from the contents of the container.
    If x is an object, will create a set with a single item in it.
    If x is a string, will treat the string as a single object (i.e. not as a list of chars)"""
    if x is None:
        x = set()
    if isinstance(x, basestring):
        x = set([x])
    else:
        try:
            x = set(x)
        except TypeError:
            x= set([x])
    return x
