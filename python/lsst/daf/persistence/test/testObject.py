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


class TestObject(object):
    """A basic object for testing. Contains a data blob that can be compared with other TestObject instances
    for equality (or inequality).
    """

    def __init__(self, data):
        self.data = data

    def __eq__(self, other):
        return self.data == other.data

    def __repr__(self):
        return "TestObject(data=%r)" % self.data


class TestObjectPair(object):
    """An object for testing that contains 2 objects.
    """

    def __init__(self, objA, objB):
        self.objA = objA
        self.objB = objB

    @staticmethod
    def assembler(dataId, componentDict, cls):
        return cls(componentDict['a'], componentDict['b'])

    @staticmethod
    def disassembler(obj, dataId, componentDict):
        componentDict['a'] = obj.objA
        componentDict['b'] = obj.objB

    def __repr__(self):
        return "TestObjectPair(objA=%r, objB=%r" % (self.objA, self.objB)
