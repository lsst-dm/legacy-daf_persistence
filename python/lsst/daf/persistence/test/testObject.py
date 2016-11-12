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

    def __ne__(self, other):
        return self.data != other.data

    def __repr__(self):
        return "TestObject(data=%r)" % self.data


class TestObjectPair(object):
    """An object for testing that contains 2 objects.
    """

    def __init__(self, objA=None, objB=None):
        self.objA = objA
        self.objB = objB
        self.usedInitSetter = True if objA and objB else False
        self.usedASetter = False
        self.usedBSetter = False

    @staticmethod
    def assembler(dataId, componentInfo, cls):
        return cls(componentInfo['a'].obj, componentInfo['b'].obj)

    @staticmethod
    def disassembler(obj, dataId, componentInfo):
        componentInfo['a'].obj = obj.objA
        componentInfo['b'].obj = obj.objB

    def __repr__(self):
        return "TestObjectPair(objA=%r, objB=%r" % (self.objA, self.objB)

    def set_a(self, obj):
        self.objA = obj
        self.usedASetter = True

    def set_b(self, obj):
        self.objB = obj
        self.usedBSetter = True

    def get_a(self):
        return self.objA

    def get_b(self):
        return self.objB


class TestObjectCamelCaseSetter(object):
    """A test object with camel case setter and getter e.g. `def setFoo...`"""
    def __init__(self):
        self._foo = None

    def setFoo(self, val):
        self._foo = val

    def getFoo(self):
        return self._foo


class TestObjectUnderscoreSetter(object):
    """A test object with lower case camel case setter and getter e.g. `def setFoo...`"""
    def __init__(self):
        self._foo = None

    def set_foo(self, val):
        self._foo = val

    def get_foo(self):
        return self._foo

