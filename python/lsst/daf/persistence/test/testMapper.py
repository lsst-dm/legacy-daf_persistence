#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
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

import os

from .. import ButlerLocation
from .. import Mapper
from .. import Storage
from . import TestObject


class EmptyTestMapper(Mapper):
    """Class that can be used as a stub for a mapper."""

    def __init__(self, root=None, parentRegistry=None, repositoryCfg=None, **kwargs):
        self.kwargs = kwargs
        pass


class MapperForTestWriting(Mapper):

    def __init__(self, root, **kwargs):
        self.root = root
        self.storage = Storage.makeFromURI(self.root)

    def map_foo(self, dataId, write):
        python = TestObject
        persistable = None
        storage = 'PickleStorage'
        fileName = 'filename'
        for key, value in dataId.items():
            fileName += '_' + key + str(value)
        fileName += '.txt'
        path = os.path.join(self.root, fileName)
        if not write and not os.path.exists(path):
            return None
        return ButlerLocation(python, persistable, storage, path, dataId, self, self.storage)
