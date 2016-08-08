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


import os
import lsst.daf.persistence as dafPersist


class PickleMapper(dafPersist.Mapper):

    def __init__(self, root=None, outPath=''):
        self.root = root
        self.outPath = outPath

    def map_x(self, dataId, write):
        path = "foo%(ccd)d.pickle" % dataId
        path = os.path.join(self.root, self.outPath, path)
        return dafPersist.ButlerLocation(None, None, "PickleStorage", path, {}, self)
