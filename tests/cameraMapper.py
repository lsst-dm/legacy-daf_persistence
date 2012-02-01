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


import re
import lsst.daf.persistence as dafPersist

class CameraMapper(dafPersist.Mapper):
    def __init__(self):
        self.templates = dict(
            raw="raw_v%(visit)d_R%(raft)s_S%(sensor)s_C%(amp)s_E%(snap)03d.pickle",
            flat="flat_R%(raft)s_S%(sensor)s_C%(amp)s_E%(snap)03d.pickle",
            calexp="calexp_v%(visit)d_R%(raft)s_S%(sensor)s.pickle")
        self.synonyms = dict(
                ccd="sensor",
                channel="amp"
                )
        self.levels = dict(
                skyTile=["visit", "raft", "sensor"],
                visit=["snap", "raft", "sensor", "amp"],
                raft=["snap", "sensor", "amp"],
                sensor=["snap", "amp"],
                amp=[])


    def getKeys(self, datasetType, level):
        keySet = set()
        if datasetType is None:
            for t in self.templates.iterkeys():
                keySet.update(self.getKeys(t))
        else:
            keySet.update(re.findall(r'\%\((\w+)\)',
                self.templates[datasetType]))
        if level is not None:
            keySet -= set(self.levels[level])
        return keySet

    def getDefaultLevel(self):
        return "sensor"

    def getDefaultSubLevel(self, level):
        return dict(
                sensor="amp",
                raft="sensor",
                visit="sensor",
                skyTile="sensor")[level]

    def query(self, datasetType, key, format, dataId):
        return self.registry.query(datasetType, key, format, dataId)

    def map(self, datasetType, dataId):
        path = self.templates[datasetType] % dataId
        return dafPersist.ButlerLocation(None, None, "PickleStorage", path, {})

for datasetType in ["raw", "flat", "calexp"]:
    setattr(CameraMapper, "map_" + datasetType,
            lambda self, dataId:
            CameraMapper.map(self, datasetType, dataId))
    setattr(CameraMapper, "query_" + datasetType,
            lambda self, key, format, dataId:
            CameraMapper.query(self, datasetType, key, format, dataId))
