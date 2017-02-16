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
import os


class CameraMapper(dafPersist.Mapper):

    def __init__(self, *args, **kwargs):
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

    def _formatMap(self, ch, k, datasetType):
        if ch in "diouxX":
            return int
        elif ch in "eEfFgG":
            return float
        elif ch in "crs":
            return str
        else:
            raise RuntimeError("Unexpected format specifier %s"
                               " for field %s in template for dataset %s" %
                               (ch, k, datasetType))

    def getKeys(self, datasetType, level):
        if level == '':
            level = self.getDefaultLevel()

        keyDict = dict()
        if datasetType is None:
            for t in self.templates:
                keyDict.update(self.getKeys(t))
        else:
            d = dict([
                (k, self._formatMap(v, k, datasetType))
                for k, v in
                re.findall(r'\%\((\w+)\).*?([diouxXeEfFgGcrs])',
                           self.templates[datasetType])
            ])
            keyDict.update(d)
        if level is not None:
            for l in self.levels[level]:
                if l in keyDict:
                    del keyDict[l]
        return keyDict

    def getDefaultLevel(self):
        return "sensor"

    def getDefaultSubLevel(self, level):
        if level == '':
            level = self.getDefaultLevel()
        return dict(
            sensor="amp",
            raft="sensor",
            visit="sensor",
            skyTile="sensor")[level]

    def query(self, datasetType, format, dataId):
        return self.registry.query(datasetType, format, dataId)

    def map(self, datasetType, dataId, write=False):
        path = self.templates[datasetType] % dataId
        return dafPersist.ButlerLocation(
            None, None, "PickleStorage", path, {}, self,
            dafPersist.Storage.makeFromURI(self.root))


for datasetType in ["raw", "flat", "calexp"]:
    setattr(CameraMapper, "map_" + datasetType,
            lambda self, dataId, write:
            CameraMapper.map(self, datasetType, dataId, write))
    setattr(CameraMapper, "query_" + datasetType,
            lambda self, format, dataId:
            CameraMapper.query(self, datasetType, format, dataId))
