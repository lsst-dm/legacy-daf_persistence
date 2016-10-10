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


"""This module defines the ButlerLocation class."""

import lsst.daf.base as dafBase

from collections import namedtuple
from past.builtins import basestring
import yaml

from . import iterify, doImport


class ButlerComposite(object):
    """Initializer

    Parameters
    ----------
    assembler : function object
        Function object or importable string to a function object that can be called with the assembler
        signature: (dataId, componentDict, cls).
    disassembler : function object
        Function object or importable string to a function object that can be called with the disassembler
        signature: (object, dataId, componentDict).
    python : class object
        A python class object or importable string to a class object that can be used by the assembler to
        instantiate an object to be returned.
    dataId : dict or DataId
        The dataId that is used to look up components.
    mapper : Mapper instance
        A reference to the mapper that created this ButlerComposite object.
    """

    ComponentInfo = namedtuple('ComponentInfo', 'datasetType')

    def __init__(self, assembler, disassembler, python, dataId, mapper):
        self.assembler = doImport(assembler) if isinstance(assembler, basestring) else assembler
        self.disassembler = doImport(disassembler) if isinstance(disassembler, basestring) else disassembler
        self.python = doImport(python) if isinstance(python, basestring) else python
        self.dataId = dataId
        self.mapper = mapper
        self.componentInfo = {}
        self.repository = None

    def add(self, id, datasetType):
        """Add a description of a component needed to fetch the composite dataset.

        Parameters
        ----------
        id : string
            The name of the component in the policy definition.
        datasetType : string
            The name of the datasetType of the component.
        """
        self.componentInfo[id] = ButlerComposite.ComponentInfo(datasetType=datasetType)

    def __repr__(self):
        return "ButlerComposite(assembler=%s, disassembler=%s, python=%s, dataId=%s, components=%s)" % (
            self.assembler, self.disassembler, self.python, self.dataId, self.componentInfo)

    def setRepository(self, repository):
        self.repository = repository

    def getRepository(self):
        return self.repository


class ButlerLocation(yaml.YAMLObject):
    """ButlerLocation is a struct-like class that holds information needed to
    persist and retrieve an object using the LSST Persistence Framework.

    Mappers should create and return ButlerLocations from their
    map_{datasetType} methods."""

    yaml_tag = u"!ButlerLocation"
    yaml_loader = yaml.Loader
    yaml_dumper = yaml.Dumper

    def __repr__(self):
        return \
            'ButlerLocation(pythonType=%r, cppType=%r, storageName=%r, locationList=%r,' \
            ' additionalData=%r, mapper=%r, dataId=%r)' % \
            (self.pythonType, self.cppType, self.storageName, self.locationList,
             self.additionalData, self.mapper, self.dataId)

    def __init__(self, pythonType, cppType, storageName, locationList, dataId, mapper, storage=None,
                 usedDataId=None, datasetType=None):
        self.pythonType = pythonType
        self.cppType = cppType
        self.storageName = storageName
        self.mapper = mapper
        self.storage = storage
        self.locationList = iterify(locationList)
        self.additionalData = dafBase.PropertySet()
        for k, v in dataId.items():
            self.additionalData.set(k, v)
        self.dataId = dataId
        self.usedDataId = usedDataId
        self.datasetType = datasetType

    def __str__(self):
        s = "%s at %s(%s)" % (self.pythonType, self.storageName,
                              ", ".join(self.locationList))
        return s

    @staticmethod
    def to_yaml(dumper, obj):
        """Representer for dumping to YAML
        :param dumper:
        :param obj:
        :return:
        """
        return dumper.represent_mapping(ButlerLocation.yaml_tag,
                                        {'pythonType': obj.pythonType, 'cppType': obj.cppType,
                                         'storageName': obj.storageName,
                                         'locationList': obj.locationList, 'mapper': obj.mapper,
                                         'storage': obj.storage, 'dataId': obj.dataId})

    @staticmethod
    def from_yaml(loader, node):
        obj = loader.construct_mapping(node)
        return ButlerLocation(**obj)

    def setRepository(self, repository):
        self.repository = repository

    def getRepository(self):
        return self.repository

    def getPythonType(self):
        return self.pythonType

    def getCppType(self):
        return self.cppType

    def getStorageName(self):
        return self.storageName

    def getLocations(self):
        return self.locationList

    def getAdditionalData(self):
        return self.additionalData
