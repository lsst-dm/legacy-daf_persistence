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
import os
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


    class ComponentInfo():
        """Information about a butler composite object. Some details come from the policy and some are filled
        in by the butler. Component info is used while assembling and disassembling a composite object in
        butler. It is used as an input to assemblers and disassemblers (which are part of the butler public
        API).

        Parameters
        ----------
        datasetType : string
            The datasetType of the component.
        obj : object instance
            The python object instance that is this component.
        setter : string
            The name of the function in the parent object to set this component.
            Optional - may be None
        getter : string
            The name of the function in the parent object to get this component.
            Optional - may be None
        subset : bool
            If true, indicates that the obj should be a list of objects found via butlerSubset.
        inputOnly : bool
            If true, indicates that the obj should not be serialized when performing a butler.put.
        """
        def __init__(self, datasetType, obj, setter, getter, subset, inputOnly):
            self.datasetType = datasetType
            self.obj = obj
            self.setter = setter
            self.getter = getter
            self.subset = subset
            self.inputOnly = inputOnly

        def __repr__(self):
            return 'ComponentInfo(datasetType:%s, obj:%s, setter:%s, getter:%s, subset:%s)' % \
                    (self.datasetType, self.obj, self.setter, self.getter, self.subset)


    def __repr__(self):
        return 'ButlerComposite(assembler:%s, disassembler:%s, python:%s, dataId:%s, mapper:%s, componentInfo:%s, repository:%s)' % \
                (self.assembler,
                self.disassembler,
                self.python,
                self.dataId,
                self.mapper,
                self.componentInfo,
                self.repository)

        def __repr__(self):
            return "ComponentInfo(datasetType=%s, obj=%s, setter=%s, getter=%s)" % (
                self.datasetType, self.obj, self.setter, self.getter)


    def __init__(self, assembler, disassembler, python, dataId, mapper):
        self.assembler = doImport(assembler) if isinstance(assembler, basestring) else assembler
        self.disassembler = doImport(disassembler) if isinstance(disassembler, basestring) else disassembler
        self.python = doImport(python) if isinstance(python, basestring) else python
        self.dataId = dataId
        self.mapper = mapper
        self.componentInfo = {}
        self.repository = None

    def add(self, id, datasetType, setter, getter, subset, inputOnly):
        """Add a description of a component needed to fetch the composite dataset.

        Parameters
        ----------
        id : string
            The name of the component in the policy definition.
        datasetType : string
            The name of the datasetType of the component.
        setter : string or None
            The name of the function used to set this component into the python type that contains it.
            Specifying a setter is optional, use None if the setter won't be specified or used.
        getter : string or None
            The name of the function used to get this component from the python type that contains it.
            Specifying a setter is optional, use None if the setter won't be specified or used.
        subset : bool
            If true, indicates that the obj should be a list of objects found via butlerSubset.
        inputOnly : bool
            If true, indicates that the obj should not be serialized when performing a butler.put.
        """
        self.componentInfo[id] = ButlerComposite.ComponentInfo(datasetType=datasetType,
                                                               obj = None,
                                                               setter=setter,
                                                               getter=getter,
                                                               subset=subset,
                                                               inputOnly=inputOnly)

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
    map_{datasetType} methods.

    Parameters
    ----------
    pythonType - string or class instance
        This is the type of python object that should be created when reading the location.

    cppType - string or None
        The type of cpp object represented by the location (optional, may be None)

    storageName - string
        The type of storage the object is in or should be place into.

    locationList - list of string
        A list of URI to place the object or where the object might be found. (Typically when reading the
        length is expected to be exactly 1).

    dataId - dict
        The dataId that was passed in when mapping the location. This may include keys that were not used for
        mapping this location.

    mapper - mapper class instance
        The mapper object that mapped this location.

    storage - storage class instance
        The storage interface that can be used to read or write this location.

    usedDataId - dict
        The dataId components that were used to map this location. If the mapper had to look up keys those
        will be in this dict (even though they may not appear in the dataId parameter). If the dataId
        parameter contained keys that were not required to map this item then those keys will NOT be in this
        parameter.

    datasetType - string
        The datasetType that this location represents.
    """

    yaml_tag = u"!ButlerLocation"
    yaml_loader = yaml.Loader
    yaml_dumper = yaml.Dumper

    def __repr__(self):
        return \
            'ButlerLocation(pythonType=%r, cppType=%r, storageName=%r, storage=%r, locationList=%r,' \
            ' additionalData=%r, mapper=%r, dataId=%r)' % \
            (self.pythonType, self.cppType, self.storageName, self.storage, self.locationList,
             self.additionalData, self.mapper, self.dataId)

    def __init__(self, pythonType, cppType, storageName, locationList, dataId, mapper, storage=None,
                 usedDataId=None, datasetType=None):
        # pythonType is sometimes unicode with Python 2 and pybind11; this breaks the interpreter
        self.pythonType = str(pythonType) if isinstance(pythonType, basestring) else pythonType
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

    def getLocationsWithRoot(self):
        return [os.path.join(self.storage.root, l) for l in self.getLocations()]

    def getAdditionalData(self):
        return self.additionalData

    def getStorage(self):
        return self.storage
