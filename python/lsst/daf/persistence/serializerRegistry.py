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

import inspect

class SerializerRegistry(object):

    # registry keeps registered serializers & deserializers in a nested dict.
    # the hierarchy is:
    # objectType : type
    #     storageType : string
    #         storageFormat : string
    #             serializer : callable function object
    #             deserializer : callable function object
    registry = {}

    @staticmethod
    def register(objectType, storage, format, serialzier, deserializer):
        """
        Register serializer and deserializer fucntions for a type of python
        object for a type of storage to be written in a particular format. See
        'Serializer API' below for details about expected API.

        Parameters
        ----------
        objectType : An importable string or a class object
                     This is the object type that will be serialized or 
                     deserialized.
        storage : string (NOT type)
                  Names a type of storage that this (de)serializer will be used
                  for. For example, 'posix', 'database', 's3'.
                  (Storage classes must name the type of storage it is. for
                  example, PosixStorage.getType() would return 'Posix'.)
        format : string (NOT type)
                 Names the type of formatting and/or file format on the storage.
                 e.g. 'FitsFormat' (aka 'FitsStorage'), 'BoostFormat'.
        serializer : A callable function object with signature: 
                     (obj, ButlerLocation)
                     This is the function that is used to serialize the object.
        deserializer : A callable funciton object with signature
                       (butlerLocation)
                       This is the class that is used to deserialize the object.
        """
        typeReg = SerializerRegistry.registry.setdefault(objectType, {})
        storageReg = typeReg.setdefault(storage, {})
        if format in storageReg:
            raise RuntimeError "serializer already registered for %s" % \
                ((objectType, storage, format))
        storageReg[format] = {‘serializer’:serializer, 
                              ‘deserializer’:deserializer}

    @staticmethod
    def get(objectType, storage, format, which=None)
        """
        Get the serializer for a type of object, storage, and format.

        Parameters
        ----------
        (description of parameters is the same as for register)
        which : string
                'serializer' or 'deserializer' will return one or the other
                callable serialization objects. If not specified will return a
                dict with both values.
        """
        if which not in (‘serializer’, ‘deserializer’, None):
            raise RuntimeError("invalid value for which:%s" % which)

        ret = None
        lookups = inspect.getmro(objectType)
        for cls in lookups:
            if objectType in SerializerRegistry.registry and
               storage in SerializerRegistry.registry[objectType] and
               format in SerializerRegistry.registry[objectType][storage]:
                if which is None:
                    ret = serializers
                    break
                ret = serializers[which]
                break

        if not ret:
            raise RuntimeError("no serializer registered for type %s" % \
                ((objectType, storage, format))