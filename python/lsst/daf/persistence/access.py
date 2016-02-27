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

import cPickle
import collections
import importlib
import os

from lsst.daf.persistence import LogicalLocation, Policy, butlerExceptions, StorageList, Persistence
import lsst.pex.policy as pexPolicy
import lsst.pex.logging as pexLog
from .safeFileIo import SafeFilename

# How to document Storage class protocol? Virtual base class seems somewhat non-pythonic?
# todo: https://docs.python.org/2/library/abc.html?highlight=abc#module-abc is acceptable.
class Storage(object):

    @staticmethod
    def getMapperClass(root):
        raise NotImplementedError("getMapperClass is not implemented by derived class")


class PosixStorage:

    @classmethod
    def cfg(cls, root):
        """Helper func to create a properly formatted Policy to configure a PosixStorage instance.

        :param root: a posix path where the repository is or should be created.
        :return:
        """
        return Policy({'root':root, 'cls':cls})

    def __init__(self, cfg):
        """Initializer

        :param cfg: a Policy that defines the configuration for this class. It is recommended that the cfg be
                    created by calling PosixStorage.cfg()
        :return:
        """
        self.log = pexLog.Log(pexLog.Log.getDefaultLog(), "daf.persistence.butler")
        self.root = cfg['root']
        if self.root and not os.path.exists(self.root):
            os.makedirs(self.root)

        # Always use an empty Persistence policy until we can get rid of it
        persistencePolicy = pexPolicy.Policy()
        self.persistence = Persistence.getPersistence(persistencePolicy)

    @staticmethod
    def getMapperClass(root):
        """Returns the mapper class associated with a repository root.

        Supports the legacy _parent symlink search (which was only ever posix-only. This should not be used by
        new code and repositories; they should use the Repository parentCfg mechanism."""
        if not (root):
            return None

        # Find a "_mapper" file containing the mapper class name
        basePath = root
        mapperFile = "_mapper"
        globals = {}
        while not os.path.exists(os.path.join(basePath, mapperFile)):
            # Break abstraction by following _parent links from CameraMapper
            if os.path.exists(os.path.join(basePath, "_parent")):
                basePath = os.path.join(basePath, "_parent")
            else:
                raise RuntimeError(
                        "No mapper provided and no %s available" %
                        (mapperFile,))
        mapperFile = os.path.join(basePath, mapperFile)

        # Read the name of the mapper class and instantiate it
        with open(mapperFile, "r") as f:
            mapperName = f.readline().strip()
        components = mapperName.split(".")
        if len(components) <= 1:
            raise RuntimeError("Unqualified mapper name %s in %s" %
                    (mapperName, mapperFile))
        pkg = importlib.import_module(".".join(components[:-1]))
        return getattr(pkg, components[-1])

    def mapperClass(self):
        """Get the class object for the mapper specified in the stored repository"""
        return PosixStorage.getMapperClass(self.root)

    def setCfg(self, repoCfg):
        """Writes the configuration to root in the repository on disk.

        :param repoCfg: the Policy cfg to be written
        :return: None
        """
        if not self.root:
            raise RuntimeError("Storage root was declared to be None.")
        path = os.path.join(self.root, 'repoCfg.yaml')
        repoCfg.dumpToFile(path)

    def loadCfg(self):
        """Reads the configuration from the repository on disk at root.

        :return: the Policy cfg
        """
        if not self.root:
            raise RuntimeError("Storage root was declared to be None.")
        path = os.path.join(self.root, 'repoCfg.yaml')
        return Policy(filePath=path)

    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by ButlerLocation

        :param butlerLocation: the location & formatting for the object to be written.
        :param obj: the object to be written.
        :return: None
        """
        self.log.log(pexLog.Log.DEBUG, "Put location=%s obj=%s" % (butlerLocation, obj))
        additionalData = butlerLocation.getAdditionalData()
        storageName = butlerLocation.getStorageName()
        locations = butlerLocation.getLocations()
        with SafeFilename(locations[0]) as locationString:
            logLoc = LogicalLocation(locationString, additionalData)

            if storageName == "PickleStorage":
                with open(logLoc.locString(), "wb") as outfile:
                    cPickle.dump(obj, outfile, cPickle.HIGHEST_PROTOCOL)
                return

            if storageName == "ConfigStorage":
                obj.save(logLoc.locString())
                return

            if storageName == "FitsCatalogStorage":
                flags = additionalData.getInt("flags", 0)
                obj.writeFits(logLoc.locString(), flags=flags)
                return

            # Create a list of Storages for the item.
            storageList = StorageList()
            storage = self.persistence.getPersistStorage(storageName, logLoc)
            storageList.append(storage)

            if storageName == 'FitsStorage':
                self.persistence.persist(obj, storageList, additionalData)
                return

            # Persist the item.
            if hasattr(obj, '__deref__'):
                # We have a smart pointer, so dereference it.
                self.persistence.persist(obj.__deref__(), storageList, additionalData)
            else:
                self.persistence.persist(obj, storageList, additionalData)

class Access:
    """Implements an butler framework interface for Transport, Storage, and Registry"""

    @classmethod
    def cfg(cls, storageCfg):
        """Helper func to create a properly formatted Policy to configure an Access instance.

        :param storageCfg: a cfg to instantiate a storage.
        :return:
        """
        return Policy({'storageCfg': storageCfg})

    def __init__(self, cfg):
        """Initializer

        :param cfg: a Policy that defines the configuration for this class. It is recommended that the cfg be
                    created by calling Access.cfg()
        :return:
        """
        self.storage = cfg['storageCfg.cls'](cfg['storageCfg'])

    def mapperClass(self):
        """Get the mapper class associated with a repository root.

        :return: the mapper class
        """
        return self.storage.mapperClass()

    def root(self):
        """Get the repository root as defined by the Storage class, this refers to the 'top' of a persisted
        repository. The exact type of Root can vary based on Storage type.

        :return: the root of the persisted repository.
        """

        return self.storage.root

    def setCfg(self, repoCfg):
        """Writes the repository configuration to Storage.

        :param repoCfg: the Policy cfg to be written
        :return: None
        """
        self.storage.setCfg(repoCfg)

    def loadCfg(self):
        """Reads the repository configuration from Storage.

        :return: the Policy cfg
        """
        return self.storage.loadCfg()

    def write(self, butlerLocation, obj):
        """Passes an object to Storage to be written into the repository.

        :param butlerLocation: the location & formatting for the object to be written.
        :param obj: the object to be written.
        :return: None
        """
        self.storage.write(butlerLocation, obj)
