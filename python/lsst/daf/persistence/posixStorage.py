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
from future import standard_library
standard_library.install_aliases()
from past.builtins import basestring
import sys
import copy
import pickle
import importlib
import os
import urllib.parse
import glob
import shutil

import yaml

from . import (LogicalLocation, Persistence, Policy, StorageList, Registry,
               Storage, RepositoryCfg, safeFileIo, ButlerLocation)
from lsst.log import Log
import lsst.pex.policy as pexPolicy
from .safeFileIo import SafeFilename


class PosixStorage(Storage):

    def __init__(self, uri):
        """Initializer

        :return:
        """
        self.log = Log.getLogger("daf.persistence.butler")
        self.root = parseRes = urllib.parse.urlparse(uri).path
        if self.root and not os.path.exists(self.root):
            os.makedirs(self.root)

        # Always use an empty Persistence policy until we can get rid of it
        persistencePolicy = pexPolicy.Policy()
        self.persistence = Persistence.getPersistence(persistencePolicy)

        self.registry = Registry.create(location=self.root)

    def __repr__(self):
        return 'PosixStorage(root=%s)' % self.root

    @staticmethod
    def relativePath(fromPath, toPath):
        """Get a relative path from a location to a location.

        Parameters
        ----------
        fromPath : string
            A path at which to start. It can be a relative path or an
            absolute path.
        toPath : string
            A target location. It can be a relative path or an absolute path.

        Returns
        -------
        string
            A relative path that describes the path from fromPath to toPath.
        """
        return os.path.relpath(toPath, fromPath)

    @staticmethod
    def absolutePath(fromPath, relativePath):
        """Get an absolute path for the path from fromUri to toUri

        Parameters
        ----------
        fromPath : the starting location
            A location at which to start. It can be a relative path or an
            absolute path.
        relativePath : the location relative to fromPath
            A relative path.

        Returns
        -------
        string
            Path that is an absolute path representation of fromPath +
            relativePath, if one exists. If relativePath is absolute or if
            fromPath is not related to relativePath then relativePath will be
            returned.
        """
        if os.path.isabs(relativePath):
            return relativePath
        if not os.path.isabs(fromPath):
            fromPath = os.path.abspath(fromPath)
        return os.path.normpath(os.path.join(fromPath, relativePath))

    @staticmethod
    def _getRepositoryCfg(uri):
        """Get a persisted RepositoryCfg
        """
        repositoryCfg = None
        parseRes = urllib.parse.urlparse(uri)
        loc = os.path.join(parseRes.path, 'repositoryCfg.yaml')
        if os.path.exists(loc):
            with open(loc, 'r') as f:
                repositoryCfg = yaml.load(f)
            if repositoryCfg.root is None:
                repositoryCfg.root = uri
        return repositoryCfg

    @staticmethod
    def getRepositoryCfg(uri):
        repositoryCfg = PosixStorage._getRepositoryCfg(uri)
        if repositoryCfg is not None:
            return repositoryCfg

        return repositoryCfg

    @staticmethod
    def putRepositoryCfg(cfg, loc=None):
        if loc is None or cfg.root == loc:
            # the cfg is at the root location of the repository so don't write root, let it be implicit in the
            # location of the cfg.
            cfg = copy.copy(cfg)
            loc = cfg.root
            cfg.root = None
        # This class supports schema 'file' and also treats no schema as 'file'.
        # Split the URI and take only the path; remove the schema fom loc if it's there.
        parseRes = urllib.parse.urlparse(loc)
        loc = parseRes.path
        if not os.path.exists(loc):
            os.makedirs(loc)
        loc = os.path.join(loc, 'repositoryCfg.yaml')
        with safeFileIo.FileForWriteOnceCompareSame(loc) as f:
            yaml.dump(cfg, f)

    @staticmethod
    def getMapperClass(root):
        """Get the mapper class associated with a repository root.

        Supports the legacy _parent symlink search (which was only ever posix-only. This should not be used by
        new code and repositories; they should use the Repository parentCfg mechanism.

        :param root: the location of a persisted ReositoryCfg is (new style repos), or the location where a
                     _mapper file is (old style repos).
        :return: a class object or a class instance, depending on the state of the mapper when the repository
                 was created.
        """
        if not (root):
            return None

        cfg = PosixStorage._getRepositoryCfg(root)
        if cfg is not None:
            return cfg.mapper

        # Find a "_mapper" file containing the mapper class name
        basePath = root
        mapperFile = "_mapper"
        while not os.path.exists(os.path.join(basePath, mapperFile)):
            # Break abstraction by following _parent links from CameraMapper
            if os.path.exists(os.path.join(basePath, "_parent")):
                basePath = os.path.join(basePath, "_parent")
            else:
                mapperFile = None
                break

        if mapperFile is not None:
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

        return None

    @staticmethod
    def getParentSymlinkPath(root):
        """For Butler V1 Repositories only, if a _parent symlink exists, get the location pointed to by the
        symlink.

        Parameters
        ----------
        root : string
            A path to the folder on the local filesystem.

        Returns
        -------
        string or None
            A path to the parent folder indicated by the _parent symlink, or None if there is no _parent
            symlink at root.
        """
        linkpath = os.path.join(root, '_parent')
        if os.path.exists(linkpath):
            try:
                return os.readlink(os.path.join(root, '_parent'))
            except OSError:
                # some of the unit tests rely on a folder called _parent instead of a symlink to aother
                # location. Allow that; return the path of that folder.
                return os.path.join(root, '_parent')
        return None

    def mapperClass(self):
        """Get the class object for the mapper specified in the stored repository"""
        return PosixStorage.getMapperClass(self.root)

    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by ButlerLocation

        :param butlerLocation: the location & formatting for the object to be written.
        :param obj: the object to be written.
        :return: None
        """
        self.log.debug("Put location=%s obj=%s", butlerLocation, obj)

        additionalData = butlerLocation.getAdditionalData()
        storageName = butlerLocation.getStorageName()
        locations = butlerLocation.getLocations()

        pythonType = butlerLocation.getPythonType()
        if pythonType is not None:
            if isinstance(pythonType, basestring):
                # import this pythonType dynamically
                pythonTypeTokenList = pythonType.split('.')
                importClassString = pythonTypeTokenList.pop()
                importClassString = importClassString.strip()
                importPackage = ".".join(pythonTypeTokenList)
                importType = __import__(importPackage, globals(), locals(), [importClassString], 0)
                pythonType = getattr(importType, importClassString)
        # todo this effectively defines the butler posix "do serialize" command to be named "put". This has
        # implications; write now I'm worried that any python type that can be written to disk and has a
        # method called 'put' will be called here (even if it's e.g. destined for FitsStorage).
        # We might want a somewhat more specific API.
        if hasattr(pythonType, 'butlerWrite'):
            pythonType.butlerWrite(obj, butlerLocation=butlerLocation)
            return

        with SafeFilename(os.path.join(self.root, locations[0])) as locationString:
            logLoc = LogicalLocation(locationString, additionalData)

            if storageName == "PickleStorage":
                with open(logLoc.locString(), "wb") as outfile:
                    pickle.dump(obj, outfile, pickle.HIGHEST_PROTOCOL)
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

    def read(self, butlerLocation):
        """Read from a butlerLocation.

        :param butlerLocation:
        :return: a list of objects as described by the butler location. One item for each location in
                 butlerLocation.getLocations()
        """
        additionalData = butlerLocation.getAdditionalData()
        # Create a list of Storages for the item.
        storageName = butlerLocation.getStorageName()
        results = []
        locations = butlerLocation.getLocations()
        pythonType = butlerLocation.getPythonType()
        if pythonType is not None:
            if isinstance(pythonType, basestring):
                # import this pythonType dynamically
                pythonTypeTokenList = pythonType.split('.')
                importClassString = pythonTypeTokenList.pop()
                importClassString = importClassString.strip()
                importPackage = ".".join(pythonTypeTokenList)
                importType = __import__(importPackage, globals(), locals(), [importClassString], 0)
                pythonType = getattr(importType, importClassString)

        # see note re. discomfort with the name 'butlerWrite' in the write method, above.
        # Same applies to butlerRead.
        if hasattr(pythonType, 'butlerRead'):
            results = pythonType.butlerRead(butlerLocation=butlerLocation)
            return results

        for locationString in locations:
            locationString = os.path.join(self.root, locationString)

            logLoc = LogicalLocation(locationString, additionalData)

            if storageName == "PafStorage":
                finalItem = pexPolicy.Policy.createPolicy(logLoc.locString())
            elif storageName == "YamlStorage":
                finalItem = Policy(filePath=logLoc.locString())
            elif storageName == "PickleStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError("No such pickle file: " + logLoc.locString())
                with open(logLoc.locString(), "rb") as infile:
                    # py3: We have to specify encoding since some files were written
                    # by python2, and 'latin1' manages that conversion safely. See:
                    # http://stackoverflow.com/questions/28218466/unpickling-a-python-2-object-with-python-3/28218598#28218598
                    if sys.version_info.major >= 3:
                        finalItem = pickle.load(infile, encoding="latin1")
                    else:
                        finalItem = pickle.load(infile)
            elif storageName == "FitsCatalogStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError("No such FITS catalog file: " + logLoc.locString())
                hdu = additionalData.getInt("hdu", 0)
                flags = additionalData.getInt("flags", 0)
                finalItem = pythonType.readFits(logLoc.locString(), hdu, flags)
            elif storageName == "ConfigStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError("No such config file: " + logLoc.locString())
                finalItem = pythonType()
                finalItem.load(logLoc.locString())
            else:
                storageList = StorageList()
                storage = self.persistence.getRetrieveStorage(storageName, logLoc)
                storageList.append(storage)
                itemData = self.persistence.unsafeRetrieve(
                    butlerLocation.getCppType(), storageList, additionalData)
                finalItem = pythonType.swigConvert(itemData)
            results.append(finalItem)

        return results

    def butlerLocationExists(self, location):
        """Implementaion of PosixStorage.exists for ButlerLocation objects."""
        storageName = location.getStorageName()
        if storageName not in ('BoostStorage', 'FitsStorage', 'PafStorage',
                               'PickleStorage', 'ConfigStorage', 'FitsCatalogStorage'):
            self.log.warn("butlerLocationExists for non-supported storage %s" % location)
            return False
        for locationString in location.getLocations():
            logLoc = LogicalLocation(locationString, location.getAdditionalData()).locString()
            obj = self.instanceSearch(path=logLoc)
            if obj:
                return True
        return False

    def exists(self, location):
        """Check if location exists.

        Parameters
        ----------
        location : ButlerLocation or string
            A a string or a ButlerLocation that describes the location of an object in this storage.

        Returns
        -------
        bool
            True if exists, else False.
        """
        if isinstance(location, ButlerLocation):
            return self.butlerLocationExists(location)

        obj = self.instanceSearch(path=location)
        return bool(obj)

    def locationWithRoot(self, location):
        """Get the full path to the location.

        :param location:
        :return:
        """
        return os.path.join(self.root, location)

    def lookup(self, *args, **kwargs):
        """Perform a lookup in the registry"""
        return self.registry.lookup(*args, **kwargs)

    @staticmethod
    def v1RepoExists(root):
        """Test if a Version 1 Repository exists.

        Version 1 Repositories only exist in posix storages and do not have a RepositoryCfg file.
        To "exist" the folder at root must exist and contain files or folders.

        Parameters
        ----------
        root : string
            A path to a folder on the local filesystem.

        Returns
        -------
        bool
            True if the repository at root exists, else False.
        """
        return os.path.exists(root) and bool(os.listdir(root))


    ####################################
    # PosixStorage-only API (for now...)

    def copyFile(self, fromLocation, toLocation):
        shutil.copy(os.path.join(self.root, fromLocation), os.path.join(self.root, toLocation))

    def getLocalFile(self, path):
        """Get the path to a local copy of the file, downloading it to a temporary if needed.

        Parameters
        ----------
        A path the the file in storage, relative to root.

        Returns
        -------
        A path to a local copy of the file. May be the original file (if storage is local)."""
        p = os.path.join(self.root, path)
        if os.path.exists(p):
            return p
        else:
            return None

    def instanceSearch(self, path):
        """Search for the given path in this storage instance.

        If the path contains an HDU indicator (a number in brackets before the
        dot, e.g. 'foo.fits[1]', this will be stripped when searching and so
        will match filenames without the HDU indicator, e.g. 'foo.fits'. The
        path returned WILL contain the indicator though, e.g. ['foo.fits[1]'].

        Parameters
        ----------
        path : string
            A filename (and optionally prefix path) to search for within root.

        Returns
        -------
        string or None
            The location that was found, or None if no location was found.
        """
        return self.search(self.root, path)

    @staticmethod
    def search(root, path, searchParents=False):
        """Look for the given path in the current root.

        Also supports searching for the path in Butler v1 repositories by
        following the Butler v1 _parent symlink

        If the path contains an HDU indicator (a number in brackets, e.g.
        'foo.fits[1]', this will be stripped when searching and so
        will match filenames without the HDU indicator, e.g. 'foo.fits'. The
        path returned WILL contain the indicator though, e.g. ['foo.fits[1]'].

        Parameters
        ----------
        root : string
            The path to the root directory.
        path : string
            The path to the file within the root directory.
        searchParents : bool, optional
            For Butler v1 repositories only, if true and a _parent symlink
            exists, then the directory at _parent will be searched if the file
            is not found in the root repository. Will continue searching the
            parent of the parent until the file is found or no additional
            parent exists.
        """
        # Separate path into a root-equivalent prefix (in dir) and the rest
        # (left in path)
        rootDir = root
        # First remove trailing slashes (#2527)
        while len(rootDir) > 1 and rootDir[-1] == '/':
            rootDir = rootDir[:-1]

        if path.startswith(rootDir + "/"):
            # Common case; we have the same root prefix string
            path = path[len(rootDir + '/'):]
            pathPrefix = rootDir
        elif rootDir == "/" and path.startswith("/"):
            path = path[1:]
            pathPrefix = None
        else:
            # Search for prefix that is the same as root
            pathPrefix = os.path.dirname(path)
            while pathPrefix != "" and pathPrefix != "/":
                if os.path.realpath(pathPrefix) == os.path.realpath(root):
                    break
                pathPrefix = os.path.dirname(pathPrefix)
            if pathPrefix == "/":
                path = path[1:]
            elif pathPrefix != "":
                path = path[len(pathPrefix)+1:]

        # Now search for the path in the root or its parents
        # Strip off any cfitsio bracketed extension if present
        strippedPath = path
        pathStripped = None
        firstBracket = path.find("[")
        if firstBracket != -1:
            strippedPath = path[:firstBracket]
            pathStripped = path[firstBracket:]

        dir = rootDir
        while True:
            paths = glob.glob(os.path.join(dir, strippedPath))
            if len(paths) > 0:
                if pathPrefix != rootDir:
                    paths = [p[len(rootDir+'/'):] for p in paths]
                if pathStripped is not None:
                    paths = [p + pathStripped for p in paths]
                return paths
            if searchParents:
                dir = os.path.join(dir, "_parent")
                if not os.path.exists(dir):
                    return None
            else:
                return None

    @staticmethod
    def prepOutputRootRepos(outputRoot, root):
        # Path manipulations are subject to race condition
        if not os.path.exists(outputRoot):
            try:
                os.makedirs(outputRoot)
            except OSError as e:
                if not e.errno == errno.EEXIST:
                    raise
            if not os.path.exists(outputRoot):
                raise RuntimeError("Unable to create output repository '%s'" % (outputRoot,))
        if os.path.exists(root):
            # Symlink existing input root to "_parent" in outputRoot.
            src = os.path.abspath(root)
            dst = os.path.join(outputRoot, "_parent")
            if not os.path.exists(dst):
                try:
                    os.symlink(src, dst)
                except OSError:
                    pass
            if os.path.exists(dst):
                if os.path.realpath(dst) != os.path.realpath(src):
                    raise RuntimeError("Output repository path "
                                       "'%s' already exists and differs from "
                                       "input repository path '%s'" % (dst, src))
            else:
                raise RuntimeError("Unable to symlink from input "
                                   "repository path '%s' to output repository "
                                   "path '%s'" % (src, dst))
        # We now use the outputRoot as the main root with access to the
        # input via "_parent".


Storage.registerStorageClass(scheme='', cls=PosixStorage)
Storage.registerStorageClass(scheme='file', cls=PosixStorage)
