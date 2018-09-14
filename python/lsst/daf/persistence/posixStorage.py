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
from past.builtins import basestring
import sys
import pickle
import importlib
import os
import re
import urllib.parse
import glob
import shutil
import yaml

from . import (LogicalLocation, Policy,
               StorageInterface, Storage, ButlerLocation,
               NoRepositroyAtRoot, RepositoryCfg, doImport)
from lsst.log import Log
from .safeFileIo import SafeFilename, safeMakeDir


__all__ = ["PosixStorage"]


class PosixStorage(StorageInterface):
    """Defines the interface for a storage location on the local filesystem.

    Parameters
    ----------
    uri : string
        URI or path that is used as the storage location.
    create : bool
        If True a new repository will be created at the root location if it
        does not exist. If False then a new repository will not be created.

    Raises
    ------
    NoRepositroyAtRoot
        If create is False and a repository does not exist at the root
        specified by uri then NoRepositroyAtRoot is raised.
    """

    def __init__(self, uri, create):
        self.log = Log.getLogger("daf.persistence.butler")
        self.root = self._pathFromURI(uri)
        if self.root and not os.path.exists(self.root):
            if not create:
                raise NoRepositroyAtRoot("No repository at {}".format(uri))
            safeMakeDir(self.root)

    def __repr__(self):
        return 'PosixStorage(root=%s)' % self.root

    @staticmethod
    def _pathFromURI(uri):
        """Get the path part of the URI"""
        return urllib.parse.urlparse(uri).path

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
        fromPath = os.path.realpath(fromPath)
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
        fromPath = os.path.realpath(fromPath)
        return os.path.normpath(os.path.join(fromPath, relativePath))

    @staticmethod
    def getRepositoryCfg(uri):
        """Get a persisted RepositoryCfg

        Parameters
        ----------
        uri : URI or path to a RepositoryCfg
            Description

        Returns
        -------
        A RepositoryCfg instance or None
        """
        storage = Storage.makeFromURI(uri)
        location = ButlerLocation(pythonType=RepositoryCfg,
                                  cppType=None,
                                  storageName=None,
                                  locationList='repositoryCfg.yaml',
                                  dataId={},
                                  mapper=None,
                                  storage=storage,
                                  usedDataId=None,
                                  datasetType=None)
        return storage.read(location)

    @staticmethod
    def putRepositoryCfg(cfg, loc=None):
        storage = Storage.makeFromURI(cfg.root if loc is None else loc, create=True)
        location = ButlerLocation(pythonType=RepositoryCfg,
                                  cppType=None,
                                  storageName=None,
                                  locationList='repositoryCfg.yaml',
                                  dataId={},
                                  mapper=None,
                                  storage=storage,
                                  usedDataId=None,
                                  datasetType=None)
        storage.write(location, cfg)

    @staticmethod
    def getMapperClass(root):
        """Get the mapper class associated with a repository root.

        Supports the legacy _parent symlink search (which was only ever posix-only. This should not be used by
        new code and repositories; they should use the Repository parentCfg mechanism.

        Parameters
        ----------
        root : string
            The location of a persisted ReositoryCfg is (new style repos), or
            the location where a _mapper file is (old style repos).

        Returns
        -------
        A class object or a class instance, depending on the state of the
        mapper when the repository was created.
        """
        if not (root):
            return None

        cfg = PosixStorage.getRepositoryCfg(root)
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

    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by
        ButlerLocation

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object to be written.
        obj : object instance
            The object to be written.
        """
        self.log.debug("Put location=%s obj=%s", butlerLocation, obj)

        writeFormatter = self.getWriteFormatter(butlerLocation.getStorageName())
        if not writeFormatter:
            writeFormatter = self.getWriteFormatter(butlerLocation.getPythonType())
        if writeFormatter:
            writeFormatter(butlerLocation, obj)
            return

        raise(RuntimeError("No formatter for location:{}".format(butlerLocation)))

    def read(self, butlerLocation):
        """Read from a butlerLocation.

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object(s) to be read.

        Returns
        -------
        A list of objects as described by the butler location. One item for
        each location in butlerLocation.getLocations()
        """
        readFormatter = self.getReadFormatter(butlerLocation.getStorageName())
        if not readFormatter:
            readFormatter = self.getReadFormatter(butlerLocation.getPythonType())
        if readFormatter:
            return readFormatter(butlerLocation)

        raise(RuntimeError("No formatter for location:{}".format(butlerLocation)))

    def butlerLocationExists(self, location):
        """Implementation of PosixStorage.exists for ButlerLocation objects.
        """
        storageName = location.getStorageName()
        if storageName not in ('FitsStorage',
                               'PickleStorage', 'ConfigStorage', 'FitsCatalogStorage',
                               'YamlStorage', 'ParquetStorage', 'MatplotlibStorage'):
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
            A a string or a ButlerLocation that describes the location of an
            object in this storage.

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

    @staticmethod
    def v1RepoExists(root):
        """Test if a Version 1 Repository exists.

        Version 1 Repositories only exist in posix storages, do not have a
        RepositoryCfg file, and contain either a registry.sqlite3 file, a
        _mapper file, or a _parent link.

        Parameters
        ----------
        root : string
            A path to a folder on the local filesystem.

        Returns
        -------
        bool
            True if the repository at root exists, else False.
        """
        return os.path.exists(root) and (
            os.path.exists(os.path.join(root, "registry.sqlite3")) or
            os.path.exists(os.path.join(root, "_mapper")) or
            os.path.exists(os.path.join(root, "_parent"))
        )

    def copyFile(self, fromLocation, toLocation):
        """Copy a file from one location to another on the local filesystem.

        Parameters
        ----------
        fromLocation : path
            Path and name of existing file.
         toLocation : path
            Path and name of new file.

        Returns
        -------
        None
        """
        shutil.copy(os.path.join(self.root, fromLocation), os.path.join(self.root, toLocation))

    def getLocalFile(self, path):
        """Get a handle to a local copy of the file, downloading it to a
        temporary if needed.

        Parameters
        ----------
        A path the the file in storage, relative to root.

        Returns
        -------
        A handle to a local copy of the file. If storage is remote it will be
        a temporary file. If storage is local it may be the original file or
        a temporary file. The file name can be gotten via the 'name' property
        of the returned object.
        """
        p = os.path.join(self.root, path)
        try:
            return open(p)
        except IOError as e:
            if e.errno == 2:  # 'No such file or directory'
                return None
            else:
                raise e

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

        Returns
        -------
        string or None
            The location that was found, or None if no location was found.
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
    def storageExists(uri):
        """Ask if a storage at the location described by uri exists

        Parameters
        ----------
        root : string
            URI to the the root location of the storage

        Returns
        -------
        bool
            True if the storage exists, false if not
        """
        return os.path.exists(PosixStorage._pathFromURI(uri))


def readConfigStorage(butlerLocation):
    """Read an lsst.pex.config.Config from a butlerLocation.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object(s) to be read.

    Returns
    -------
    A list of objects as described by the butler location. One item for
    each location in butlerLocation.getLocations()
    """
    results = []
    for locationString in butlerLocation.getLocations():
        locStringWithRoot = os.path.join(butlerLocation.getStorage().root, locationString)
        logLoc = LogicalLocation(locStringWithRoot, butlerLocation.getAdditionalData())
        if not os.path.exists(logLoc.locString()):
            raise RuntimeError("No such config file: " + logLoc.locString())
        pythonType = butlerLocation.getPythonType()
        if pythonType is not None:
            if isinstance(pythonType, basestring):
                pythonType = doImport(pythonType)
        finalItem = pythonType()
        finalItem.load(logLoc.locString())
        results.append(finalItem)
    return results


def writeConfigStorage(butlerLocation, obj):
    """Writes an lsst.pex.config.Config  object to a location specified by
    ButlerLocation.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object to be written.
    obj : object instance
        The object to be written.
    """
    filename = os.path.join(butlerLocation.getStorage().root, butlerLocation.getLocations()[0])
    with SafeFilename(filename) as locationString:
        logLoc = LogicalLocation(locationString, butlerLocation.getAdditionalData())
        obj.save(logLoc.locString())


def readFitsStorage(butlerLocation):
    """Read objects from a FITS file specified by ButlerLocation.

    The object is read using class or static method
    ``readFitsWithOptions(path, options)``, if it exists, else
    ``readFits(path)``. The ``options`` argument is the data returned by
    ``butlerLocation.getAdditionalData()``.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object(s) to be read.

    Returns
    -------
    A list of objects as described by the butler location. One item for
    each location in butlerLocation.getLocations()
    """
    pythonType = butlerLocation.getPythonType()
    if pythonType is not None:
        if isinstance(pythonType, basestring):
            pythonType = doImport(pythonType)
    supportsOptions = hasattr(pythonType, "readFitsWithOptions")
    results = []
    additionalData = butlerLocation.getAdditionalData()
    for locationString in butlerLocation.getLocations():
        locStringWithRoot = os.path.join(butlerLocation.getStorage().root, locationString)
        logLoc = LogicalLocation(locStringWithRoot, additionalData)
        # test for existence of file, ignoring trailing [...]
        # because that can specify the HDU or other information
        filePath = re.sub(r"(\.fits(.[a-zA-Z0-9]+)?)(\[.+\])$", r"\1", logLoc.locString())
        if not os.path.exists(filePath):
            raise RuntimeError("No such FITS file: " + logLoc.locString())
        if supportsOptions:
            finalItem = pythonType.readFitsWithOptions(logLoc.locString(), options=additionalData)
        else:
            finalItem = pythonType.readFits(logLoc.locString())
        results.append(finalItem)
    return results


def writeFitsStorage(butlerLocation, obj):
    """Writes an object to a FITS file specified by ButlerLocation.

    The object is written using method
    ``writeFitsWithOptions(path, options)``, if it exists, else
    ``writeFits(path)``. The ``options`` argument is the data returned by
    ``butlerLocation.getAdditionalData()``.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object to be written.
    obj : object instance
        The object to be written.
    """
    supportsOptions = hasattr(obj, "writeFitsWithOptions")
    additionalData = butlerLocation.getAdditionalData()
    locations = butlerLocation.getLocations()
    with SafeFilename(os.path.join(butlerLocation.getStorage().root, locations[0])) as locationString:
        logLoc = LogicalLocation(locationString, additionalData)
        if supportsOptions:
            obj.writeFitsWithOptions(logLoc.locString(), options=additionalData)
        else:
            obj.writeFits(logLoc.locString())


def readParquetStorage(butlerLocation):
    """Read a catalog from a Parquet file specified by ButlerLocation.

    The object returned by this is expected to be a subtype
    of `ParquetTable`, which is a thin wrapper to `pyarrow.ParquetFile`
    that allows for lazy loading of the data.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object(s) to be read.

    Returns
    -------
    A list of objects as described by the butler location. One item for
    each location in butlerLocation.getLocations()
    """
    results = []
    additionalData = butlerLocation.getAdditionalData()

    for locationString in butlerLocation.getLocations():
        locStringWithRoot = os.path.join(butlerLocation.getStorage().root, locationString)
        logLoc = LogicalLocation(locStringWithRoot, additionalData)
        if not os.path.exists(logLoc.locString()):
            raise RuntimeError("No such parquet file: " + logLoc.locString())

        pythonType = butlerLocation.getPythonType()
        if pythonType is not None:
            if isinstance(pythonType, basestring):
                pythonType = doImport(pythonType)

        filename = logLoc.locString()

        # pythonType will be ParquetTable (or perhaps MultilevelParquetTable)
        #  filename should be the first kwarg, but being explicit here.
        results.append(pythonType(filename=filename))

    return results


def writeParquetStorage(butlerLocation, obj):
    """Writes pandas dataframe to parquet file.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object(s) to be read.
    obj : `lsst.qa.explorer.parquetTable.ParquetTable`
        Wrapped DataFrame to write.

    """
    additionalData = butlerLocation.getAdditionalData()
    locations = butlerLocation.getLocations()
    with SafeFilename(os.path.join(butlerLocation.getStorage().root, locations[0])) as locationString:
        logLoc = LogicalLocation(locationString, additionalData)
        filename = logLoc.locString()
        obj.write(filename)


def writeYamlStorage(butlerLocation, obj):
    """Writes an object to a YAML file specified by ButlerLocation.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object to be written.
    obj : object instance
        The object to be written.
    """
    additionalData = butlerLocation.getAdditionalData()
    locations = butlerLocation.getLocations()
    with SafeFilename(os.path.join(butlerLocation.getStorage().root, locations[0])) as locationString:
        logLoc = LogicalLocation(locationString, additionalData)
        with open(logLoc.locString(), "w") as outfile:
            yaml.dump(obj, outfile)


def readPickleStorage(butlerLocation):
    """Read an object from a pickle file specified by ButlerLocation.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object(s) to be read.

    Returns
    -------
    A list of objects as described by the butler location. One item for
    each location in butlerLocation.getLocations()
    """
    # Create a list of Storages for the item.
    results = []
    additionalData = butlerLocation.getAdditionalData()
    for locationString in butlerLocation.getLocations():
        locStringWithRoot = os.path.join(butlerLocation.getStorage().root, locationString)
        logLoc = LogicalLocation(locStringWithRoot, additionalData)
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
        results.append(finalItem)
    return results


def writePickleStorage(butlerLocation, obj):
    """Writes an object to a pickle file specified by ButlerLocation.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object to be written.
    obj : object instance
        The object to be written.
    """
    additionalData = butlerLocation.getAdditionalData()
    locations = butlerLocation.getLocations()
    with SafeFilename(os.path.join(butlerLocation.getStorage().root, locations[0])) as locationString:
        logLoc = LogicalLocation(locationString, additionalData)
        with open(logLoc.locString(), "wb") as outfile:
            pickle.dump(obj, outfile, pickle.HIGHEST_PROTOCOL)


def readFitsCatalogStorage(butlerLocation):
    """Read a catalog from a FITS table specified by ButlerLocation.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object(s) to be read.

    Returns
    -------
    A list of objects as described by the butler location. One item for
    each location in butlerLocation.getLocations()
    """
    pythonType = butlerLocation.getPythonType()
    if pythonType is not None:
        if isinstance(pythonType, basestring):
            pythonType = doImport(pythonType)
    results = []
    additionalData = butlerLocation.getAdditionalData()
    for locationString in butlerLocation.getLocations():
        locStringWithRoot = os.path.join(butlerLocation.getStorage().root, locationString)
        logLoc = LogicalLocation(locStringWithRoot, additionalData)
        if not os.path.exists(logLoc.locString()):
            raise RuntimeError("No such FITS catalog file: " + logLoc.locString())
        kwds = {}
        if additionalData.exists("hdu"):
            kwds["hdu"] = additionalData.getInt("hdu")
        if additionalData.exists("flags"):
            kwds["flags"] = additionalData.getInt("flags")
        finalItem = pythonType.readFits(logLoc.locString(), **kwds)
        results.append(finalItem)
    return results


def writeFitsCatalogStorage(butlerLocation, obj):
    """Writes a catalog to a FITS table specified by ButlerLocation.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object to be written.
    obj : object instance
        The object to be written.
    """
    additionalData = butlerLocation.getAdditionalData()
    locations = butlerLocation.getLocations()
    with SafeFilename(os.path.join(butlerLocation.getStorage().root, locations[0])) as locationString:
        logLoc = LogicalLocation(locationString, additionalData)
        if additionalData.exists("flags"):
            kwds = dict(flags=additionalData.getInt("flags"))
        else:
            kwds = {}
        obj.writeFits(logLoc.locString(), **kwds)


def readMatplotlibStorage(butlerLocation):
    """Read from a butlerLocation (always fails for this storage type).

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object(s) to be read.

    Returns
    -------
    A list of objects as described by the butler location. One item for
    each location in butlerLocation.getLocations()
    """
    raise NotImplementedError("Figures saved with MatplotlibStorage cannot be retreived using the Butler.")


def writeMatplotlibStorage(butlerLocation, obj):
    """Writes a matplotlib.figure.Figure to a location, using the template's
    filename suffix to infer the file format.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object to be written.
    obj : matplotlib.figure.Figure
        The object to be written.
    """
    additionalData = butlerLocation.getAdditionalData()
    locations = butlerLocation.getLocations()
    with SafeFilename(os.path.join(butlerLocation.getStorage().root, locations[0])) as locationString:
        logLoc = LogicalLocation(locationString, additionalData)
        # SafeFilename appends a random suffix, which corrupts the extension
        # matplotlib uses to guess the file format.
        # Instead, we extract the extension from the original location
        # and pass that as the format directly.
        _, ext = os.path.splitext(locations[0])
        if ext:
            ext = ext[1:]  # strip off leading '.'
        else:
            # If there is no extension, we let matplotlib fall back to its
            # default.
            ext = None
        obj.savefig(logLoc.locString(), format=ext)


def readYamlStorage(butlerLocation):
    """Read an object from a YAML file specified by a butlerLocation.

    Parameters
    ----------
    butlerLocation : ButlerLocation
        The location for the object(s) to be read.

    Returns
    -------
    A list of objects as described by the butler location. One item for
    each location in butlerLocation.getLocations()
    """
    results = []
    for locationString in butlerLocation.getLocations():
        logLoc = LogicalLocation(butlerLocation.getStorage().locationWithRoot(locationString),
                                 butlerLocation.getAdditionalData())
        if not os.path.exists(logLoc.locString()):
            raise RuntimeError("No such YAML file: " + logLoc.locString())
        # Butler Gen2 repository configurations are handled specially
        if butlerLocation.pythonType == 'lsst.daf.persistence.RepositoryCfg':
            finalItem = Policy(filePath=logLoc.locString())
        else:
            with open(logLoc.locString(), "rb") as infile:
                finalItem = yaml.load(infile)
        results.append(finalItem)
    return results


PosixStorage.registerFormatters("FitsStorage", readFitsStorage, writeFitsStorage)
PosixStorage.registerFormatters("ParquetStorage", readParquetStorage, writeParquetStorage)
PosixStorage.registerFormatters("ConfigStorage", readConfigStorage, writeConfigStorage)
PosixStorage.registerFormatters("PickleStorage", readPickleStorage, writePickleStorage)
PosixStorage.registerFormatters("FitsCatalogStorage", readFitsCatalogStorage, writeFitsCatalogStorage)
PosixStorage.registerFormatters("MatplotlibStorage", readMatplotlibStorage, writeMatplotlibStorage)
PosixStorage.registerFormatters("YamlStorage", readYamlStorage, writeYamlStorage)

Storage.registerStorageClass(scheme='', cls=PosixStorage)
Storage.registerStorageClass(scheme='file', cls=PosixStorage)
