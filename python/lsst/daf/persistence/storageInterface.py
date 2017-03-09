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
from abc import ABCMeta, abstractmethod


class StorageInterface:
    """Defines the interface for a connection to a Storage location.

    Init will create the storage location if it does not exist.

    Parameters
    ----------
    uri : string
        URI or path that is used as the storage location.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, uri):
        """initialzer"""

    @abstractmethod
    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by ButlerLocation

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object to be written.
        obj : object instance
            The object to be written.
        """

    @abstractmethod
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

    @abstractmethod
    def getLocalFile(self, path):
        """Get the path to a local copy of the file, downloading it to a
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

    @abstractmethod
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

    @abstractmethod
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

    @staticmethod
    @abstractmethod
    def search(root, path):
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

        Returns
        -------
        string or None
            The location that was found, or None if no location was found.
        """

    @abstractmethod
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

    @abstractmethod
    def locationWithRoot(self, location):
        """Get the full path to the location.

        :param location:
        :return:
        """

    @staticmethod
    @abstractmethod
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

    @staticmethod
    @abstractmethod
    def putRepositoryCfg(cfg, loc=None):
        """Serialize a RepositoryCfg to a location.

        When loc == cfg.root, the RepositoryCfg is to be writtenat the root
        location of the repository. In that case, root is not written, it is
        implicit in the location of the cfg. This allows the cfg to move from
        machine to machine without modification.

        Parameters
        ----------
        cfg : RepositoryCfg instance
            The RepositoryCfg to be serailized.
        loc : None, optional
            The location to write the RepositoryCfg. If loc is None, the
            location will be read from the root parameter of loc.

        Returns
        -------
        None
        """

    @staticmethod
    @abstractmethod
    def getMapperClass(root):
        """Get the mapper class associated with a repository root.

        Parameters
        ----------
        root : string
            The location of a persisted ReositoryCfg is (new style repos).

        Returns
        -------
        A class object or a class instance, depending on the state of the
        mapper when the repository was created.
        """

    # Optional: Only needs to work if relative paths are sensical on this
    # storage type and for the case where fromPath and toPath are of the same
    # storage type.
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
        return toPath

    # Optional: Only needs to work if relative paths and absolute paths are
    # sensical on this storage type and for the case where fromPath and toPath
    # are of the same storage type.
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
        return relativePath
