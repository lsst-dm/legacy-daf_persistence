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
from future import standard_library
standard_library.install_aliases()

import copy
import os
import swiftclient as swift
import tempfile
import time
import yaml

from . import Storage, StorageInterface, PosixStorage, ButlerLocation
from lsst.log import Log


class SwiftStorage(StorageInterface):
    """Storage Interface implementation specific for Swift

    Requires that the following environment variables exist:
    OS_USERNAME : string
        The username to use when authorizing the connection.
    OS_PASSWORD : string
        The password to use when authorizing the connection.

    Parameters
    ----------
    uri : string
        A URI to connect to a swift storage location. The form of the URI is
        `swift://[URL without 'http://']/[Object API Version]/[tenant name (account)]/[container]`
        For example:
        `swift://nebula.ncsa.illinois.edu:5000/v2.0/lsst/my_container`

    Downloads blobs from storage to a file, and uses PosixStorage to load that
    file into an object. Handles to files are cached (this is effectively
    necessary for e.g. iterating over fits headers) by location in
    self.fileCache. If it turns out that this allows the temporary file
    directory to grow too large it may work to modify this to keep only the
    most recently accessed file, which would still support iterating over a
    single file's fits headers.
    """
    def __init__(self, uri):
        self._log = Log.getLogger("daf.persistence.butler")
        self._uri = uri
        scheme, \
            self._url, \
            self._version, \
            self._tenantName, \
            self._containerName = self._parseURI(uri)
        self._url = "http://" + os.path.join(self._url, self._version)
        self._connection = self._getConnection()

        self.fileCache = {}  # (location, file handle)

        # Creating a new container is an idempotent operation: if the container
        # already exists it is a no-operation.
        try:
            self._connection.put_container(self._containerName)
        except swift.ClientException:
            raise RuntimeError("Connection to {} tenant '{}' failed.".format(
                self._url, self._tenantName))

    @staticmethod
    def _parseURI(uri):
        """Parse the URI into paramters expected for the SwiftStorage URI.

        Parameters
        ----------
        uri : string
            URI with form described by the init function documentation.

        Returns
        -------
        tuple of strings
            (scheme, url, object api version, tenant name, container name)

        Raises
        ------
        RuntimeError
            If the URI does not start with 'swift://'
        ValueError
            If the URI after scheme does not have the correct number of fields.
        """
        if not uri.startswith("swift://"):
            raise RuntimeError(
                "Swift URI must start with 'swift://' {} will not work".format(
                    uri))
        scheme = "swift"
        uri = uri[8:]
        url, apiVersion, tenantName, containerName = uri.split('/')
        return (scheme, url, apiVersion, tenantName, containerName)

    def _getConnection(self):
        """Get a connection to a swift container.

        Gets the username and password to access the container from the
        environment variables OS_USERNAME and OS_PASSWORD.

        Gets the authorization url and the tenant name from the URI passed to
        __init__.

        Assumes auth version 2.

        Returns
        -------
        swift.client.Connection
            The object representing the connection. The object may or may not
            actually be connected.
        """
        user = os.getenv('OS_USERNAME')
        if user is None:
            raise RuntimeError(
                'SwiftStorage could not find OS_USERNAME in environment')
        key = os.getenv('OS_PASSWORD')
        if key is None:
            raise RuntimeError(
                'SwiftStorage could not find OS_PASSWORD in environment')

        # TODO are auth_version and self._version (from the input URI, usually
        # 'v2.0') supposed to correlate? How?

        return swift.Connection(authurl=self._url, user=user, key=key,
                                tenant_name=self._tenantName,
                                auth_version=2)

    def containerExists(self):
        """Query if the container for this SwiftStorage exists."""
        try:
            self._connection.head_container(self._containerName)
        except swift.ClientException:
            return False
        return True

    def deleteContainer(self):
        """Delete all the objects in this container"""
        respHeaders, containers = self._connection.get_account()
        for container in containers:
            name = container['name']
            if name == self._containerName:
                headers, objects = self._connection.get_container(
                    self._containerName)
                for obj in objects:
                    self._connection.delete_object(self._containerName, obj['name'])
                self._connection.delete_container(self._containerName)
                break

    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by
        ButlerLocation

        This file uses PosixStorage to write the object to a file on disk (that
        is to say: serialize it). Then the file is uploaded to the swift
        container. When we have better support for pluggable serializers,
        hopefully the first step of writing to disk can be skipped and the
        object can be serialzied and streamed directly to the swift container.

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object to be written.
        obj : object instance
            The object to be written.
        """
        swiftLocations = butlerLocation.getLocations()
        # Here the ButlerLocation is modified sligtly to write to a temporary
        # file via PosixStorage. (Then the temporary file is written to the
        # swift container)
        localFile = tempfile.NamedTemporaryFile()
        butlerLocation.locationList = [localFile.name]
        butlerLocation.storage = PosixStorage('/')
        butlerLocation.storage.write(butlerLocation, obj)
        butlerLocation.locationList = swiftLocations
        with open(localFile.name, 'rb') as f:
            self._connection.put_object(
                container=self._containerName, obj=swiftLocations[0],
                contents=f, content_length=os.stat(localFile.name).st_size)

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
        localFile = self.getLocalFile(butlerLocation.getLocations()[0])
        butlerLocation.locationList = [localFile.name]
        butlerLocation.storage = PosixStorage('/')
        obj = butlerLocation.storage.read(butlerLocation)
        return obj

    def _getLocalFile(self, location):
        """Implementation of getLocalFile that does not wrap the swift
        ClientException so that this function may be used by this class in
        various functions that want to handle the exception directly.

        Parameters
        ----------
        location : string
            A location of the the file in storage, relative to the storage's
            root.

        Raises
        ------
        swift.ClientException
            Indicates an error downloading the object.
        """
        location = self.getFitsHeaderStrippedPath(location)[0]
        localFile = self.fileCache.get(location, None)
        if not localFile:
            localFile = tempfile.NamedTemporaryFile(
                suffix=os.path.splitext(location)[1])
            headers, contents = self._connection.get_object(self._containerName,
                                                            location)
            localFile.write(contents)
            localFile.flush()
            self.fileCache[location] = localFile
        localFile.seek(0)
        return localFile

    def getLocalFile(self, location):
        """TODO this is changed to return a handle to a local temproary file.
        If we keep it there's a few places that will have to be modified.

        As it is expected the local file will be read (not written to), the
        current position in the file is set to 0 before it is returned.

        The local file will be deleted when the file object is deleted, so this
        function does not close the file unless an error is raised internally.
        Callers may close or explicitly delete the object when they are done
        with it or may allow it to be garbage collected.

        Parameters
        ----------
        location : string
            A location of the the file in storage, relative to the storage's
            root.

        Returns
        -------
        A handle to a local copy of the file. If storage is remote it will be
        a temporary file. If storage is local it may be the original file or
        a temporary file. The file name can be gotten via the 'name' property
        of the returned object.

        Raises
        ------
        RuntimeError
            If there is an error downloading the object.
        """
        self._log.info("SwiftStorage getting file {}".format(location))
        start = time.time()
        try:
            f = self._getLocalFile(location)
            self._log.info(
                "...getting file took {} seconds.".format(time.time() - start))
            return f
        except swift.ClientException:
            raise RuntimeError(
                "Could not download object '{0}/{1}' not found.".format(
                    self._containerName, location))

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
            location = location.getLocations()[0]
        return bool(self.instanceSearch(location))

    @staticmethod
    def getFitsHeaderStrippedPath(path):
        """Get the path with the optional FITS header selector stripped off.

        Parameters
        ----------
        path : string
            A file path that may end with [n]

        Returns
        -------
        (string, string)
            Tuple, the first item is the path without the fits header, the
            second item is the part that was stripped, if any.
        """
        strippedPath = path
        pathStripped = ''
        firstBracket = path.find("[")
        if firstBracket != -1:
            strippedPath = path[:firstBracket]
            pathStripped = path[firstBracket:]
        return strippedPath, pathStripped

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
        # Now search for the path in the root or its parents
        # Strip off any cfitsio bracketed extension if present
        strippedPath, pathStripped = self.getFitsHeaderStrippedPath(path)
        try:
            headers, objects = self._connection.get_container(
                self._containerName)
            locations = [obj['name'] for obj in objects]
        except swift.ClientException as err:
            raise RuntimeError("Container \'{0}\' not found: {1}".format(
                self._containerName, err))
        import fnmatch
        locations = fnmatch.filter(locations, strippedPath)
        locations = [location + pathStripped for location in locations]
        return locations if locations else None

    @staticmethod
    def search(uri, path):
        """Look for the given path in the current root.

        Also supports searching for the path in Butler v1 repositories by
        following the Butler v1 _parent symlink

        If the path contains an HDU indicator (a number in brackets, e.g.
        'foo.fits[1]', this will be stripped when searching and so
        will match filenames without the HDU indicator, e.g. 'foo.fits'. The
        path returned WILL contain the indicator though, e.g. ['foo.fits[1]'].

        Parameters
        ----------
        uri : string
            The uri to the repository.
        path : string
            The path to the file within the root directory.

        Returns
        -------
        string or None
            The location that was found, or None if no location was found.
        """
        storage = SwiftStorage(uri)
        return storage.instanceSearch(path)

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

        Raises
        ------
        RuntimeError
            If the copy fails.
        """
        try:
            self._connection.copy_object(
                self._containerName, fromLocation,
                self._containerName + '/' + toLocation)
        except swift.ClientException as err:
            raise RuntimeError("copyFile error: {}".format(err))

    def locationWithRoot(self, location):
        """Get the full path to the location.

        :param location:
        :return:
        """
        # TODO is this a sensical representation? maybe
        # [location]@[self._uri] makes more sense? Or what?
        return self._uri + '/' + location

    @staticmethod
    def getRepositoryCfg(uri):
        """Get a persisted RepositoryCfg

        This implementation assumes that one container holds exactly one
        repository.

        Parameters
        ----------
        uri : URI or path to a RepositoryCfg
            Description

        Returns
        -------
        A RepositoryCfg instance or None
        """
        storage = SwiftStorage(uri)
        try:
            localFile = storage._getLocalFile('repositoryCfg')
        except swift.ClientException:
            # Assuming the file does not exist, not some other kind of error
            # (how can we do more precise handling of this expcetion?)
            return None
        with open(localFile.name, mode='r') as f:
            repositoryCfg = yaml.load(f)
            if repositoryCfg.root is None:
                repositoryCfg.root = storage._uri
            return repositoryCfg
        return None

    @staticmethod
    def putRepositoryCfg(cfg, loc=None):
        """Serialize a RepositoryCfg to a location.

        When loc == cfg.root, the RepositoryCfg is to be writtenat the root
        location of the repository. In that case, root is not written, it is
        implicit in the location of the cfg. This allows the cfg to move from
        machine to machine without modification.

        This implementation assumes that one container holds exactly one
        repository.

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
        if loc is None or cfg.root == loc:
            # the cfg is at the root location of the repository so don't write
            # root, let it be implicit in the location of the cfg.
            cfg = copy.copy(cfg)
            loc = cfg.root
            cfg.root = None
        storage = SwiftStorage(loc)
        with tempfile.NamedTemporaryFile() as f:
            yaml.dump(cfg, f)
            f.seek(0)
            storage._connection.put_object(
                container=storage._containerName, obj='repositoryCfg',
                contents=f, content_length=os.stat(f.name).st_size)

    @staticmethod
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
        cfg = SwiftStorage.getRepositoryCfg(root)
        if cfg is None:
            return None
        return cfg.mapper

Storage.registerStorageClass(scheme='swift', cls=SwiftStorage)
