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

from . import ButlerLocation, PosixStorage, Storage, StorageInterface

import boto3
import botocore
import copy
import fnmatch
import os
import tempfile
import yaml


class S3Storage(StorageInterface):
    """Storage Interface implementation specific for Swift

    Requires that the following environment variables exist:
    S3_USERNAME : string
        The username to use when authorizing the connection.
    S3_PASSWORD : string
        The password to use when authorizing the connection.

    Parameters
    ----------
    uri : string
        A URI to connect to a swift storage location. The form of the URI is
        `s3://[URL without 'http://']/[bucket]`
        For example:
        `TODO`

    Downloads blobs from storage to a file, and uses PosixStorage to load that
    file into an object. Handles to files are cached (this is effectively
    necessary for e.g. iterating over fits headers) by location in
    self.fileCache. If it turns out that this allows the temporary file
    directory to grow too large it may work to modify this to keep only the
    most recently accessed file, which would still support iterating over a
    single file's fits headers.
    """

    def __init__(self, uri):
        """initialzer"""

        scheme, bucket = self._parseURI(uri)
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)
        status = 'init'
        try:
            self.s3.meta.client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                status = 'not_exist'
        if status == 'not_exist':
            self.s3.create_bucket(Bucket=bucket)
        else:
            raise RuntimeError("Could not connect to s3.")

    @staticmethod
    def _parseURI(uri):
        """Parse the URI into paramters expected for the S3Storage URI.

        Parameters
        ----------
        uri : string
            URI with form described by the init function documentation.

        Returns
        -------
        tuple of strings
            (scheme, bucketName)

        Raises
        ------
        RuntimeError
            If the URI does not start with 's3://'
        ValueError
            If the URI after scheme does not have the correct number of fields.
        """
        expectedScheme = "s3://"
        if not uri.startswith(expectedScheme):
            raise RuntimeError(
                "S3 URI must start with {}, {} will not work".format(
                    expectedScheme, uri))
        scheme = "s3"
        uri = uri[len(expectedScheme):]
        bucket = uri
        return (scheme, bucket)

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
        locations = butlerLocation.getLocations()
        # Here the ButlerLocation is modified sligtly to write to a temporary
        # file via PosixStorage. (Then the temporary file is written to the
        # swift container)
        localFile = tempfile.NamedTemporaryFile()
        butlerLocation.locationList = [localFile.name]
        butlerLocation.storage = PosixStorage('/')
        butlerLocation.storage.write(butlerLocation, obj)
        obj = self.s3.Object(self.bucket, locations[0])
        obj.put(Body=open(localFile.name, 'rb'))

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
            obj = self.s3.Object(self.bucket, location)
            localFile.write(obj.get()['Body'])
            localFile.flush()
            self.fileCache[location] = localFile
        localFile.seek(0)
        return localFile

    def getLocalFile(self, location):
        """As it is expected the local file will be read (not written to), the
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
        f = self._getLocalFile(location)
        return f

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
        try:
            self.s3.Object(self.bucket, location).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise RuntimeError("Error communicating with s3: {}".format(e))
        else:
            exists = True
        return exists

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
        strippedPath, pathStripped = self.getFitsHeaderStrippedPath(path)

        bucket = self.s3.buckets[self.bucket]

        locations = []
        locations.extend(fnmatch.filter((key for key in bucket.objects()),
                                        strippedPath))
        locations = [location + pathStripped for location in locations]
        return locations if locations else None


    @staticmethod
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
        storage = S3Storage(root)
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
        """
        copy_source = {
            'Bucket': self.bucket,
            'Key': fromLocation
        }
        bucket = self.s3.Bucket(self.bucket)
        obj = bucket.Object(toLocation)
        obj.copy(copy_source)

    def locationWithRoot(self, location):
        """Get the full path to the location.

        :param location:
        :return:
        """
        return self._uri + '/' + location

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
        storage = Storage(uri)
        return storage._getLocalFile('repositoryCfg')

    @staticmethod
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
        # TODO probably this could be serialized direclty, skipping the local file.
        if loc is None or cfg.root == loc:
            # the cfg is at the root location of the repository so don't write
            # root, let it be implicit in the location of the cfg.
            cfg = copy.copy(cfg)
            loc = cfg.root
            cfg.root = None
        storage = Storage(loc)
        with tempfile.NamedTemporaryFile() as f:
            yaml.dump(cfg, f)
            f.seek(0)
            obj = storage.s3.Object(storage.bucket, tempfile.name)
            obj.put(Body=open(tempfile.name, 'rb'))

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
        cfg = S3Storage.getRepositoryCfg(root)
        if cfg is None:
            return None
        return cfg.mapper

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
