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
from builtins import object

import copy
import inspect
import os

from lsst.daf.persistence import Storage, listify, doImport, Policy


class RepositoryArgs(object):

    """Arguments passed into a Butler that are used to instantiate a repository. This includes arguments that
    can be used to create a new repository (cfgRoot, root, mapper, mapperArgs, policy) and are persisted along
    with the new repository's configuration file. These areguments can also describe how a new or existing
    repository are to be used (cfgRoot or root, tags, mode). When indicating an existing repository it is
    better to not specify unnecessary arguments, as if they conflict with the persisted repository
    configuration then a RuntimeError will be raised during Butler init.

    A RepositoryArgs class can be initialized from a dict, if the first argument to the initializer is a dict.

    Attributes
    ----------
    cfgRoot : URI or dict
        If dict, the initalizer is re-called with the expanded dict.
        If URI, this is the location where the RepositoryCfg should be found (existing repo) or put (new repo)
    root : URI
        If different than cfgRoot then this is the location where the repository should exist. A RepositoryCfg
        will be put at cfgRoot and its root will be a path to root.
    mapper : string or class object.
        The mapper to use with this repository. If string, should refer an importable object. If class object,
        should be a mapper to be instantiated by the Butler during Butler init.
    tags : list or object
        A list of unique identifiers to uniquely identify this repository and its parents when performing
        Butler.get.
    mode : string
        should be one of 'r', 'w', or 'rw', for 'read', 'write', or 'read-write'. Can be omitted; input
        repositories will default to 'r', output repositories will default to 'r'. 'w' on an input repository
        will raise a RuntimeError during Butler init, tho 'rw' works and is equivalent to 'r'. Output
        repositories may be 'r' or 'rw', 'r' for an output repository will raise a RuntimeError during Butler
        init.
    """
    def __init__(self, cfgRoot=None, root=None, mapper=None, mapperArgs=None, tags=None,
                 mode=None, policy=None):
        try:
            #  is cfgRoot a dict? try dict init:
            self.__init__(**cfgRoot)
        except TypeError:
            self._root = Storage.absolutePath(os.getcwd(), root.rstrip(os.sep)) if root else root
            self._cfgRoot = Storage.absolutePath(os.getcwd(), cfgRoot.rstrip(os.sep)) if cfgRoot else cfgRoot
            self._mapper = mapper
            self.mapperArgs = mapperArgs
            self.tags = set(listify(tags))
            self.mode = mode
            self.policy = Policy(policy) if policy is not None else None

    def __repr__(self):
        return "%s(root=%r, cfgRoot=%r, mapper=%r, mapperArgs=%r, tags=%s, mode=%r, policy=%s)" % (
            self.__class__.__name__, self.root, self._cfgRoot, self._mapper, self.mapperArgs, self.tags,
            self.mode, self.policy)

    @property
    def mapper(self):
        return self._mapper

    @mapper.setter
    def mapper(self, mapper):
        if mapper is not None and self._mapper:
            raise RuntimeError("Explicity clear mapper (set to None) before changing its value.")
        self._mapper = mapper

    @property
    def cfgRoot(self):
        return self._cfgRoot if self._cfgRoot is not None else self._root

    @property
    def root(self):
        return self._root if self._root is not None else self._cfgRoot

    @staticmethod
    def inputRepo(storage, tags=None):
        return RepositoryArgs(storage, tags)

    @staticmethod
    def outputRepo(storage, mapper=None, mapperArgs=None, tags=None, mode=None):
        return RepositoryArgs(storage, mapper, mapperArgs, tags, mode)

    def tag(self, tag):
        """add a tag to the repository cfg"""
        if isinstance(tag, basestring):
            self.tags.add(tag)
        else:
            try:
                self.tags.update(tag)
            except TypeError:
                self.tags.add(tag)


class Repository(object):
    """Represents a repository of persisted data and has methods to access that data.
    """

    def __init__(self, repoData):
        """Initialize a Repository with parameters input via RepoData.

        Parameters
        ----------
        repoData : RepoData
            Object that contains the parameters with which to init the Repository.
        """
        self._storage = Storage.makeFromURI(repoData.cfg.root)
        if repoData.isNewRepository and not repoData.isV1Repository:
            self._storage.putRepositoryCfg(repoData.cfg, repoData.args.cfgRoot)
        self._mapperArgs = repoData.cfg.mapperArgs  # keep for reference in matchesArgs
        self._initMapper(repoData)

    def _initMapper(self, repoData):
        '''Initialize and keep the mapper in a member var.

        Parameters
        ----------
        repoData : RepoData
            The RepoData with the properties of this Repository.
        '''

        # rule: If mapper is:
        # - an object: use it as the mapper.
        # - a string: import it and instantiate it with mapperArgs
        # - a class object: instantiate it with mapperArgs
        mapper = repoData.cfg.mapper

        # if mapper is a string, import it:
        if isinstance(mapper, basestring):
            mapper = doImport(mapper)
        # now if mapper is a class type (not instance), instantiate it:
        if inspect.isclass(mapper):
            mapperArgs = copy.copy(repoData.cfg.mapperArgs)
            if mapperArgs is None:
                mapperArgs = {}
            if 'root' not in mapperArgs:
                mapperArgs['root'] = repoData.cfg.root
            mapper = mapper(parentRegistry=repoData.parentRegistry,
                            repositoryCfg=repoData.cfg,
                            **mapperArgs)
        self._mapper = mapper

        def __repr__(self):
            return 'config(id=%s, storage=%s, parent=%s, mapper=%s, mapperArgs=%s, cls=%s)' % \
                   (self.id, self._storage, self.parent, self._mapper, self.mapperArgs, self.cls)

    # todo want a way to make a repository read-only
    def write(self, butlerLocation, obj):
        """Write a dataset to Storage.

        :param butlerLocation: Contains the details needed to find the desired dataset.
        :param dataset: The dataset to be written.
        :return:
        """
        butlerLocationStorage = butlerLocation.getStorage()
        if butlerLocationStorage:
            return butlerLocationStorage.write(butlerLocation, obj)
        else:
            return self._storage.write(butlerLocation, obj)

    def read(self, butlerLocation):
        """Read a dataset from Storage.

        :param butlerLocation: Contains the details needed to find the desired dataset.
        :return: An instance of the dataset requested by butlerLocation.
        """
        butlerLocationStorage = butlerLocation.getStorage()
        if butlerLocationStorage:
            return butlerLocationStorage.read(butlerLocation)
        else:
            return self._storage.read(butlerLocation)

    #################
    # Mapper Access #

    def mappers(self):
        return (self._mapper, )

    def getRegistry(self):
        """Get the registry from the mapper

        Returns
        -------
        Registry or None
            The registry from the mapper or None if the mapper does not have one.
        """
        if self._mapper is None:
            return None
        return self._mapper.getRegistry()

    def getKeys(self, *args, **kwargs):
        """
        Get the keys available in the repository/repositories.
        :param args:
        :param kwargs:
        :return: A dict of {key:valueType}
        """
        # todo: getKeys is not in the mapper API
        if self._mapper is None:
            return None
        keys = self._mapper.getKeys(*args, **kwargs)
        return keys

    def map(self, *args, **kwargs):
        """Find a butler location for the given arguments.
        See mapper.map for more information about args and kwargs.

        :param args: arguments to be passed on to mapper.map
        :param kwargs: keyword arguments to be passed on to mapper.map
        :return: The type of item is dependent on the mapper being used but is typically a ButlerLocation.
        """
        if self._mapper is None:
            raise RuntimeError("No mapper assigned to Repository")
        loc = self._mapper.map(*args, **kwargs)
        if loc is None:
            return None
        loc.setRepository(self)
        return loc

    def queryMetadata(self, *args, **kwargs):
        """Gets possible values for keys given a partial data id.

        See mapper documentation for more explanation about queryMetadata.

        :param args: arguments to be passed on to mapper.queryMetadata
        :param kwargs: keyword arguments to be passed on to mapper.queryMetadata
        :return:The type of item is dependent on the mapper being used but is typically a set that contains
        available values for the keys in the format input argument.
        """
        if self._mapper is None:
            return None
        ret = self._mapper.queryMetadata(*args, **kwargs)
        return ret

    def backup(self, *args, **kwargs):
        """Perform mapper.backup.

        See mapper.backup for more information about args and kwargs.

        :param args: arguments to be passed on to mapper.backup
        :param kwargs: keyword arguments to be passed on to mapper.backup
        :return: None
        """
        if self._mapper is None:
            return None
        self._mapper.backup(*args, **kwargs)

    def getMapperDefaultLevel(self):
        """Get the default level of the mapper.

        This is typically used if no level is passed into butler methods that call repository.getKeys and/or
        repository.queryMetadata. There is a bug in that code because it gets the default level from this
        repository but then uses that value when searching all repositories. If this and other repositories
        have dissimilar data, the default level value will be nonsensical. A good example of this issue is in
        Butler.subset; it needs refactoring.

        :return:
        """
        if self._mapper is None:
            return None
        return self._mapper.getDefaultLevel()

    def exists(self, location):
        """Check if location exists in storage.

        Parameters
        ----------
        location : ButlerLocation
            Desrcibes a location in storage to look for.

        Returns
        -------
        bool
            True if location exists, False if not.
        """
        butlerLocationStorage = location.getStorage()
        if butlerLocationStorage:
            return butlerLocationStorage.exists(location)
        else:
            return self._storage.exists(location)
