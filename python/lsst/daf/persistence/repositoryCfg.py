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

# -*- python -*-

import copy
import os
import yaml

from lsst.daf.persistence import listify, iterify, doImport, Storage
from past.builtins import basestring


class RepositoryCfg(yaml.YAMLObject):
    """RepositoryCfg stores the configuration of a repository. Its contents are persisted to the repository
    when the repository is created in persistent storage. Thereafter the the RepositoryCfg should not change.

    Parameters
    ----------
    mapper : string
        The mapper associated with the repository. The string should be importable to a class object.
    mapperArgs : dict
        Arguments & values to pass to the mapper when initializing it.
    parents : list of URI
        URIs to the locaiton of the parent RepositoryCfgs of this repository.
    policy : dict
        Policy associated with this repository, overrides all other policy data (which may be loaded from
        policies in derived packages).
    deserializing : bool
        Butler internal use only. This flag is used to indicate to the init funciton that the repository class
        is being deserialized and should not perform certain operations that normally happen in other uses of
        init.
    """
    yaml_tag = u"!RepositoryCfg_v1"

    def freeze(self):
        """Disable all setters so the RepositoryCfg can not be modified any longer."""
        self._frozen = True

    def raiseIfFrozen(self):
        if self._frozen:
            raise RuntimeError("{!r} is frozen.".format(self))

    def __deepcopy__(self, memo):
        """Returns a deep copy of self. Will be not-frozen"""
        return type(self)(root=copy.deepcopy(self._root, memo),
                          mapper=copy.deepcopy(self._mapper, memo),
                          mapperArgs=copy.deepcopy(self._mapperArgs, memo),
                          parents=copy.deepcopy(self._parents, memo),
                          policy=copy.deepcopy(self._policy, memo),
                          deserializing=True)

    def __init__(self, root, mapper, mapperArgs, parents, policy, deserializing=False):
        self._frozen = False
        self._root = root
        self._mapper = mapper
        self._mapperArgs = mapperArgs
        #  Where possible we mangle the parents so that they are relative to root, for example if the root and
        #  the parents are both in the same PosixStorage. The parents are stored in mangled form; when
        #  deserializing the parents we do not re-mangle them.
        if deserializing:
            self._parents = parents
        else:
            self._parents = None
            self.addParents(iterify(parents))
        self._policy = policy

    @staticmethod
    def v1Constructor(loader, node):
        """Constructor for 'version 1' of the serlized RepositoryCfg.

        If new parameters are added to RepositoryCfg they will have to be checked for in d; if they are there
        then their value should be used and if they are not there a default value must be used in place.

        In case the structure of the serialzed file must be changed in a way that invalidates some of the
        keys:
        1. Increment the version number (after _v1) in the yaml_tag of this class.
        2. Add a new constructor (similar to this one) to deserialze new serializations of this class.
        3. Registered the new constructor for the new version with yaml, the same way it is done at the bottom
           of this file.
        4. All constructors for the older version(s) of persisted RepositoryCfg must be changed to adapt
           the old keys to their new uses and create the current (new) version of a repository cfg, or raise a
           RuntimeError in the case that older versions of serialized RepositoryCfgs can not be adapted.
        There is an example of migrating from a fictitious v0 to v1 in tests/repositoryCfg.py
        """
        d = loader.construct_mapping(node)
        cfg = RepositoryCfg(root=d['_root'], mapper=d['_mapper'], mapperArgs=d['_mapperArgs'],
                            parents=d['_parents'], policy=d.get('_policy', None), deserializing=True)
        return cfg

    def __eq__(self, other):
        if not other:
            return False
        return self.root == other.root and \
            self.mapper == other.mapper and \
            self.mapperArgs == other.mapperArgs and \
            self.parents == other.parents

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def root(self):
        return self._root

    @root.setter
    def root(self, root):
        self.raiseIfFrozen()
        if root is not None and self._root is not None:
            raise RuntimeError("Explicity clear root (set to None) before changing the value of root.")
        self._root = root

    @property
    def mapper(self):
        return self._mapper

    @mapper.setter
    def mapper(self, mapper):
        self.raiseIfFrozen()
        if self._mapper is not None:
            raise RuntimeError("Should not set mapper over previous not-None value.")
        self._mapper = mapper

    @property
    def mapperArgs(self):
        return self._mapperArgs

    @mapperArgs.setter
    def mapperArgs(self, newDict):
        self.raiseIfFrozen()
        self._mapperArgs = newDict

    @property
    def parents(self):
        if self._parents is None:
            return []

        def getAbs(parent):
            if isinstance(parent, RepositoryCfg):
                parentRoot = parent.root
                parent.root = None
                parent.root = Storage.absolutePath(self.root, parentRoot)
            else:
                parent = Storage.absolutePath(self.root, parent)
            return parent

        return [getAbs(p) for p in self._parents]

    def addParents(self, newParents):
        """Add a parent or list of parents to this RepositoryCfg

        Parameters
        ----------
        newParents : string or RepositoryCfg instance, or list of these.
            If string, newParents should be a path or URI to the parent
            repository. If RepositoryCfg, newParents should be a RepositoryCfg
            that describes the parent repository in part or whole.
        """
        self.raiseIfFrozen()
        if len(newParents) > 0 and self._parents is None:
            self._parents = []
        newParents = listify(newParents)
        for newParent in newParents:
            if isinstance(newParent, RepositoryCfg):
                newParent = copy.copy(newParent)
                parentRoot = newParent.root
                newParent.root = None
                newParent.root = Storage.relativePath(self.root,
                                                      parentRoot)
            else:
                newParent = Storage.relativePath(self.root, newParent)
            if newParent not in self._parents:
                self._parents.append(newParent)

    @property
    def policy(self):
        return self._policy

    def update(self, other):
        """Update self with the values from other that are not None."""
        self.raiseIfFrozen()
        if other._root is not None:
            self._root = other._root
        if other._mapper is not None:
            self._mapper = other._mapper
        if other._mapperArgs is not None:
            self._mapperArgs = other._mapperArgs
        if other._parents is not None:
            self._parents = other._parents
        if other._policy is not None:
            self._policy = other._policy

    @staticmethod
    def makeFromArgs(repositoryArgs, parents):
        cfg = RepositoryCfg(root=repositoryArgs.root,
                            mapper=repositoryArgs.mapper,
                            mapperArgs=repositoryArgs.mapperArgs,
                            parents=parents,
                            policy=repositoryArgs.policy)
        return cfg

    def matchesArgs(self, repositoryArgs):
        """Checks that a repositoryArgs instance will work with this repositoryCfg. This is useful
        when loading an already-existing repository that has a persisted cfg, to ensure that the args that are
        passed into butler do not conflict with the persisted cfg."""
        if repositoryArgs.root is not None and self._root != repositoryArgs.root:
            return False

        repoArgsMapper = repositoryArgs.mapper
        cfgMapper = self._mapper
        if isinstance(repoArgsMapper, basestring):
            repoArgsMapper = doImport(repoArgsMapper)
        if isinstance(cfgMapper, basestring):
            cfgMapper = doImport(cfgMapper)
        if repoArgsMapper is not None and repoArgsMapper != cfgMapper:
            return False
        # check mapperArgs for any keys in common and if their value does not match then return false.
        if self._mapperArgs is not None and repositoryArgs.mapperArgs is not None:
            for key in set(self._mapperArgs.keys()) & set(repositoryArgs.mapperArgs):
                if self._mapperArgs[key] != repositoryArgs.mapperArgs[key]:
                    return False
        if repositoryArgs.policy and repositoryArgs.policy != self._policy:
            return False

        return True

    def __repr__(self):
        return "%s(root=%r, mapper=%r, mapperArgs=%r, parents=%s, policy=%s)" % (
            self.__class__.__name__,
            self._root,
            self._mapper,
            self._mapperArgs,
            self._parents,
            self._policy)

    def __hash__(self):
        return hash(self.__repr__())

yaml.add_constructor(u"!RepositoryCfg_v1", RepositoryCfg.v1Constructor)
