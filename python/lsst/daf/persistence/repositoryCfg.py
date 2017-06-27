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
import yaml
from . import iterify, doImport, Storage, ParentsMismatch
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

    def __init__(self, root, mapper, mapperArgs, parents, policy):
        self._root = root
        self._mapper = mapper
        self._mapperArgs = mapperArgs
        self._parents = None
        self.addParents(iterify(parents))
        self._policy = policy
        self.dirty = True  # if dirty, the parameters have been changed since the cfg was read or written.

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
                            parents=None, policy=d.get('_policy', None))
        #  Where possible we mangle the parents so that they are relative to root, for example if the root and
        #  the parents are both in the same PosixStorage. The parents are serialized in mangled form; when
        #  deserializing the parents we do not re-mangle them.
        cfg._parents = d['_parents']
        cfg.dirty = False
        return cfg

    def __eq__(self, other):
        if not other:
            return False
        return self.root == other.root and \
            self.mapper == other.mapper and \
            self.mapperArgs == other.mapperArgs and \
            self.parents == other.parents and \
            self.policy == other.policy

    def __ne__(self, other):
        return not self.__eq__(other)

    def extend(self, other):
        """Extend this RepositoryCfg with extendable values from the other RepositoryCfg.

        Currently the only extendable value is parents; see `extendParents` for more detials about extending
        the parents list.

        Parameters
        ----------
        other : RepositoryCfg
            A RepositoryCfg instance to update values from.

        Raises
        ------
        RuntimeError
            If non-extendable parameters do not match a RuntimeError will be raised.
            (If this RepositoryCfg's parents can not be extended with the parents of the other repository,
            extendParents will raise).
        """
        if (self.root != other.root or
                self.mapper != other.mapper or
                self.mapperArgs != other.mapperArgs or
                self.policy != other.policy):
            raise RuntimeError("{} can not be extended with cfg:{}".format(self, other))
        self.extendParents(other.parents)

    def _extendsParents(self, newParents):
        """Query if a list of parents starts with the same list of parents as this RepositoryCfg's parents,
        with new parents at the end.

        Parameters
        ----------
        newParents : list of string and/or RepositoryCfg
            A list of parents that contains all the parents that would be in this RepositoryCfg.
            This must include parents that may already be in this RepositoryCfg's parents list. Paths must be
            in absolute form (not relative).

        Returns
        -------
        bool
            True if the beginning of the new list matches this RepositoryCfg's parents list, False if not.
        """
        doesExtendParents = False
        return doesExtendParents

    def extendParents(self, newParents):
        """Determine if a parents list matches our parents list, with extra items at the end. If a list of
        parents does not match but the mismatch is because of new parents at the end of the list, then they
        can be added to the cfg.

        Parameters
        ----------
        newParents : list of string
            A list of parents that contains all the parents that are to be recorded into this RepositoryCfg.
            This must include parents that may already be in this RepositoryCfg's parents list

        Raises
        ------
        ParentsListMismatch
            Description
        """
        newParents = self._normalizeParents(self.root, newParents)
        doRaise = False
        if self._parents != newParents:
            if all(x == y for (x, y) in zip(self._parents, newParents)):
                if len(self._parents) < len(newParents):
                    self._parents = newParents
                    self.dirty = True
                elif len(self._parents) == len(newParents):
                    pass
                else:
                    doRaise = True
            else:
                doRaise = True
        if doRaise:
            raise ParentsMismatch(("The beginning of the passed-in parents list: {} does not match the " +
                                  "existing parents list in this RepositoryCfg: {}").format(
                                  newParents, self._parents))

    @property
    def root(self):
        return self._root

    @root.setter
    def root(self, root):
        if root is not None and self._root is not None:
            raise RuntimeError("Explicity clear root (set to None) before changing the value of root.")
        self._root = root

    @property
    def mapper(self):
        return self._mapper

    @mapper.setter
    def mapper(self, mapper):
        if self._mapper is not None:
            raise RuntimeError("Should not set mapper over previous not-None value.")
        self.dirty = True
        self._mapper = mapper

    @property
    def mapperArgs(self):
        return self._mapperArgs

    @mapperArgs.setter
    def mapperArgs(self, newDict):
        self.dirty = True
        self._mapperArgs = newDict

    @property
    def parents(self):
        if self._parents is None:
            return []
        return self._denormalizeParents(self.root, self._parents)

    @staticmethod
    def _normalizeParents(root, newParents):
        """Eliminate symlinks in newParents and get the relative path (if one exists) from root to each parent
        root.

        Parameters
        ----------
        newParents : list containing strings and RepoistoryCfg instances
            Same as in `addParents`.

        Returns
        -------
        list of strings and RepositoryCfg instances.
            Normalized list of parents
        """
        newParents = iterify(newParents)
        for i in range(len(newParents)):
            if isinstance(newParents[i], RepositoryCfg):
                newParents[i] = copy.copy(newParents[i])
                parentRoot = newParents[i].root
                newParents[i].root = None
                newParents[i].root = Storage.relativePath(root, parentRoot)
            else:
                newParents[i] = Storage.relativePath(root, newParents[i])
        return newParents

    @staticmethod
    def _denormalizeParents(root, parents):
        def getAbs(root, parent):
            if isinstance(parent, RepositoryCfg):
                parentRoot = parent.root
                parent.root = None
                parent.root = Storage.absolutePath(root, parentRoot)
            else:
                parent = Storage.absolutePath(root, parent)
            return parent
        return [getAbs(root, parent) for parent in parents]

    def addParents(self, newParents):
        """Add a parent or list of parents to this RepositoryCfg

        Parameters
        ----------
        newParents : string or RepositoryCfg instance, or list of these.
            If string, newParents should be a path or URI to the parent
            repository. If RepositoryCfg, newParents should be a RepositoryCfg
            that describes the parent repository in part or whole.
        """
        if len(newParents) > 0 and self._parents is None:
            self._parents = []
        newParents = self._normalizeParents(self.root, newParents)
        for newParent in newParents:
            if newParent not in self._parents:
                self.dirty = True
                self._parents.append(newParent)

    @property
    def policy(self):
        return self._policy

    @staticmethod
    def makeFromArgs(repositoryArgs):
        cfg = RepositoryCfg(root=repositoryArgs.root,
                            mapper=repositoryArgs.mapper,
                            mapperArgs=repositoryArgs.mapperArgs,
                            parents=None,
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


yaml.add_constructor(u"!RepositoryCfg_v1", RepositoryCfg.v1Constructor)
