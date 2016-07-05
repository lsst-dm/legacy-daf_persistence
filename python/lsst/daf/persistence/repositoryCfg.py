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

import inspect
import yaml

from lsst.daf.persistence import listify

class RepositoryCfg(yaml.YAMLObject):
    yaml_tag = u"!RepositoryCfg_v1"

    def __init__(self, root, mapper, mapperArgs, parents, isLegacyRepository=False):
        self._root = root
        self._mapper = mapper
        self._mapperArgs = mapperArgs
        if parents is None:
            self._parents = []
        elif not hasattr(parents, '__iter__'):
            self._parents = list(parents)
        else:
            self._parents = parents
        self._isLegacyRepository = isLegacyRepository

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
                            parents=d['_parents'], isLegacyRepository=d['_isLegacyRepository'])
        return cfg

    def __eq__(self, other):
        if not other:
            return False
        return self._root == other._root and \
               self.mapper == other._mapper and \
               self.mapperArgs == other._mapperArgs and \
               self.parents == other._parents and \
               self._isLegacyRepository == other._isLegacyRepository

    def __ne__(self, other):
        return not self.__eq__(other)

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

    @property
    def mapperArgs(self):
        return self._mapperArgs
    
    @mapperArgs.setter
    def mapperArgs(self, newDict):
        self._mapperArgs = newDict

    @property
    def parents(self):
        return self._parents

    def addParents(self, newParents):
        newParents = listify(newParents)
        for newParent in newParents:
            if newParent not in self._parents:
                self._parents.append(newParent)

    @property
    def isLegacyRepository(self):
        return self._isLegacyRepository

    @staticmethod
    def makeFromArgs(repositoryArgs):
        cfg = RepositoryCfg(root=repositoryArgs.root, 
                            mapper = repositoryArgs.mapper, 
                            mapperArgs = repositoryArgs.mapperArgs,
                            parents=None,
                            isLegacyRepository=repositoryArgs.isLegacyRepository)
        return cfg

    def matchesArgs(self, repositoryArgs):
        if repositoryArgs.root is not None and self._root != repositoryArgs.root:
            return False
        if repositoryArgs.mapper is not None:
            if inspect.isclass(self._mapper):
                if not inspect.isclass(repositoryArgs.mapper):
                    return False
                if self._mapper != repositoryArgs.mapper:
                    return False
            else:
                if type(self._mapper) != type(repositoryArgs.mapper):
                    return False
        # check mapperArgs for any keys in common and if their value does not match then return false.
        if self._mapperArgs is not None and repositoryArgs.mapperArgs is not None:
            for key in set(self._mapperArgs.keys()) & set(repositoryArgs.mapperArgs):
                if self._mapperArgs[key] != repositoryArgs.mapperArgs[key]:
                    return False
        return True

    def __repr__(self):
        return "%s(root=%r, mapper=%r, mapperArgs=%r, parents=%s, isLegacyRepository=%s)" % (
            self.__class__.__name__, 
            self._root, 
            self._mapper, 
            self._mapperArgs, 
            self._parents,
            self._isLegacyRepository) 

yaml.add_constructor(u"!RepositoryCfg_v1", RepositoryCfg.v1Constructor)
