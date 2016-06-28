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
    yaml_tag = u"!RepositoryCfg"

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

    @property
    def root(self):
        return self._root

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
        # todo need to check for duplicate parents
        self._parents.extend(newParents)

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
