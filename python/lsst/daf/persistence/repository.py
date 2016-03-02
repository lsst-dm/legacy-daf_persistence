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

import collections
import copy
import inspect
import itertools
import uuid

from lsst.daf.persistence import Access, butlerExceptions, Policy


class Repository(object):
    """
    Default Multiple Parent & Output Behaviors.
    Multiple output support:
    * a Repository can have peer repositories.
    * all outputs (writes) go to all non-parent repositories. (e.g. mapping with write==False will return mappings from
      from all peer Repositories.
    Multiple parent support:
    * parents are processed in priority order, as defined by the order of the tuple passed in as cfg['parentCfgs']
    * parent search may be 'single result' or 'aggregate result':
        * single result will return after the first parent search returns a positive result.
        * aggregate result will query all parents and return a collection of results.
    * parent search is depth first
    * reads look only in parent repositories.

    Recursion is implemented in a few functions which may be overridden to alter recursive behavior:
    def doPeersAndParents()
    def doPeers()
    def doParents()


    """
    _supportedParentJoin = ('left', 'outer')


    @classmethod
    def cfg(cls, id=None, accessCfg=None, parentCfgs=[], parentJoin='left', peerCfgs=[], mapper=None, mapperArgs=None):
        if parentJoin not in Repository._supportedParentJoin:
            raise RuntimeError('Repository.cfg parentJoin:%s not supported, must be one of:'
                               % (parentJoin, Repository._supportedParentJoin))
        return Policy({'cls':cls, 'id':id, 'accessCfg':accessCfg, 'parentCfgs':parentCfgs,
                       'parentJoin':parentJoin, 'peerCfgs':peerCfgs, 'mapper':mapper,
                       'mapperArgs':mapperArgs})

    def __init__(self, cfg):
        self.cfg = cfg
        self._access = Access(cfg['accessCfg']) if cfg['accessCfg'] is not None else None
        self._parentJoin = cfg['parentJoin']
        if not self._parentJoin in Repository._supportedParentJoin:
            raise RuntimeError('Repository.__init__ parentJoin:%s not supported, must be one of:'
                               % (self._parentJoin, Repository._supportedParentJoin))

        self._parents = []
        for parentCfg in cfg['parentCfgs']:
            self._parents.append(Repository.Repository(parentCfg))
        self._peers = []
        for peerCfg in cfg['peerCfgs']:
            self._peers.append(Repository.Repository(peerCfg))

        self._id = cfg['id']

        if cfg['accessCfg.storageCfg.root'] is not None:
            self._access.setCfg(cfg)

        self.initMapper(cfg)


    def initMapper(self, repoCfg):
        # rule: If mapper is:
        # 1. an object: use it as the mapper.
        # 2. a string: import it and instantiate it with mapperArgs
        # 3. None: look for the mapper named in 'access' and use that string as in item 2.
        mapper = repoCfg['mapper']
        if mapper is None:
            mapper = self._access.mapperClass()
            if mapper is None:
                self._mapper = None
                return None
            # todo make mappers instantiated via a single mapperCfg object?
            # mapper takes root which is not ideal. it should be accessing objects via storage.
            # cameraMapper will require much refactoring to support this.
            # mapper = mapperClass(root=self._access.root(), **repoCfg['mapperArgs'])
        # if mapper is a string, import it:
        if isinstance(mapper, basestring):
            mapper = __import__(mapper)
        # now if mapper is a class type (not instance), instantiate it:
        if inspect.isclass(mapper):
            mapperArgs = copy.copy(repoCfg['mapperArgs'])
            if mapperArgs is None:
                mapperArgs = {}
            if 'root' not in mapperArgs:
                mapperArgs['root'] = self._access.root()
            mapper = mapper(**mapperArgs)
        self._mapper = mapper


        def __repr__(self):
            return 'config(id=%s, accessCfg=%s, parent=%s, mapper=%s, mapperArgs=%s, cls=%s)' % \
                   (self.id, self.accessCfg, self.parent, self.mapper, self.mapperArgs, self.cls)

    @staticmethod
    def Repository(repoCfg):
        # if it's a cfg it should have a 'cls' attribute.
        # if not, assume it's a Repository instance.
        if isinstance(repoCfg, Policy):
            return repoCfg['cls'](repoCfg)
        return repoCfg

    @staticmethod
    def getCfg(accessCfg):
        """get a persisted repository cfg from a location specified by accessCfg"""
        access = Access(accessCfg)
        return access.getCfg()

    @staticmethod
    def addChildToCfg(cfg, childCfg):
        cfg['childCfgs'].append(childCfg)

    # todo want a way to make a repository read-only - should not have, or should raise, in the write method
    def write(self, butlerLocation, obj):
        """Write a dataset to Storage.

        :param butlerLocation: Contains the details needed to find the desired dataset.
        :param dataset: The dataset to be written.
        :return:
        """
        return self._access.write(butlerLocation, obj)

    #######################
    ## Recursion support ##

    def doSelfAndPeers(self, func, *args, **kwargs):
        """Performs a function on self and each repository in _peers

        :param func: The fucntion to be performed
        :param args: args for the function
        :param kwargs: kwargs for the function
        :return: a list of return values from peers where the func did not return None.
                 if the func returned None from all peers, then returns None.
        """
        ret = []
        res = func(self, *args, **kwargs)
        if res is not None:
            # if res is a list, extend ret. else append ret:
            try:
                ret.extend(res)
            except TypeError:
                ret.append(res)
        for child in self._peers:
            res = func(child, *args, **kwargs)
            if res is not None:
                try:
                    ret.extend(res)
                except TypeError:
                    ret.append(res)
        if len(ret) is 0:
            ret = None
        return ret

    def doParents(self, func, *args, **kwargs):
        """Performas a depth-first search on parents.

        For each parent:
            performs func.
            if results are none:
                performs func on parent.
            if results are not none and join is 'left':
                returns result
            else
                appends result to list of results
        returns results if the list is not empty, else None

        If self._parentJoin is 'left' will return the return value of the first func that does not return
        None. If self._parentJoin is 'outer' will return a list of all the results of first-level parents
        (i.e. not grandparents) from func that are not None.

        :param func: a function to perform parents
        :param args: args for the function
        :param kwargs: kwargs for the function
        :return: if only 1 parent is to be used: the element to return: the element.
                 if many parents used: a list of results; one element from each parent.
                 if all the parents returned None, then None.
        """
        ret = []
        for parent in self._parents:
            res = func(parent, *args, **kwargs)
            if res is None:
                res = parent.doParents(func, *args, **kwargs)
            if res is not None:
                if self._parentJoin is 'left':
                    return res
                else:
                    ret.append(res)

        if len(ret) is 0:
            ret = None
        return ret

    def read(self, butlerLocation):
        """Read a dataset from Storage.

        :param butlerLocation: Contains the details needed to find the desired dataset.
        :return: An instance of the dataset requested by butlerLocation.
        """
        return self._access.read(butlerLocation)

    ###################
    ## Mapper Access ##

    def mappers(self):
        return (self._mapper, )

    def getKeys(self, *args, **kwargs):
      return self.doParents(Repository.doGetKeys, *args, **kwargs)

    def doGetKeys(self, *args, **kwargs):
        # todo: getKeys is not in the mapper API
        if self._mapper is None:
            return None
        return self._mapper.getKeys(*args, **kwargs)

    def map(self, *args, **kwargs):
        if 'write' in kwargs and kwargs['write'] is True:
            return self.doSelfAndPeers(Repository.doMap, *args, **kwargs)
        else:
            return self.doParents(Repository.doMap, *args, **kwargs)

    def doMap(self, *args, **kwargs):
        if self._mapper is None:
            return None
        loc = self._mapper.map(*args, **kwargs)
        if loc is None:
            return None
        # if not isinstance(ret, list):
        #     ret = [ret]
        # for r in ret:
        #     r.setRepository(self)
        if isinstance(loc, list):
            for l in loc:
                l.setRepository(self)
        else:
            loc.setRepository(self)
        return loc

    def queryMetadata(self, *args, **kwargs):
        # expect mdList to be a list of sets
        mdList= self.doParents(Repository.doQueryMetadata, *args, **kwargs)
        return mdList

    def doQueryMetadata(self, *args, **kwargs):
        if self._mapper is None:
            return None
        ret = self._mapper.queryMetadata(*args, **kwargs)
        if ret is None:
            return ret
        return ret

    def backup(self, *args, **kwargs):
        self.doSelfAndPeers(Repository.doBackup, *args, **kwargs)

    def doBackup(self, *args, **kwargs):
        if self._mapper is None:
            return None
        self._mapper.backup(*args, **kwargs)

    def getMapperDefaultLevel(self):
        if self._mapper is None:
            return None
        return self._mapper.getDefaultLevel()


