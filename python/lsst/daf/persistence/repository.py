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
import os
import uuid

from lsst.daf.persistence import Access, Policy, Mapper, LogicalLocation, ButlerLocation

import yaml

class RepositoryCfg(Policy, yaml.YAMLObject):
    yaml_tag = u"!RepositoryCfg"
    yaml_loader = yaml.Loader
    yaml_dumper = yaml.Dumper

    def __init__(self, cls, id, accessCfg, parentCfgs, parentJoin, peerCfgs, mapper, mapperArgs):
        super(RepositoryCfg, self).__init__()
        if not hasattr(parentCfgs, '__iter__'):
            parentCfgs = (parentCfgs,)
        self.update({'cls':cls, 'id':id, 'accessCfg':accessCfg, 'parentCfgs':parentCfgs,
                   'parentJoin':parentJoin, 'peerCfgs':peerCfgs, 'mapper':mapper,
                   'mapperArgs':mapperArgs})

    @staticmethod
    def to_yaml(dumper, obj):
        return dumper.represent_mapping(RepositoryCfg.yaml_tag,
                                        {'cls':obj['cls'], 'id':obj['id'], 'accessCfg':obj['accessCfg'],
                                         'parentCfgs':obj['parentCfgs'], 'parentJoin':obj['parentJoin'],
                                         'peerCfgs':obj['peerCfgs'], 'mapper':obj['mapper'],
                                         'mapperArgs':obj['mapperArgs']})
    @staticmethod
    def from_yaml(loader, node):
        obj = loader.construct_mapping(node)
        return RepositoryCfg(**obj)

    # todo these load & write methods are coupled to posix storage. need to invent butler mechanism for
    # multiple dispatch and implement it.
    @staticmethod
    def butlerRead(butlerLocation):
        if butlerLocation.getStorageName() is not "YamlStorage":
            raise NotImplementedError("RepositoryCfg only supports YamlStorage")
        ret = []
        for location in butlerLocation.getLocations():
            logLoc = LogicalLocation(location, butlerLocation.getAdditionalData())
            with open(logLoc.locString()) as f:
                cfg = yaml.load(f)
            cfg['accessCfg.storageCfg.root'] = os.path.dirname(location)
            ret.append(cfg)
        return ret

    @staticmethod
    def butlerWrite(obj, butlerLocation):
        if butlerLocation.getStorageName() is not "YamlStorage":
            raise NotImplementedError("RepositoryCfg only supports YamlStorage")
        ret = []
        for location in butlerLocation.getLocations():
            logLoc = LogicalLocation(location, butlerLocation.getAdditionalData())
            if not os.path.exists(os.path.dirname(logLoc.locString())):
                os.makedirs(os.path.dirname(logLoc.locString()))
            with open(logLoc.locString(), 'w') as f:
                yaml.dump(obj, f)

class Repository(object):
    """
    Default Multiple Parent & Output Behaviors.
    * put/write operates only in non-parent repositories.
    * get/read operates only in parent repositories.
    Multiple output support:
    * a Repository can have peer repositories.
    * all outputs (writes) go to all non-parent repositories. (e.g. mapping with write==False will return
      mappings for from all peer Repositories.
    Multiple parent support:
    * parents are processed in priority order, as defined by the order of the tuple passed in as
      cfg['parentCfgs']
    * parent search is depth first (1st parent, parents of 1st parent, grandparents of 1st parent, 2nd parent,
      etc)
    * if parentJoin is 'left' returns after first result is found.
    * if parentJoin is 'outer' will return all results found.

    Recursion is implemented in a few functions which may be overridden to alter recursive behavior:
    def doPeersAndParents()
    def doPeers()
    def doParents()


    """
    _supportedParentJoin = ('left', 'outer')

    @classmethod
    def cfg(cls, id=None, accessCfg=None, parentCfgs=[], parentJoin='left', peerCfgs=[], mapper=None, mapperArgs=None):
        """
        Helper func to create a properly formatted Policy to configure a Repository.

        .. warning::

            cfg is 'wet paint' and very likely to change. Use of it in production code other than via the 'old
            butler' API is strongly discouraged.


        :param id: an identifier for this repository. Currently only used for debugging.
        :param accessCfg: cfg for the Access class
        :param parentCfgs: a tuple of repo cfgs of parent repositories, in search-priority order.
        :param parentJoin: behavior specifier for searching parents. must be one of _supportedParentJoin.
        :param peerCfgs: tuple of repo cfgs of peer repositories.
        :param mapper: mapper to use with this repo. May be a fully-qualified name
                       (e.g. lsst.daf.butlerUtils.CameraMapper) to be instantiated, a class instance, or a
                       class type to be instantiated.
        :param mapperArgs: a dict of arguments to pass to the Mapper if it is to be instantiated.
        :return: a properly populated cfg Policy.
        """
        if parentJoin not in Repository._supportedParentJoin:
            raise RuntimeError('Repository.cfg parentJoin:%s not supported, must be one of:'
                               % (parentJoin, Repository._supportedParentJoin))
        return RepositoryCfg(cls=cls, id=id, accessCfg=accessCfg, parentCfgs=parentCfgs,
                             parentJoin=parentJoin, peerCfgs=peerCfgs, mapper=mapper, mapperArgs=mapperArgs)

    @staticmethod
    def makeFromCfg(repoCfg):
        '''Instantiate a Repository from a configuration.
        In come cases the repoCfg may have already been instantiated into a Repository, this is allowed and
        the input var is simply returned.

        .. warning::

            cfg is 'wet paint' and very likely to change. Use of it in production code other than via the 'old
            butler' API is strongly discouraged.


        :param repoCfg: the cfg for this repository. It is recommended this be created by calling
                        Repository.cfg()
        :return: a Repository instance
        '''
        if isinstance(repoCfg, Policy):
            return repoCfg['cls'](repoCfg)
        return repoCfg


    def __init__(self, cfg):
        '''Initialize a Repository with parameters input via config.

        :param cfg: It is recommended that this config be created by calling Repository.cfg(...) to ensure all
                    the required keys are set.
        :return:
        '''
        self.cfg = cfg
        self._access = Access(self.cfg['accessCfg']) if self.cfg['accessCfg'] is not None else None
        self._parentJoin = self.cfg['parentJoin']
        if not self._parentJoin in Repository._supportedParentJoin:
            raise RuntimeError('Repository.__init__ parentJoin:%s not supported, must be one of:'
                               % (self._parentJoin, Repository._supportedParentJoin))
        self._parents = []
        parentCfgs = self.cfg['parentCfgs']
        if not hasattr(parentCfgs, '__iter__'):
            parentCfgs = (parentCfgs,)
        for parentCfg in parentCfgs:
            self._parents.append(Repository.makeFromCfg(parentCfg))
        self._peers = []
        for peerCfg in self.cfg['peerCfgs']:
            self._peers.append(Repository.makeFromCfg(peerCfg))
        self._id = self.cfg['id']

        self._initMapper(cfg)

    def _initMapper(self, repoCfg):
        '''Initialize and keep the mapper in a member var.

        :param repoCfg:
        :return:
        '''

        # rule: If mapper is:
        # - a policy: instantiate it via the policy
        # - an object: use it as the mapper.
        # - a string: import it and instantiate it with mapperArgs
        # - None: look for the mapper named in 'access' and use that string as in item 2.
        mapper = repoCfg['mapper']
        if mapper is None:
            if self._access is not None:
                mapper = self._access.mapperClass()
            if mapper is None:
                self._mapper = None
                return None
        # if mapper is a cfg (IE an instance of the badly-named Policy class), instantiate via the cfg.
        if isinstance(mapper, Policy):
            # code at this location that knows that the mapper needs to share the repo's access instance is
            # not ideal IMO. Not sure how to rectify in a good way.
            if mapper['access'] is None:
                #mapper = copy.copy(mapper)
                mapper['access'] = self._access
            mapper = Mapper.Mapper(mapper)
        # if mapper is a string, import it:
        if isinstance(mapper, basestring):
            mapper = __import__(mapper)
        # now if mapper is a class type (not instance), instantiate it:
        if inspect.isclass(mapper):
            # cameraMapper requires root which is not ideal. it should be accessing objects via storage.
            # cameraMapper and other existing mappers (hscMapper) will require much refactoring to support this.
            args = inspect.getargspec(mapper.__init__)
            useRootKeyword = not 'cfg' in args.args
            if not useRootKeyword:
                try:
                    # try new style init first; pass cfg to mapper
                    mapper = mapper(cfg=repoCfg['mapperCfg'])
                except TypeError:
                    # try again, using old style cfg: using mapperArgs and root keywords
                    useRootKeyword = True
            if useRootKeyword:
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
    def loadCfg(accessCfg):
        """Load a repository cfg that has been saved in a location specified by accessCfg

        .. warning::

            cfg is 'wet paint' and very likely to change. Use of it in production code other than via the 'old
            butler' API is strongly discouraged.
        """
        access = Access(accessCfg)
        return access.loadCfg()

    # todo want a way to make a repository read-only
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
        """
        Get the keys available in the repository/repositories.
        :param args:
        :param kwargs:
        :return: A dict of {key:valueType} or a list of these dicts, depending on the parentJoin rules.
        """
        return self.doParents(Repository.doGetKeys, *args, **kwargs)

    def doGetKeys(self, *args, **kwargs):
        """Get the keys from this repository only. Typically this function is called only by doParents, and
        other classes should call getKeys.

        :param args:
        :param kwargs:
        :return: A dict of {key:valueType}
        """
        # todo: getKeys is not in the mapper API
        if self._mapper is None:
            return None
        return self._mapper.getKeys(*args, **kwargs)

    def map(self, *args, **kwargs):
        """Find a butler location for the given arguments.

        If 'write' is in the kwargs and set to True then this is treated as a mapping intended to be used in a
        call to butler.put and will look in the output repositories. Otherwise it's treated as a mapping for
        butler.get and will look in the input repositories.

        See mapper documentation for more detials about the use of map.
        :param args: arguments to be passed on to mapper.map
        :param kwargs: keyword arguments to be passed on to mapper.map
        :return: An item or a list, depending on parentJoin rules. The type of item is dependent on the mapper
        being used but is typically a ButlerLocation.
        """
        if 'write' in kwargs and kwargs['write'] is True:
            return self.doSelfAndPeers(Repository.doMap, *args, **kwargs)
        else:
            return self.doParents(Repository.doMap, *args, **kwargs)

    def doMap(self, *args, **kwargs):
        """Perform the map function on this repository only.

        See mapper.map for more information about args and kwargs.

        :param args: arguments to be passed on to mapper.map
        :param kwargs: keyword arguments to be passed on to mapper.map
        :return: The type of item is dependent on the mapper being used but is typically a ButlerLocation.
        """
        if self._mapper is None:
            return None
        loc = self._mapper.map(*args, **kwargs)
        if loc is None:
            return None
        loc.setRepository(self)
        return loc

    def queryMetadata(self, *args, **kwargs):
        """Gets possible values for keys given a partial data id.

        See mapper documentation for more explanation about queryMetadata.

        :param args: arguments to be passed on to mapper.map
        :param kwargs: keyword arguments to be passed on to mapper.map
        :return: An item or a list, depending on parentJoin rules. The type of item is dependent on the mapper
        being used but is typically a set that contains available values for the keys in the format iarg.
        """
        mdList= self.doParents(Repository.doQueryMetadata, *args, **kwargs)
        return mdList

    def doQueryMetadata(self, *args, **kwargs):
        """Perform the queryMetadata function on this repository only.

        See mapper.queryMetadata for more information about args and kwargs.

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
        """Calls mapper.backup on all output repositories.

        :param args: arguments to be passed on to mapper.backup
        :param kwargs: keyword arguments to be passed on to mapper.backup
        :return: None
        """
        self.doSelfAndPeers(Repository.doBackup, *args, **kwargs)

    def doBackup(self, *args, **kwargs):
        """Perform mapper.backup on this repository only.

        See mapper.backup for more information about args and kwargs.

        :param args: arguments to be passed on to mapper.backup
        :param kwargs: keyword arguments to be passed on to mapper.backup
        :return: None
        """
        if self._mapper is None:
            return None
        self._mapper.backup(*args, **kwargs)

    def getMapperDefaultLevel(self):
        """Get the default level for this repository only.

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


