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

from lsst.daf.persistence import Access, Policy, Mapper, LogicalLocation, ButlerLocation, Storage

import yaml

class RepositoryCfg(Policy, yaml.YAMLObject):
    yaml_tag = u"!RepositoryCfg"
    yaml_loader = yaml.Loader
    yaml_dumper = yaml.Dumper

    def __init__(self, cls, mapper, mapperArgs, storageCfg, parentCfgs, tags, mode):
        super(RepositoryCfg, self).__init__()
        def listify(x):
            if x is None:
                raise RuntimeError("Unexpected None value")
            if not hasattr(x, '__iter__'):
                x = [x]
            return x
        
        tags = listify(tags)
        parentCfgs = listify(parentCfgs)

        self.update({'cls':cls, 'mapper':mapper, 'mapperArgs':mapperArgs, 'storageCfg':storageCfg, 
                     'parentCfgs':parentCfgs, 'tags':tags, 'mode':mode})

    @staticmethod
    def to_yaml(dumper, obj):
        return dumper.represent_mapping(RepositoryCfg.yaml_tag,
                                        {'mode':obj['mode'],
                                         'cls':obj['cls'], 
                                         'storageCfg':obj['storageCfg'],
                                         'parentCfgs':obj['parentCfgs'],
                                         'mapper':obj['mapper'],
                                         'mapperArgs':obj['mapperArgs'],
                                         'tags':obj['tags']})
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
            cfg['storageCfg.root'] = os.path.dirname(location)
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
    """Represents a repository of persisted data and has methods to access that data.
    """

    @classmethod
    def cfg(cls, mode, mapper=None, mapperArgs=None, parentCfgs=None, storageCfg=None, tags=[]):
        """
        Helper func to create a properly formatted Policy to configure a Repository.

        .. warning::

            cfg is 'wet paint' and very likely to change. Use of it in production code other than via the 'old
            butler' API is strongly discouraged.


        :param id: an identifier for this repository. Currently only used for debugging.
        :param parentCfgs: a tuple of repo cfgs of parent repositories, in search-priority order.
        :param parentJoin: behavior specifier for searching parents. must be one of _supportedParentJoin.
        :param peerCfgs: tuple of repo cfgs of peer repositories.
        :param mapper: mapper to use with this repo. May be a fully-qualified name
                       (e.g. lsst.daf.butlerUtils.CameraMapper) to be instantiated, a class instance, or a
                       class type to be instantiated.
        :param mapperArgs: a dict of arguments to pass to the Mapper if it is to be instantiated.
        :return: a properly populated cfg Policy.
        """
        if parentCfgs is None:
            parentCfgs = []
        return RepositoryCfg(cls=cls, storageCfg=storageCfg, parentCfgs=parentCfgs, mapper=mapper, 
                             mapperArgs=mapperArgs, tags=tags, mode=mode)

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
        self._storage = Storage.makeFromCfg(cfg['storageCfg'])
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
            if self._storage is not None:
                mapper = self._storage.mapperClass()
            if mapper is None:
                self._mapper = None
                return None
        # if mapper is a cfg (IE an instance of the badly-named Policy class), instantiate via the cfg.
        if isinstance(mapper, Policy):
            # code at this location that knows that the mapper needs to share the repo's access instance is
            # not ideal IMO. Not sure how to rectify in a good way.
            if mapper['storage'] is None:
                #mapper = copy.copy(mapper)
                mapper['storage'] = self._storage
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
                # so that root doesn't have to be redundantly passed in cfgs, if root is specified in the
                # storage and if it is an argument to the mapper, make sure that it's present in mapperArgs.
                if ('root' in inspect.getargspec(mapper.__init__ ).args and 
                    'root' not in mapperArgs):
                    mapperArgs['root'] = self._storage.root
                mapper = mapper(**mapperArgs)
        self._mapper = mapper


        def __repr__(self):
            return 'config(id=%s, storage=%s, parent=%s, mapper=%s, mapperArgs=%s, cls=%s)' % \
                   (self.id, self._storage, self.parent, self.mapper, self.mapperArgs, self.cls)

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
        return self._storage.write(butlerLocation, obj)

    def read(self, butlerLocation):
        """Read a dataset from Storage.

        :param butlerLocation: Contains the details needed to find the desired dataset.
        :return: An instance of the dataset requested by butlerLocation.
        """
        return self._storage.read(butlerLocation)

    ###################
    ## Mapper Access ##

    def mappers(self):
        return (self._mapper, )

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
        if not keys:
            return None
        return keys

    def map(self, *args, **kwargs):
        """Find a butler location for the given arguments.
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


