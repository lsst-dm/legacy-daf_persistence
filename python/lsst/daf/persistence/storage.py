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

import yaml

from lsst.daf.persistence import Policy

class Storage(object):
    """Base class for storages"""

    @staticmethod
    def makeFromCfg(storageCfg):
        '''Instantiate from a configuration.
        In come cases the storageCfg may have already been instantiated into a Repository, this is allowed and
        the input var is simply returned.

        .. warning::

            cfg is 'wet paint' and very likely to change. Use of it in production code other than via the 'old
            butler' API is strongly discouraged.


        :param storageCfg: the cfg for this repository. It is recommended this be created by calling
                           a the cfg member function of a Storage subclass.
        :return: a Storage instance. Exactly what type depends on how the cfg was made.
        '''
        if isinstance(storageCfg, Policy):
            return storageCfg['cls'](storageCfg)
        return storageCfg


class StorageCfg(Policy):
    yaml_tag = u"!StorageCfg"
    yaml_loader = yaml.Loader
    yaml_dumper = yaml.Dumper

    def __init__(self, cls, root=None):
        super(StorageCfg, self).__init__()
        self.update({'root':root, 'cls':cls})

    @staticmethod
    def to_yaml(dumper, obj):
        return dumper.represent_mapping(StorageCfg.yaml_tag, {'cls':obj['cls']})

    @staticmethod
    def from_yaml(loader, node):
        obj = loader.construct_mapping(node)
        return StorageCfg(**obj)
