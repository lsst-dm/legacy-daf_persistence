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

import cPickle
import collections
import os

from lsst.daf.persistence import Policy

import yaml

class AccessCfg(Policy, yaml.YAMLObject):
    yaml_tag = u"!AccessCfg"
    def __init__(self, cls, storageCfg):
        super(AccessCfg, self).__init__({'storageCfg':storageCfg, 'cls':cls})

class Access:
    """Implements an butler framework interface for Transport, Storage, and Registry"""

    @classmethod
    def cfg(cls, storageCfg):
        """Helper func to create a properly formatted Policy to configure an Access instance.

        :param storageCfg: a cfg to instantiate a storage.
        :return:
        """
        return AccessCfg(cls=cls, storageCfg=storageCfg)

    def __init__(self, cfg):
        """Initializer

        :param cfg: a Policy that defines the configuration for this class. It is recommended that the cfg be
                    created by calling Access.cfg()
        :return:
        """
        self.storage = cfg['storageCfg.cls'](cfg['storageCfg'])

    def __repr__(self):
        return 'Access(storage=%s)' % self.storage

    def mapperClass(self):
        """Get the mapper class associated with a repository root.

        :return: the mapper class
        """
        return self.storage.mapperClass()

    def root(self):
        """Get the repository root as defined by the Storage class, this refers to the 'top' of a persisted
        repository. The exact type of Root can vary based on Storage type.

        :return: the root of the persisted repository.
        """

        return self.storage.root

    def locationWithRoot(self, location):
        """Given a location, get a fully qualified handle to location including storage root.

        Note; at the time of this writing the only existing storage type is PosixStorage. This returns the
        root+location.
        :param location:
        :return:
        """
        return self.storage.locationWithRoot(location)

    def setCfg(self, repoCfg):
        """Writes the repository configuration to Storage.

        :param repoCfg: the Policy cfg to be written
        :return: None
        """
        self.storage.setCfg(repoCfg)

    def loadCfg(self):
        """Reads the repository configuration from Storage.

        :return: the Policy cfg
        """
        return self.storage.loadCfg()

    def write(self, butlerLocation, obj):
        """Passes an object to Storage to be written into the repository.

        :param butlerLocation: the location & formatting for the object to be written.
        :param obj: the object to be written.
        :return: None
        """
        self.storage.write(butlerLocation, obj)

    def read(self, butlerLocation):
        """Reads an object from storage

        :param butlerLocation: describes the location & how to load the object.
        :return:
        """
        return self.storage.read(butlerLocation=butlerLocation)

    def exists(self, location):
        """Query if a location exists.

        As of this writing the only storage type is PosixStorage, and it works to say that 'location' is a
        simple locaiton descriptor. In the case of PosixStorage that's a path. If this needs to become more
        complex it could be changed to be a butlerLocation, or something else, as needed.
        :param location: a simple location descriptor, type is dependent on Storage.
        :return: True if location exists, else False.
        """
        return self.storage.exists(location)
