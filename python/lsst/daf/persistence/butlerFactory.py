#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

"""This module defines the ButlerFactory class."""

import lsst.daf.base as dafBase
import lsst.pex.logging as pexLog
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import Butler, Mapper, Persistence

class ButlerFactory(object):
    """ButlerFactory creates data Butlers containing data mappers.
    
    The ButlerFactory class takes a mapper for an input data collection and an
    output data collection.  It can then create Butlers with these mappers.
    Each such Butler created can have state based on a partial data
    identifier.

    A data identifier is a dictionary.  The keys match those understood by a
    mapper; the values select particular data sets or collections of data
    sets.  For example, one key might be "visit".  Specifying a value of
    "695934" for this key might select a collection of images.

    The mappers perform four functions:
      1. Determine what keys are valid for dataset ids.
      2. Obtain a collection of potential dataset ids matching a
         partial dataset id.
      3. Map a dataset id to the location of the dataset, including its
         C++ and Python types.
      4. Manipulate a retrieved dataset object so that it conforms to a
         standard.

    This class can be configured by a policy.  Available keys are:
        mapperName (default = CfhtMapper)
        mapperPolicy
        persistencePolicy

    Public methods:

    __init__(self, policy=None, mapper=None)

    create(self, **partialId)
    """

    def __init__(self, policy=None, mapper=None):
        """Construct a ButlerFactory.

        @param policy policy to use for configuration.
        @param mapper mapper object.
        """

        policyFile = pexPolicy.DefaultPolicyFile("daf_persistence",
                "ButlerFactoryDictionary.paf", "policy")
        defaults = pexPolicy.Policy.createPolicy(policyFile,
                policyFile.getRepositoryPath())
        if policy is None:
            self.policy = pexPolicy.Policy()
        else:
            self.policy = policy
        self.policy.mergeDefaults(defaults)

        self.mapper = mapper
        if self.mapper is None and self.policy.exists('mapperName'):
            if self.policy.exists('mapperPolicy'):
                subpolicy = self.policy.get('mapperPolicy')
            else:
                subpolicy = pexPolicy.Policy()
            self.mapper = _importMapper(
                    self.policy.get('mapperName'), subpolicy)

        if self.policy.exists('persistencePolicy'):
            persistencePolicy = self.policy.getPolicy('persistencePolicy')
        else:
            persistencePolicy = pexPolicy.Policy()
        self.persistence = Persistence.getPersistence(persistencePolicy)

    def create(self, **partialId):
        """Create a Butler given an optional partial dataset id.

        @param partialId  keyword arguments specifying the partial dataset id.
        @returns a new Butler.
        """

        return Butler(self.mapper, self.persistence, partialId)

def _importMapper(name, policy):
    """Private function to import a mapper class and construct an instance."""
    mapperTokenList = name.split('.')
    importClassString = mapperTokenList.pop()
    importClassString = importClassString.strip()
    importPackage = ".".join(mapperTokenList)
    module = __import__(importPackage, globals(), locals(), \
            [importClassString], -1)
    if not hasattr(module, importClassString):
        raise TypeError, "Unable to find mapper class %s in module %s" % (
                importClassString, importPackage)
    mapper = getattr(module, importClassString)
    if not isinstance(mapper, type):
        raise TypeError, "Not a Mapper (or even a class): %s" % (mapper,)
    if not issubclass(mapper, Mapper):
        raise TypeError, "Not a Mapper: %s" % (mapper,)
    return mapper(policy)
