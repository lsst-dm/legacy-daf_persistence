#!/usr/bin/env python
# -*- python -*-

"""This module defines the ButlerFactory class."""

import lsst.daf.base as dafBase
import lsst.pex.logging as pexLog
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import Butler, Persistence

class ButlerFactory(object):
    """ButlerFactory creates data Butlers containing data mappers.
    
    The ButlerFactory class takes mappers for an input data collection and an
    output data collection.  It can then create Butlers with these mappers.
    Each such Butler created can have state based on a partial data
    identifier.

    A data identifier is a dictionary.  The keys match those understood by a
    mapper; the values select particular data sets or collections of data
    sets.  For example, one key might be "visit".  Specifying a value of
    "695934" for this key might select a collection of images.

    The mappers perform four functions:
      1. Determine what keys are valid for data identifiers.
      2. Obtain a collection of potential data identifiers matching a
         partial data identifier.
      3. Map a data identifier to the location of the data set, including its
         C++ and Python types.
      4. Manipulate a retrieved data set object so that it conforms to a
         standard.

    This class can be configured by a policy.  Available keys are:
        inputMapperName (default = CfhtMapper)
        inputMapperPolicy
        outputMapperName (default = LsstMapper)
        outputMapperPolicy
        persistencePolicy

    Public methods:

    __init__(self, policy=None, inputMapper=None, outputMapper=None)

    create(self, **partialId)
    """

    def __init__(self, policy=None, inputMapper=None, outputMapper=None):
        """Construct a ButlerFactory.

        @param policy        policy to use for configuration.
        @param inputMapper   input collection mapper object.
        @param outputMapper  output collection mapper object.
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

        self.inputMapper = inputMapper
        if self.inputMapper is None and self.policy.exists('inputMapperName'):
            if self.policy.exists('inputMapperPolicy'):
                subpolicy = self.policy.get('inputMapperPolicy')
            else:
                subpolicy = pexPolicy.Policy()
            self.inputMapper = self._importMapper(
                    self.policy.get('inputMapperName'), subpolicy)

        self.outputMapper = outputMapper
        if self.outputMapper is None and self.policy.exists('outputMapper'):
            if self.policy.exists('outputMapperPolicy'):
                subpolicy = self.policy.get('outputMapperPolicy')
            else:
                subpolicy = pexPolicy.Policy()
            self.outputMapper = self._importMapper(
                    self.policy.get('outputMapperName'), subpolicy)

        if self.policy.exists('persistencePolicy'):
            persistencePolicy = self.policy.getPolicy('persistencePolicy')
        else:
            persistencePolicy = pexPolicy.Policy()
        self.persistence = Persistence.getPersistence(persistencePolicy)

    def create(self, **partialId):
        """Create a Butler given an optional partial data identifier.

        @param partialId  keyword arguments specifying the partial data id.
        @returns a new Butler.
        """

        return Butler(self.inputMapper, self.outputMapper,
                self.persistence, partialId)

    def _importMapper(name, policy):
        """Private function to import a mapper class and construct an instance."""
        mapperTokenList = name.split('.')
        importClassString = mapperTokenList.pop()
        importClassString = importClassString.strip()
        importPackage = ".".join(mapperTokenList)
        module = __import__(importPackage, globals(), locals(), \
                [importClassString], -1)
        mapper = getattr(module, importClassString)
        return mapper(policy)
