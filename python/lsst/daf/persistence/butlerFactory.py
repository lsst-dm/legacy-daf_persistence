#!/usr/bin/env python
# -*- python -*-

"""
ButlerFactory
"""

import lsst.daf.base as dafBase
import lsst.pex.logging as pexLog
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import Butler, Persistence

class ButlerFactory(object):
    """This class creates Butlers."""

    def __init__(self, policy=None, inputMapper=None, outputMapper=None):
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
        return Butler(self.inputMapper, self.outputMapper,
                self.persistence, partialId)

    def _importMapper(name, policy):
        mapperTokenList = name.split('.')
        importClassString = mapperTokenList.pop()
        importClassString = importClassString.strip()
        importPackage = ".".join(mapperTokenList)
        module = __import__(importPackage, globals(), locals(), \
                [importClassString], -1)
        mapper = getattr(module, importClassString)
        return mapper(policy)
