#!/usr/bin/env python

import glob
import os
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import ButlerLocation, Mapper
import lsst.ip.isr.calibDatabase as calibDatabase

class CfhtMapper(Mapper):
    fileRegExps = {
        "raw": r'raw-(?P<obsid>\d+)-e(?P<exposure>\d+)-c(?P<ccd>\d+)-a(?P<amp>\d+)\.fits'
    }
    fields = "D1 D2 D3 D4 W1 W2 W3 W4".split()

    def __init__(self, policy=None, root=None, calibrationRoot=None,
            calibrationDb=None, rawTemplate=None):
        Mapper.__init__(self)

        policyFile = pexPolicy.DefaultPolicyFile("daf_persistence",
                "CfhtMapperDictionary.paf", "policy")
        defaults = pexPolicy.Policy.createPolicy(policyFile,
                policyFile.getRepositoryPath())
        if policy is None:
            self.policy = pexPolicy.Policy()
        else:
            self.policy = policy
        self.policy.mergeDefaults(defaults)

        # Explicit arguments override policy
        self.root = root
        if self.root is None and self.policy.exists("root"):
            self.root = self.policy.get("root")

        self.calibrationRoot = calibrationRoot
        if self.calibrationRoot is None and \
                self.policy.exists("calibrationRoot"):
            self.calibrationRoot = self.policy.get("calibrationRoot")

        self.calibrationDb = calibrationDb
        if self.calibrationDb is None and \
                self.policy.exists("calibrationDb"):
            self.calibrationDb = self.policy.get("calibrationDb")

        if os.path.split(self.calibrationDb)[0] == '':
            self.calibrationDb = os.path.join(self.root, self.calibrationDb)

        self.rawTemplate = rawTemplate
        if self.rawTemplate is None and \
                self.policy.exists("rawTemplate"):
            self.rawTemplate = self.policy.get("rawTempalte")

        self.butler = None
        self.metadataCache = {}

    def keys(self):
        return ["field", "obsid", "exposure", "ccd", "amp", "filter",
                "expTime", "skyTile"]

    def parseFilename(self, filename):
        dataId = {}
        for dataSetType, fileRegExp in fileRegExps:
            match = re.match(fileRegExp, filename)
            if match:
                for k in self.keys():
                    try:
                        dataId[k] = match.group(k)
                    except IndexError:
                        pass
                return dataId

    def getCollection(self, dataSetType, key, dataId):
        if self.cache is None or not self.cache.has_key(dataSetType):
            self._generateCache(dataSetType)
        cache = self.cache[dataSetType]
        fieldList = cache.fields
        mappedFields = set()
        for k in dataId.keys():
            if not k in fieldList:
                mappedFields += k
        keySet = set()
        for tuple in cache:
            selected = True
            i = 0
            while selected and i < len(fieldList):
                field = fieldList[i]
                value = tuple[i]
                i += 1
                if dataId.has_key(field) and value != dataId[field]:
                    selected = False
            if selected:
                keySet += tuple[key]
        return keySet

    def map_raw(self, dataId):
        path = self.root
        path = os.path.join(path, self.rawTemplate % dataId)
        return ButlerLocation(
                "lsst.afw.image.DecoratedImageU", "DecoratedImageU",
                [("FitsStorage", path)], dataId)

    def map_bias(self, dataId):
        dateTime = self.metadataForDataId(dataId).get('taiObs')
        path = self.calibDb.lookup(dateTime, 'bias',
                dataId['ccd'], dataId['amp'], None, 0)
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.afw.image.DecoratedImageF", "DecoratedImageF",
                [("FitsStorage", path)], dataId)

    def map_dark(self, dataId):
        dateTime = self.metadataForDataId(dataId).get('taiObs')
        if dataId.has_key('expTime'):
            expTime = dataId['expTime']
        else:
            expTime = self.metadataForDataId(dataId).get('expTime')
        path = self.calibDb.lookup(dateTime, 'dark',
                dataId['ccd'], dataId['amp'], None, expTime)
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.afw.image.DecoratedImageF", "DecoratedImageF",
                [("FitsStorage", path)], dataId)

    def map_defect(self, dataId):
        dateTime = self.metadataForDataId(dataId).get('taiObs')
        path = self.calibDb.lookup(dateTime, 'defect',
                dataId['ccd'], dataId['amp'], None)
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.pex.policy.Policy", "Policy",
                [("PafStorage", path)], dataId)

    def map_flat(self, dataId):
        dateTime = self.metadataForDataId(dataId).get('taiObs')
        if dataId.has_key('filter'):
            filter = dataId['filter']
        else:
            filter = self.metadataForDataId(dataId).get('filter')
        path = self.calibDb.lookup(dateTime, 'flat',
                dataId['ccd'], dataId['amp'], filter)
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.afw.image.DecoratedImageF", "DecoratedImageF",
                [("FitsStorage", path)], dataId)

    def map_fringe(self, dataId):
        dateTime = self.metadataForDataId(dataId).get('taiObs')
        if dataId.has_key('filter'):
            filter = dataId['filter']
        else:
            filter = self.metadataForDataId(dataId).get('filter')
        path = self.calibDb.lookup(dateTime, 'fringe',
                dataId['ccd'], dataId['amp'], filter)
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.afw.image.DecoratedImageF", "DecoratedImageF",
                [("FitsStorage", path)], dataId)

    def map_linearize(self, dataId):
        path = self.calibDb.lookup(None, 'linearize')
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.pex.policy.Policy", "Policy",
                [("PafStorage", path)], dataId)

    def metadataForDataId(self, dataId):
        if self.metadataCache.has_key(dataId['obsid']):
            return self.metadataCache[dataId['obsid']]
        if self.butler is None:
            bf = ButlerFactory(inputMapper=self)
            self.butler = bf.create()
        internalId = {}
        internalId.update(dataId)
        if not internalId.has_key('exposure'):
            internalId['exposure'] = 0
        if not internalId.has_key('ccd'):
            internalId['ccd'] = 0
        if not internalId.has_key('amp'):
            internalId['amp'] = 0
        image = self.butler.get('raw', dataId)
        metadata = image.getMetadata()
        self.metadataCache[dataId['obsid']] = metadata
        return metadata

    def std_raw(self, item):
        try:
            metadata = item.getMetadata()
        except:
            return item
        # TODO -- fix up metadata here
        return item
