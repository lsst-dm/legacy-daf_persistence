#!/usr/bin/env python

import os
import re
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import Registry, ButlerFactory, ButlerLocation, Mapper, CalibDb
import lsst.daf.base as dafBase
import lsst.pex.exceptions as pexExcept

class CfhtMapper(Mapper):
    def __init__(self, policy=None, **rest):
        Mapper.__init__(self)

        mapperDict = pexPolicy.DefaultPolicyFile("daf_persistence",
                "CfhtMapperDictionary.paf", "policy")
        mapperDefaults = pexPolicy.Policy.createPolicy(mapperDict,
                mapperDict.getRepositoryPath())
        defaultPolicy = pexPolicy.DefaultPolicyFile("daf_persistence",
                "cfhtDefaults.paf", "policy")
        cfhtDefaults = pexPolicy.Policy.createPolicy(defaultPolicy,
                defaultPolicy.getRepositoryPath())
        if policy is None:
            self.policy = pexPolicy.Policy()
        else:
            self.policy = policy
        self.policy.mergeDefaults(cfhtDefaults)
        self.policy.mergeDefaults(mapperDefaults)

        for key in ["root", "calibrationRoot", "calibrationDb", "rawTemplate",
                "registry", "datatypePolicy", "registryPolicy"]:
            # Explicit arguments override policy
            value = None
            if rest.has_key(key):
                value = rest[key]
            elif self.policy.exists(key):
                value = self.policy.get(key)
            setattr(self, key, value)

        if self.calibrationDb is not None and \
                os.path.split(self.calibrationDb)[0] == '':
            self.calibrationDb = os.path.join(self.root, self.calibrationDb)
        if self.calibrationDb is not None:
            self.calibDb = CalibDb(self.calibrationDb)
        else:
            self.calibDb = None

        registryPolicy = self.policy.getPolicy("registryPolicy")
        if self.registry is None:
            self.registry = Registry.create(self.root, registryPolicy)
        else:
            self.registry = Registry.create(self.registry, registryPolicy)

        self.cache = {}
        self.butler = None
        self.metadataCache = {}

    def keys(self):
        return ["field", "obsid", "exposure", "ccd", "amp", "filter",
                "expTime", "skyTile"]

    def getCollection(self, datasetType, keys, dataId):
        if datasetType == "raw":
            return self.registry.getCollection(keys, dataId)
        dateTime = self.metadataForDataId(dataId).get('taiObs')
        ccd = "CCD009"
        if dataId.has_key("ccd"):
            ccd = dataId['ccd']
        amp = 1
        if dataId.has_key("amp"):
            amp = dataId['amp']
        filter = None
        if dataId.has_key("filter"):
            filter = dataId['filter']
        expTime = None
        if dataId.has_key("expTime"):
            expTime = dataId['expTime']
        calibs = self.calibDb.lookup(dateTime, datasetType,
                ccd, amp, filter, expTime, all=True)
        result = []
        for c in calibs:
            if len(keys) == 1:
                result.append(getattr(c, k))
            else:
                tuple = []
                for k in keys:
                    tuple.append(getattr(c, k))
                result.append(tuple)
        return result

    def map_raw(self, dataId):
        path = self.root
        path = os.path.join(path, self.rawTemplate % dataId)
        return ButlerLocation(
                "lsst.afw.image.DecoratedImageU", "DecoratedImageU",
                "FitsStorage", path, dataId)

    def map_bias(self, dataId):
        dateTime = self.metadataForDataId(dataId).get('taiObs')
        path = self.calibDb.lookup(dateTime, 'bias',
                dataId['ccd'], dataId['amp'], None, 0)
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.afw.image.ExposureF", "ExposureF",
                "FitsStorage", path, dataId)

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
                "lsst.afw.image.ExposureF", "ExposureF",
                "FitsStorage", path, dataId)

    def map_defect(self, dataId):
        dateTime = self.metadataForDataId(dataId).get('taiObs')
        path = self.calibDb.lookup(dateTime, 'defect',
                dataId['ccd'], dataId['amp'], None)
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.pex.policy.Policy", "Policy",
                "PafStorage", path, dataId)

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
                "lsst.afw.image.ExposureF", "ExposureF",
                "FitsStorage", path, dataId)

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
                "lsst.afw.image.ExposureF", "ExposureF",
                "FitsStorage", path, dataId)

    def map_linearize(self, dataId):
        path = self.calibDb.lookup(None, 'linearize')
        path = os.path.join(self.calibrationRoot, path)
        return ButlerLocation(
                "lsst.pex.policy.Policy", "Policy",
                "PafStorage", path, dataId)

    def metadataForDataId(self, dataId):
        if self.metadataCache.has_key(dataId['obsid']):
            return self.metadataCache[dataId['obsid']]
        if self.butler is None:
            bf = ButlerFactory(inputMapper=self)
            self.butler = bf.create()
        internalId = {}
        internalId.update(dataId)
        if not internalId.has_key('exposure'):
            exposures = self.butler.getCollection('raw', 'exposure',
                    internalId)
            internalId['exposure'] = exposures[0]
        if not internalId.has_key('ccd'):
            ccds = self.butler.getCollection('raw', 'ccd', internalId)
            internalId['ccd'] = ccds[0]
        if not internalId.has_key('amp'):
            amps = self.butler.getCollection('raw', 'amp', internalId)
            internalId['amp'] = amps[0]
        image = self.butler.get('raw', internalId)
        metadata = image.getMetadata()
        self.metadataCache[dataId['obsid']] = metadata
        return metadata

    def std_raw(self, item):
        try:
            metadata = item.getMetadata()
        except:
            return item
        datatypePolicy = self.datatypePolicy
        metadataPolicy = datatypePolicy.getPolicy("metadataPolicy")
        paramNames = metadataPolicy.paramNames(1)
        for paramName in paramNames:
            if metadata.exists(paramName):
                continue
            keyword = metadataPolicy.getString(paramName)
            if metadata.typeOf(keyword) == dafBase.PropertySet.TYPE_String:
                val = metadata.getString(keyword).strip()
                if paramName == "datasetId" and val.find(' ') > 0:
                    val = val[:val.index(' ')]
                metadata.set(paramName, val)
            else:
                metadata.copy(paramName, metadata, keyword)
                metadata.copy(keyword+"_original", metadata, keyword)
                metadata.remove(keyword)
        if datatypePolicy.exists('convertDateobsToTai') and \
                datatypePolicy.getBool('convertDateobsToTai'):
            dateObs = metadata.getDouble('dateObs')
            dateTime = dafBase.DateTime(dateObs, dafBase.DateTime.MJD,
                    dafBase.DateTime.UTC)
            dateObs = dateTime.get(dafBase.DateTime.MJD, dafBase.DateTime.TAI)
            metadata.setDouble('dateObs', dateObs)
        if datatypePolicy.exists('convertDateobsToMidExposure') and \
                datatypePolicy.getBool('convertDateobsToMidExposure'):
            dateObs += metadata.getDouble('expTime') * 0.5 / 3600. / 24.
            metadata.setDouble('dateObs', dateObs)
            dateTime = dafBase.DateTime(metadata.getDouble('dateObs'),
                    dafBase.DateTime.MJD)
            metadata.setDateTime('taiObs', dateTime)
        if datatypePolicy.exists('trimFilterName') and \
                datatypePolicy.getBool('trimFilterName'):
            filter = metadata.getString('filter')
            filter = re.sub(r'\..*', '', filter)
            metadata.setString('filter', filter)
        if datatypePolicy.exists('convertVisitIdToInt') and \
                datatypePolicy.getBool('convertVisitIdToInt'):
            visitId = metadata.getString('visitId')
            metadata.setInt('visitId', int(visitId))

        item.setMetadata(metadata)
        return item
