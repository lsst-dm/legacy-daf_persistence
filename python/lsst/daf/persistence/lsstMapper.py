#!/usr/bin/env python

import os
import re
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import Registry, ButlerFactory, ButlerLocation, Mapper, CalibDb
import lsst.daf.base as dafBase
import lsst.pex.exceptions as pexExcept

class LsstMapper(Mapper):
    def __init__(self, policy=None, **rest):
        Mapper.__init__(self)

        mapperDict = pexPolicy.DefaultPolicyFile("daf_persistence",
                "LsstMapperDictionary.paf", "policy")
        mapperDefaults = pexPolicy.Policy.createPolicy(mapperDict,
                mapperDict.getRepositoryPath())
        if policy is None:
            self.policy = pexPolicy.Policy()
        else:
            self.policy = policy
        self.policy.mergeDefaults(mapperDefaults)

        for key in ["root", "registry"]:
            # Explicit arguments override policy
            value = None
            if rest.has_key(key):
                value = rest[key]
            elif self.policy.exists(key):
                value = self.policy.get(key)
            setattr(self, key, value)

#         if self.registry is None:
#             self.registry = Registry.create(self.root)
#         else:
#             self.registry = Registry.create(self.registry)

        self.cache = {}
        self.butler = None
        self.metadataCache = {}

    def keys(self):
        return ["visit", "snap", "ccd", "amp", "filter", "skyTile"]

    def getCollection(self, dataSetType, keys, dataId):
        # TODO -- postIsr, ccd, visIm, icSrc, sci, sfmSrc, tmpl, diff,
        # diaSrc, movingObj, movingSrc, deep, chiSq, det, astroModel, newObj,
        # forcedDiaSrc, forcedSrc, multifitObj, finalObj
        return []

    def map_postIsr(self, dataId):
        path = os.path.join(root, self.templates['postIsr'] % dataId)
        return ButlerLocation(
                "lsst.afw.image.ExposureF", "ExposureF",
                [("FitsStorage", path)], dataId)

    def map_ccd(self, dataId):
        path = os.path.join(root, self.templates['ccd'] % dataId)
        return ButlerLocation(
                "lsst.afw.image.ExposureF", "ExposureF",
                [("FitsStorage", path)], dataId)

    def map_visIm(self, dataId):
        path = os.path.join(root, self.templates['visIm'] % dataId)
        return ButlerLocation(
                "lsst.afw.image.ExposureF", "ExposureF",
                [("FitsStorage", path)], dataId)

    def map_icSrc(self, dataId):
        path = os.path.join(root, self.templates['icSrc'] % dataId)
        return ButlerLocation(
                "lsst.afw.detection.SourceSet", "SourceSet",
                [("BoostStorage", path)], dataId)

    def map_sci(self, dataId):
        path = os.path.join(root, self.templates['sci'] % dataId)
        return ButlerLocation(
                "lsst.afw.image.ExposureF", "ExposureF",
                [("FitsStorage", path)], dataId)

    def map_sfmSrc(self, dataId):
        path = os.path.join(root, self.templates['sfmSrc'] % dataId)
        return ButlerLocation(
                "lsst.afw.detection.SourceSet", "SourceSet",
                [("BoostStorage", path)], dataId)

    def map_tmpl(self, dataId):
        path = os.path.join(root, self.templates['tmpl'] % dataId)
        return ButlerLocation(
                "lsst.afw.image.ExposureF", "ExposureF",
                [("FitsStorage", path)], dataId)

    def map_diff(self, dataId):
        path = os.path.join(root, self.templates['diff'] % dataId)
        return ButlerLocation(
                "lsst.afw.image.ExposureF", "ExposureF",
                [("FitsStorage", path)], dataId)

    def map_diaSrc(self, dataId):
        path = os.path.join(root, self.templates['diaSrc'] % dataId)
        return ButlerLocation(
                "lsst.afw.detection.DiaSourceSet", "DiaSourceSet",
                [("BoostStorage", path)], dataId)

    def map_movingObj(self, dataId):
        self._unimplemented()

    def map_movingSrc(self, dataId):
        self._unimplemented()

    def map_deep(self, dataId):
        self._unimplemented()

    def map_chiSq(self, dataId):
        self._unimplemented()

    def map_det(self, dataId):
        self._unimplemented()

    def map_astroModel(self, dataId):
        self._unimplemented()

    def map_newObj(self, dataId):
        self._unimplemented()

    def map_forcedDiaSrc(self, dataId):
        self._unimplemented()

    def map_forcedSrc(self, dataId):
        self._unimplemented()

    def map_multifitObj(self, dataId):
        self._unimplemented()

    def map_finalObj(self, dataId):
        self._unimplemented()
