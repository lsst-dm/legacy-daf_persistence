#!/usr/bin/env python

import datetime
import glob
import os
import re
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import ButlerLocation, Mapper
import lsst.daf.base as dafBase
import lsst.pex.exceptions as pexExcept

class Registry(object):
    def __init__(self):
        pass

    def create(location):
        if re.match(r'.*\.registry', location):
            return FileRegistry(location)
        # if re.match(r'.*\.sqlite3', location):
        #     return SqliteRegistry(location)
        if re.match(r'mysql:', location):
            return DbRegistry(location)
        return FsRegistry(location)

class FsRegistry(Registry):
    fileRegExps = {
        "raw": r'raw-(?P<obsid>\d+)-e(?P<exposure>\d+)-c(?P<ccd>\d+)-a(?P<amp>\d+)\.fits'
    }
    fields = "D1 D2 D3 D4 W1 W2 W3 W4".split()

    def __init__(self, location):
        Registry.__init__(self)
        pathList = glob.glob(os.path.join(location, "*", "*", "raw-*.fits"))
        # TODO -- analyze pathList

    def getCollection(self, keys, dataId):
        mappedFields = set()
        for k in dataId.keys():
            if not k in fieldList:
                mappedFields += k
        keySet = set()
        for tuple in cache.tuples:
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

class DbRegistry(Registry):
    def __init__(self, location):
        Registry.__init__(self)
        # TODO -- initialize registry
        pass

    def getCollection(self, keys, dataId):
        # TODO -- select distinct keys from registry
        pass

class FileRegistry(Registry):
    def __init__(self, location):
        Registry.__init__(self)
        pass

    def getCollection(self, keys, dataId):
        pass

# class SqliteRegistry(Registry):
#     def __init__(self, location):
#         Registry.__init__(self)
#         # TODO -- initialize registry
#         pass
# 
#     def getCollection(self, keys, dataId):
#         # TODO -- select distinct keys from registry
#         pass


class CfhtMapper(Mapper):
    def __init__(self, policy=None, **rest):
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

        for key in ["root", "calibrationRoot", "calibrationDb", "rawTemplate",
                "registry"]:
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
            self.calibDb = CalibDB(self.calibrationDb)
        else:
            self.calibDb = None

        if registry is None:
            self.registry = FsRegistry(root)
        else:
            self.registry = Registry.create(self.registry)

        self.cache = {}
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

    def getCollection(self, dataSetType, keys, dataId):
        if dataSetType == "raw":
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
        calibs = self.calibDb.lookup(dateTime, dataSetType,
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

class CalibData(object):
    """Contain what we know about calibration data"""

    def __init__(self, exposureName, version, validFrom, validTo, expTime=0, filter=None):
        self.exposureName = exposureName
        self.version = version
        self.expTime = expTime
        self.validFrom = validFrom
        self.validTo = validTo
        self.filter = filter

def needExpTime(calibType):
    return calibType in ("bias", "dark")

def needFilter(calibType):
    return calibType in ("flat", "fringe")

def DateTimeFromIsoStr(str, scale=dafBase.DateTime.TAI):
    """Convert a format of the form 2003-07-20T23:12:19.00Z (n.b. no fractional seconds)"""
    yearStr, timeStr = str.split("T")
    year, month, day = [int(x) for x in yearStr.split("-")]
    timeStr = re.sub(r"(\.0+)?Z$", "", timeStr)

    hr, min, sec = [int(x) for x in timeStr.split(":")]

    return dafBase.DateTime(year, month, day, hr, min, sec, scale)

class CalibDB(object):
    """A class to find the proper calibration files for a given type of calibration"""

    def __init__(self, calibDatabasePaf):
        """Read calibration file in calibDatabasePaf"""

        self.calibDatabasePaf = calibDatabasePaf

        try:
            self.calibPolicy = pexPolicy.Policy(self.calibDatabasePaf)
        except pexExcept.LsstCppException, e:
            raise "Failed to read %s: %s" % (self.calibDatabasePaf, e)

    def lookup(self, lsstDateTime, calibType, CCD="CCD009", amplifier=1, filter=None, expTime=None,
               all=False, nothrow=False):
        """Find the  proper calibration given an lsst::daf::data::DateTime, a calib type, a CCD and an amplifier; if appropriate, a filter may also be specified

Calibrations are only valid for a range of times (special case:  if the times are equal, it is
assumed that the files are always valid)

Valid calibTypes are bias, dark, defect, flat, fringe, and linearize

If you specify all=True, return a list of all CalibData objects that matching your desired.

If nothrow is true, return None if nothing is available
"""
        if isinstance(CCD, int):
            CCD = "CCD%03d" % CCD
        if isinstance(amplifier, int):
            amplifier = "Amplifier%03d" % amplifier

        if calibType not in ("bias", "dark", "defect", "flat", "fringe", "linearize"):
            raise RuntimeError, ("Unknown calibration type: %s" % calibType)
        #
        # A placeholder
        #
        if calibType == "linearize":
            return "linearizationLookupTable.paf"

        if calibType == "bias":
            if expTime:
                raise RuntimeError, ("You may not specify an expTime for a bias: %s" % expTime)
            expTime = 0

        if not all:
            if needExpTime(calibType) and expTime is None:
                raise RuntimeError, ("Please specify an expTime for your %s" % (calibType))

            if needFilter(calibType) and not filter:
                raise RuntimeError, ("Please specify a filter for your %s" % (calibType))

        try:
            returnVals = []
            for calib in self.calibPolicy.getPolicy("calibrations").getPolicy(CCD).getPolicy(amplifier).getArray(calibType):
                validTo = DateTimeFromIsoStr(calib.get("validTo"))
                validFrom = DateTimeFromIsoStr(calib.get("validFrom"))

                if validFrom.nsecs() == validTo.nsecs() or validFrom.nsecs() <= lsstDateTime.nsecs() < validTo.nsecs():
                    if needExpTime(calibType):
                        if all:
                            if expTime and calib.get("expTime") != expTime:
                                continue
                        else:
                            if calib.get("expTime") != expTime:
                                continue

                    if needFilter(calibType):
                        if all:
                            if filter and calib.get("filter") != filter:
                                continue
                        else:
                            if calib.get("filter") != filter:
                                continue

                    if all:
                        _expTime, _filter = None, None
                        try:
                            _expTime = calib.get("expTime")
                        except:
                            pass

                        try:
                            _filter = calib.get("filter")
                        except:
                            pass

                        returnVals.append(
                            CalibData(calib.get("exposureName"), calib.get("version"),
                                      calib.get("validFrom"), calib.get("validTo"),
                                      expTime=_expTime, filter=_filter))
                    else:
                        exposureName = calib.get("exposureName")

                        return exposureName

            if all:
                return returnVals
            else:
                pass                # continue to an exception

        except IndexError, e:
            pass
        except TypeError, e:
            pass
        except pexExcept.LsstCppException, e:
            pass

        ctype = calibType
        if needExpTime(calibType):
            ctype += " %s" % expTime
        if needFilter(calibType):
            ctype += " %s" % filter

        if nothrow:
            return None
        else:
            raise RuntimeError, "Unable to locate %s for %s %s for %s" % (ctype, CCD, amplifier,
                                                                          datetime.datetime.fromtimestamp(int(lsstDateTime.nsecs()/1e9)).strftime("%Y-%m-%dT%H:%M:%SZ"))
        
