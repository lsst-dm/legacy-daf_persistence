#!/usr/bin/env python

import re
import lsst.pex.policy as pexPolicy
import lsst.daf.base as dafBase
import lsst.pex.exceptions as pexExcept

class CalibData(object):
    """Contain what we know about calibration data"""

    def __init__(self, exposureName, version, validFrom, validTo, expTime=0, filter=None):
        self.exposureName = exposureName
        self.version = version
        self.expTime = expTime
        self.validFrom = validFrom
        self.validTo = validTo
        self.filter = filter

def _needExpTime(calibType):
    return calibType in ("bias", "dark")

def _needFilter(calibType):
    return calibType in ("flat", "fringe")

class CalibDb(object):
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
            if _needExpTime(calibType) and expTime is None:
                raise RuntimeError, ("Please specify an expTime for your %s" % (calibType))

            if _needFilter(calibType) and not filter:
                raise RuntimeError, ("Please specify a filter for your %s" % (calibType))

        try:
            returnVals = []
            for calib in self.calibPolicy.getPolicy("calibrations").getPolicy(CCD).getPolicy(amplifier).getArray(calibType):
                validTo = dafBase.DateTime(calib.get("validTo"))
                validFrom = dafBase.DateTime(calib.get("validFrom"))

                if validFrom.nsecs() == validTo.nsecs() or validFrom.nsecs() <= lsstDateTime.nsecs() < validTo.nsecs():
                    if _needExpTime(calibType):
                        if all:
                            if expTime and calib.get("expTime") != expTime:
                                continue
                        else:
                            if calib.get("expTime") != expTime:
                                continue

                    if _needFilter(calibType):
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
        if _needExpTime(calibType):
            ctype += " %s" % expTime
        if _needFilter(calibType):
            ctype += " %s" % filter

        if nothrow:
            return None
        else:
            raise RuntimeError, "Unable to locate %s for %s %s for %s" % (ctype, CCD, amplifier, lsstDateTime.toString())
