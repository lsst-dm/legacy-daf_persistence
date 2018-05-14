#
# LSST Data Management System
# Copyright 2008-2018 AURA/LSST.
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

"""Common tasks and idioms performed with the Butler.
"""

__all__ = ["dataExists", "searchDataRefs"]


def searchDataRefs(butler, datasetType, level="", dataId=None):
    """Find all data references for a partial data ID.

    Parameters
    ----------
    butler: `lsst.daf.persistence.Butler`
        The repository to query for data.
    datasetType : `str`
        The type of data references to return.
    level : `str`
        The level of data ID at which to search. If the empty string, the
        default level for ``datasetType`` shall be used.
    dataId : `lsst.daf.persistence.DataRef`, or `dict` from `str` to any
        Butler identifier naming the data to be retrieved. If ommitted, an
        unrestricted data ID shall be used.

    Returns
    -------
    dataRefs : iterable of `lsst.daf.persistence.ButlerDataRef`
        Complete data references matching ``dataId``. Only references to
        existing data shall be returned.
    """
    if dataId is None:
        dataId = {}

    refList = butler.subset(datasetType=datasetType, level=level, dataId=dataId)
    # exclude nonexistent data
    # this is a recursive test, e.g. for the sake of "raw" data
    return [dr for dr in refList if dataExists(dr)]


def dataExists(dataRef):
    """Determine if data exists at the current level or any data exists at a deeper level.

    Parameters
    ----------
    dataRef : `lsst.daf.persistence.ButlerDataRef`
        Data reference to test for existence.

    Returns
    -------
    exists : `bool`
        Return value is `True` if data exists, `False` otherwise.
    """
    subDRList = dataRef.subItems()
    if subDRList:
        for subDR in subDRList:
            if dataExists(subDR):
                return True
        return False
    else:
        return dataRef.datasetExists()
