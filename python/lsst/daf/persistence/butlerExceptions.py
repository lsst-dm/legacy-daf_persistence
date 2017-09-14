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
from builtins import str, super


class NoMapperException(Exception):
    pass


class NoResults(RuntimeError):

    def __init__(self, message, datasetType, dataId):
        message += ' datasetType:' + datasetType + ' dataId:' + str(dataId)
        super().__init__(message)


class MultipleResults(RuntimeError):

    def __init__(self, message, datasetType, dataId, locations):
        message += ' datasetType:' + datasetType + ' dataId:' + str(dataId) + ' locations:'
        for location in locations:
            message += ' ' + str(location)
        super().__init__(message)
        self.locations = locations


class ParentsMismatch(RuntimeError):
    """Raised when issues arise related to the list of parents in a RepositoryCfg not matching the expected
    value.
    """
    def __init__(self, message):
        super().__init__(message)
