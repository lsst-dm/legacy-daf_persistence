#!/usr/bin/env python

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

import os
import urlparse
import yaml

from lsst.daf.persistence import PosixStorage

class Storage(object):
    """Interface class for storages"""

    @staticmethod
    def getRepositoryCfg(uri):
        ret = None
        parseRes = urlparse.urlparse(uri)
        if parseRes.scheme == '' or parseRes.scheme == 'file':
            ret = PosixStorage.getRepositoryCfg(uri)
        return ret

    @staticmethod
    def putRepositoryCfg(cfg, loc=None):
        ret = None
        uri = loc if loc is not None else cfg.dataRoot
        parseRes = urlparse.urlparse(uri)
        if parseRes.scheme == '' or parseRes.scheme == 'file':
            PosixStorage.putRepositoryCfg(cfg, loc)

    @staticmethod
    def getMapperClass(uri):
        ret = None
        parseRes = urlparse.urlparse(uri)
        if parseRes.scheme == '' or parseRes.scheme == 'file':
            ret = PosixStorage.getMapperClass(uri)
        return ret

    @staticmethod
    def makeFromURI(uri):
        '''Instantiate from a URI.
        In come cases the storageCfg may have already been instantiated into a Repository, this is allowed and
        the input var is simply returned.

        .. warning::

            makeFromURI is 'wet paint' and very likely to change. Use of it in production code other than via 
            the 'old butler' API is strongly discouraged.


        :param uri: the uri to a locaiton that contains a repositoryCfg.
        :return: a Storage instance. Exactly what type depends on how the cfg was made.
        '''
        parseRes = urlparse.urlparse(uri)
        if parseRes.scheme == '' or parseRes.scheme == 'file':
            return PosixStorage(uri)


