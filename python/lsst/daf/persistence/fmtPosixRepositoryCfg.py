#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
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

import copy
import fcntl
import yaml
import os
import urllib
from . import PosixStorage, RepositoryCfg, safeFileIo, ParentsMismatch
from lsst.log import Log


class RepositoryCfgPosixFormatter():

    @classmethod
    def write(cls, cfg, butlerLocation):
        """Serialize a RepositoryCfg to a location.

        When the location is the same as cfg.root, the RepositoryCfg is to be written at the root location of
        the repository. In that case, root is not written in the serialized cfg; it is implicit in the
        location of the cfg. This allows the cfg to move from machine to machine without modification.

        Parameters
        ----------
        cfg : RepositoryCfg instance
            The RepositoryCfg to be serialized.
        butlerLocation : ButlerLocation
            The location to write the RepositoryCfg.
        """
        def setRoot(cfg, loc):
            loc = os.path.split(loc)[0]  # remove the `repoistoryCfg.yaml` file name
            if loc is None or cfg.root == loc:
                cfg = copy.copy(cfg)
                loc = cfg.root
                cfg.root = None
            return cfg

        loc = butlerLocation.storage.root

        log = Log.getLogger("daf.persistence.butler")
        if loc is None:
            loc = cfg.root
        # This class supports schema 'file' and also treats no schema as 'file'.
        # Split the URI and take only the path; remove the schema fom loc if it's there.
        parseRes = urllib.parse.urlparse(loc)
        loc = parseRes.path
        if not os.path.exists(loc):
            os.makedirs(loc)
        loc = os.path.join(loc, butlerLocation.getLocations()[0])
        try:
            with safeFileIo.FileForWriteOnceCompareSame(loc) as f:
                cfgToWrite = setRoot(cfg, loc)
                yaml.dump(cfgToWrite, f)
                cfg.dirty = False
        except safeFileIo.FileForWriteOnceCompareSameFailure:
            with open(loc, 'r') as fileForRead:
                log.debug("Acquiring blocking exclusive lock on {}", loc)
                fcntl.flock(fileForRead, fcntl.LOCK_EX)
                existingCfg = RepositoryCfgPosixFormatter._read(fileForRead, parseRes.path)
                try:
                    existingCfg.extend(cfg)
                except ParentsMismatch as e:
                    raise RuntimeError("Can not extend existing repository cfg because: {}".format(e))
                with open(loc, 'w') as fileForWrite:
                    cfgToWrite = setRoot(cfg, loc)
                    yaml.dump(cfg, fileForWrite)
                    cfg.dirty = False
                log.debug("Releasing blocking exclusive lock on {}", loc)

    @classmethod
    def _read(cls, fileObject, uri):
        """Get a persisted RepositoryCfg from an open file object.

        Parameters
        ----------
        fileObject : an open file object
            the file that contains the RepositoryCfg.

        Returns
        -------
        A RepositoryCfg instance or None
        """
        repositoryCfg = yaml.load(fileObject)
        if repositoryCfg.root is None:
            repositoryCfg.root = uri
        return repositoryCfg

    @classmethod
    def read(cls, butlerLocation):
        repositoryCfg = None
        loc = butlerLocation.storage.root
        fileLoc = os.path.join(loc, butlerLocation.getLocations()[0])
        if os.path.exists(fileLoc):
            log = Log.getLogger("daf.persistence.butler")
            with open(fileLoc, 'r') as fileForRead:
                log.debug("Acquiring blocking shared lock on {}", loc)
                fcntl.flock(fileForRead, fcntl.LOCK_SH)
                repositoryCfg = RepositoryCfgPosixFormatter._read(fileForRead, loc)
                log.debug("Releasing blocking exclusive lock on {}", loc)
        return repositoryCfg


PosixStorage.registerFormatter(RepositoryCfg, RepositoryCfgPosixFormatter)
