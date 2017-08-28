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
import errno
import yaml
import os
import urllib
from . import PosixStorage, RepositoryCfg, safeFileIo, ParentsMismatch


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

        # This class supports schema 'file' and also treats no schema as 'file'.
        # Split the URI and take only the path; remove the schema from loc if it's there.
        loc = butlerLocation.storage.root
        parseRes = urllib.parse.urlparse(loc if loc is not None else cfg.root)
        loc = os.path.join(parseRes.path, butlerLocation.getLocations()[0])
        try:
            with safeFileIo.SafeLockedFileForRead(loc) as f:
                existingCfg = RepositoryCfgPosixFormatter._read(f, parseRes.path)
                if existingCfg == cfg:
                    cfg.dirty = False
                    return
        except IOError as e:
            if e.errno != errno.ENOENT:  # ENOENT is 'No such file or directory'
                raise e
        with safeFileIo.SafeLockedFileForWrite(loc) as f:
            existingCfg = RepositoryCfgPosixFormatter._read(f, parseRes.path)
            if existingCfg is None:
                cfgToWrite = setRoot(cfg, loc)
            else:
                if existingCfg == cfg:
                    cfg.dirty = False
                    return
                try:
                    existingCfg.extend(cfg)
                    cfgToWrite = setRoot(existingCfg, loc)
                except ParentsMismatch as e:
                    raise RuntimeError("Can not extend existing repository cfg because: {}".format(e))
            yaml.dump(cfgToWrite, f)
            cfg.dirty = False

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
        if repositoryCfg is not None:
            if repositoryCfg.root is None:
                repositoryCfg.root = uri
        return repositoryCfg

    @classmethod
    def read(cls, butlerLocation):
        repositoryCfg = None
        loc = butlerLocation.storage.root
        fileLoc = os.path.join(loc, butlerLocation.getLocations()[0])
        try:
            with safeFileIo.SafeLockedFileForRead(fileLoc) as f:
                repositoryCfg = RepositoryCfgPosixFormatter._read(f, loc)
        except IOError as e:
            if e.errno != errno.ENOENT:  # ENOENT is 'No such file or directory'
                raise e
        return repositoryCfg


PosixStorage.registerFormatter(RepositoryCfg, RepositoryCfgPosixFormatter)
