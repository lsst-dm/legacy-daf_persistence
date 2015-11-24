#
# LSST Data Management System
#
# Copyright 2008-2015 AURA/LSST.
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
# see <https://www.lsstcorp.org/LegalNotices/>.
#
"""
Utilities for safe file IO
"""
import errno
import os
import tempfile
from contextlib import contextmanager

def safeMakeDir(directory):
    """Make a directory in a manner avoiding race conditions"""
    if directory != "" and not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError, e:
            # Don't fail if directory exists due to race
            if e.errno != errno.EEXIST:
                raise e

def setFileMode(filename):
    """Set a file mode according to the user's umask"""
    # Get the current umask, which we can only do by setting it and then reverting to the original.
    umask = os.umask(0o077)
    os.umask(umask)
    # chmod the new file to match what it would have been if it hadn't started life as a temporary
    # file (which have more restricted permissions).
    os.chmod(filename, (~umask & 0o666))

@contextmanager
def SafeFile(name):
    """Context manager to create a file in a manner avoiding race conditions

    The context manager provides a temporary file object. After the user is done,
    we move that file into the desired place and close the fd to avoid resource
    leakage.
    """
    outDir, outName = os.path.split(name)
    safeMakeDir(outDir)
    with tempfile.NamedTemporaryFile(dir=outDir, prefix=outName, delete=False) as temp:
        try:
            yield temp
        finally:
            os.rename(temp.name, name)
            setFileMode(name)

@contextmanager
def SafeFilename(name):
    """Context manager for creating a file in a manner avoiding race conditions

    The context manager provides a temporary filename with no open file descriptors
    (as this can cause trouble on some systems). After the user is done, we move the
    file into the desired place.
    """
    outDir, outName = os.path.split(name)
    safeMakeDir(outDir)
    temp = tempfile.NamedTemporaryFile(dir=outDir, prefix=outName, delete=False)
    tempName = temp.name
    temp.close() # We don't use the fd, just want a filename
    try:
        yield tempName
    finally:
        os.rename(tempName, name)
        setFileMode(name)
