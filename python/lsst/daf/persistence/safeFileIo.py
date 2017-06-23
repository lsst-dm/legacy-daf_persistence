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
from contextlib import contextmanager
import errno
import fcntl
import filecmp
import os
import tempfile
from lsst.log import Log


class DoNotWrite(RuntimeError):
    pass


def safeMakeDir(directory):
    """Make a directory in a manner avoiding race conditions"""
    if directory != "" and not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
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


class FileForWriteOnceCompareSameFailure(RuntimeError):
    pass


@contextmanager
def FileForWriteOnceCompareSame(name):
    """Context manager to get a file that can be written only once and all other writes will succeed only if
    they match the inital write.

    The context manager provides a temporary file object. After the user is done, the temporary file becomes
    the permanent file if the file at name does not already exist. If the file at name does exist the
    temporary file is compared to the file at name. If they are the same then this is good and the temp file
    is silently thrown away. If they are not the same then a runtime error is raised.
    """
    outDir, outName = os.path.split(name)
    safeMakeDir(outDir)
    temp = tempfile.NamedTemporaryFile(mode="w", dir=outDir, prefix=outName, delete=False)
    try:
        yield temp
    finally:
        try:
            temp.close()
            # If the symlink cannot be created then it will raise. If it can't be created because a file at
            # 'name' already exists then we'll do a compare-same check.
            os.symlink(temp.name, name)
            # If the symlink was created then this is the process that created the first instance of the
            # file, and we know its contents match. Move the temp file over the symlink.
            os.rename(temp.name, name)
            # At this point, we know the file has just been created. Set permissions according to the
            # current umask.
            setFileMode(name)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e
            filesMatch = filecmp.cmp(temp.name, name, shallow=False)
            os.remove(temp.name)
            if filesMatch:
                # if the files match then the compare-same check succeeded and we can silently return.
                return
            else:
                # if the files do not match then the calling code was trying to write a non-matching file over
                # the previous file, maybe it's a race condition? In any event, raise a runtime error.
                raise FileForWriteOnceCompareSameFailure("Written file does not match existing file.")


@contextmanager
def SafeFile(name):
    """Context manager to create a file in a manner avoiding race conditions

    The context manager provides a temporary file object. After the user is done,
    we move that file into the desired place and close the fd to avoid resource
    leakage.
    """
    outDir, outName = os.path.split(name)
    safeMakeDir(outDir)
    doWrite = True
    with tempfile.NamedTemporaryFile(mode="w", dir=outDir, prefix=outName, delete=False) as temp:
        try:
            yield temp
        except DoNotWrite:
            doWrite = False
        finally:
            if doWrite:
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
    temp = tempfile.NamedTemporaryFile(mode="w", dir=outDir, prefix=outName, delete=False)
    tempName = temp.name
    temp.close()  # We don't use the fd, just want a filename
    try:
        yield tempName
    finally:
        os.rename(tempName, name)
        setFileMode(name)


@contextmanager
def SafeLockedFileForRead(name):
    """Context manager for reading a file that may be locked with an exclusive lock via
    SafeLockedFileForWrite.

    This first tries to acquire the lock without blocking. If that fails then it will take a blocking lock
    on the file. However, because SafeLockedFileForWrite uses a temporary file and then replaces the old file
    with the temporary file, the fd that the blocking lock is against may no longer be valid when it unblocks.
    To work around that, this will close the (possibly) old file and try again from the beginning by trying to
    acquire the lock without blocking.

    Parameters
    ----------
    name : string
        The file name to be opened, may include path.

    Yields
    ------
    file object
        The file to be read from.
    """
    log = Log.getLogger("daf.persistence.butler")
    try:
        while True:
            try:
                with open(name, 'r') as f:
                    log.debug("Acquiring non-blocking shared lock on {}", name)
                    fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    yield f
                    break
            except IOError as e:
                if e.errno == errno.EAGAIN:
                    with open(name, 'r') as f:
                        log.debug("Acquiring blocking shared lock on {}", name)
                        fcntl.flock(f, fcntl.LOCK_SH)
                else:
                    raise e
    finally:
        log.trace("Releasing blocking shared lock on {}", name)


class _RWFile:
    """File-like object that is used to contain readable and writable files to be yielded by
    SafeLockedFileForWrite
    """
    def __init__(self, readable, writable):
        self.readable = readable
        self.writeable = writable
        self.dirty = False

    def read(self, size=None):
        if size is not None:
            return self.readable.read(size)
        return self.readable.read()

    def write(self, str):
        self.writeable.write(str)
        self.dirty = True


@contextmanager
def SafeLockedFileForWrite(name):
    """Context manager to write a file in a manner that prevents other files from reading the old file while
    the new one is being written. It opens the named file (creating it if needed) and locks it. It then
    returns a file-like object that reads from the named file and writes to a temporary file. This allows a
    file to atomically be opened, inspected, and changed if needed. After the user is done, if a write
    operation has been performed we move the temporary file into the desired place and close the fd.

    Parameters
    ----------
    name : string
        The file name to be opened, may include path.

    Yields
    ------
    file-like object
        The file to be read from and written to.
    """
    log = Log.getLogger("daf.persistence.butler")
    safeMakeDir(os.path.split(name)[0])
    try:
        with open(name, 'a+') as writeFile:
            log.debug("Acquiring blocking exclusive lock on {}", name)
            fcntl.flock(writeFile, fcntl.LOCK_EX)
            with open(name, 'r') as readFile:
                with SafeFile(name) as safeFile:
                    rwFile = _RWFile(readFile, safeFile)
                    yield rwFile
                    if rwFile.dirty is False:
                        raise DoNotWrite
    finally:
        log.debug("Released blocking exclusive lock on {}", name)
