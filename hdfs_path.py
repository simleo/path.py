"""
An object representing an HDFS path to a file or directory.
"""

import os, errno

import pydoop.hdfs as hdfs
import pydoop.hdfs.path as hpath
import path as path_mod


class HdfsPath(path_mod.path):
    """
    Represents an HDFS path.
    """
    module = hpath

    def __init__(self, other=''):
        super(HdfsPath, self).__init__(other=other)
        self.__fs = None

    def close(self):
        """
        Close this path's connection to the filesystem.
        """
        if self.__fs is not None:
            self.__fs.close()

    @property
    def fs(self):
        """
        The filesystem instance this path belongs to.
        """
        if self.__fs is None:
            host, port, _ = self.module.split(self)
            self.__fs = hdfs.hdfs(host, port)
        return self.__fs

    # not a classmethod, working directory is fs-specific
    def getcwd(self):
        """
        Current working directory, as a path object.
        """
        return self._next_class(self.fs.working_directory(decode=True))

    def chdir(self, path=None):
        """
        Change the current working directory to the specified path, or
        to this path if the path argument is None.
        """
        if path is None:
            path = self
        host, port, _ = self.module.split(path)
        with hdfs.hdfs(host, port) as fs:
            if fs.host != self.fs.host or fs.port != self.fs.port:
                raise RuntimeError('trying to chdir across filesystems')
            self.fs.set_working_directory(path)  # always succeeds

    cwd = property(getcwd, chdir)

    @classmethod
    def _always_unicode(cls, s):
        if isinstance(s, unicode):
            return s
        return s.decode('utf-8')

    def isfull(self):
        return self.module.isfull(self)

    def __enter__(self):
        self._old_dir = self.getcwd()
        self.chdir()
        return self

    def __exit__(self, *_):
        self.chdir(path=self._old_dir)
        self.close()

    def splitpath(self):
        """
        Return ``(p.parent, p.name)``.
        """
        parent, child = self.module.splitpath(self)
        return self._next_class(parent), child

    def relpath(self, start=None):
        """
        Return a relative path from start to this path, where start
        defaults to this path's current working directory.
        """
        if start is None:
            start = self.cwd
        else:
            start = self._next_class(start)
        return start.relpathto(self)

    def listdir(self, pattern=None):
        """
        Return the list of items in this directory as path objects.

        If pattern is given, it is used as a Unix shell-style wildcard
        pattern that item names must match in order to be returned.
        """
        if pattern is None:
            pattern = '*'
        if not self.isdir():
            self.__oserror(errno.ENOTDIR)
        ls = [hdfs.path.basename(_) for _ in hdfs.ls(self)]
        return [self / _ for _ in map(self._always_unicode, ls)
                if self._next_class(_).fnmatch(pattern)]

    # --- TODO: add more methods

    # utilities
    def __oserror(self, code, name=None):
        if name is None:
            name = self
        raise OSError(code, os.strerror(code), name)
