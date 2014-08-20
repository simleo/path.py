"""
An object representing an HDFS path to a file or directory.
"""

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

    # --- TODO: add more methods
