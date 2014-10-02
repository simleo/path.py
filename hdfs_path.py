"""
An object representing an HDFS path to a file or directory.
"""

import os, errno, re, codecs

import pydoop.hdfs as hdfs
import pydoop.hdfs.path as hpath
# FIXME: set readline_chunk_size=None in pydoop.hdfs.open
from pydoop.hdfs.common import BUFSIZE

import path as path_mod
PY3 = path_mod.PY3
U_NEWLINE = path_mod.U_NEWLINE


_MODE_PATTERN = re.compile(r'[rwaU]')

def sanitize_mode(mode):  # FIXME: fix this in Pydoop
    try:
        mode = _MODE_PATTERN.findall(mode)[0]
    except IndexError:
        raise ValueError('invalid mode string %r' % (mode,))
    return 'r' if mode == 'U' else mode  # FIXME: don't just ignore 'U'


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
        return self._next_class(self.fs.working_directory())

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

    def glob(self, pattern):
        raise NotImplementedError  # FIXME

    def open(self, mode="r", buff_size=0, replication=0, blocksize=0,
             readline_chunk_size=BUFSIZE, user=None):
        return hdfs.open(self, mode=sanitize_mode(mode), buff_size=buff_size,
                         replication=replication, blocksize=blocksize,
                         readline_chunk_size=readline_chunk_size, user=user)

    def text(self, encoding=None, errors='strict'):
        """
        Open file and return its contents as text, with all newline
        characters converted to ``'\n'``.

        The ``encoding`` and ``errors`` parameters work as in
        :func:`codecs.open`.
        """
        if encoding is None:
            encoding = 'ascii'
        with self.open('rb') as f:
            info = codecs.lookup(encoding)
            f = codecs.StreamReaderWriter(
                f, info.streamreader, info.streamwriter, errors
            )
            return U_NEWLINE.sub('\n', f.read())

    def lines(self, encoding=None, errors='strict', retain=True):
        """
        Open file and return its contents as a list of text lines.

        The ``encoding`` and ``errors`` parameters work as in
        :func:`codecs.open`.  If ``retain`` is set to :obj:`True`,
        newline characters are kept, after converting them to ``'\n'``.
        """
        return self.text(encoding, errors).splitlines(retain)

    def stat(self):
        """
        Perform the equivalent of ``os.stat`` on this path
        """
        return hdfs.stat(self)

    def lstat(self):
        """
        Perform the equivalent of ``os.lstat`` on this path
        """
        return hdfs.lstat(self)

    def access(self, mode, user=None):
        return hdfs.access(self, mode, user=user)

    def get_owner(self):
        return self.stat().st_uid

    def statvfs(self):
        raise NotImplementedError  # FIXME

    def pathconf(self):
        raise NotImplementedError  # FIXME

    def utime(self, times=None, user=None):
        hdfs.utime(self, times=times, user=user)

    def chmod(self, mode, user=None):
        hdfs.chmod(self, mode, user=user)

    # TODO: support numeric uid/gid when fs is the local fs
    def chown(self, user=None, group=None, hdfs_user=None):
        hdfs.chown(self, user=user, group=group, hdfs_user=hdfs_user)

    def rename(self, new, user=None):
        hdfs.rename(self, new, user=user)
        return self._next_class(new)

    def renames(self, new, user=None):
        hdfs.renames(self, new, user=user)
        return self._next_class(new)

    def makedirs(self, mode=0o777, user=None):
        hdfs.mkdir(self, user=user)

    def mkdir(self, mode=0o777, user=None, recursive=False, can_exist=False):
        if not can_exist and self.exists():
            self.__oserror(errno.EEXIST)
        if not recursive:
            where = self.parent or self.cwd
            if not where.exists():
                self.__oserror(errno.ENOENT)
        hdfs.mkdir(self, user=user)
        hdfs.chmod(self, mode)

    def mkdir_p(self, mode=0o777, user=None):
        self.mkdir(mode=mode, user=user, can_exist=True)

    def makedirs(self, mode=0o777, user=None):
        self.mkdir(mode=mode, user=user, recursive=True)

    def makedirs_p(self, mode=0o777, user=None):
        self.mkdir(mode=mode, user=user, can_exist=True, recursive=True)

    # utilities
    def __oserror(self, code, name=None):
        if name is None:
            name = self
        raise OSError(code, os.strerror(code), name)
