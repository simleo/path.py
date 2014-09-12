"""
Test the hdfs_path module.
"""

import os, unittest, uuid, hashlib, numbers

import pydoop.hdfs as hdfs
from hdfs_path import HdfsPath as path


def make_random_str(prefix='hdfs_path_', postfix=''):
    return '%s%s%s' % (prefix, uuid.uuid4().hex, postfix)


# purely computational operations, no I/O
class TestPurePath(unittest.TestCase):

    def setUp(self):
        self.root_str = 'hdfs://host:1/'
        self.root = path(self.root_str)

    def test_add(self):
        more = 'foo'
        self.assertEqual(self.root + more, path(self.root_str + more))
        self.assertEqual(more + self.root, path(more + self.root_str))

    def test_div_join(self):
        r, a, b = self.root, 'a', 'b'
        exp_res = path('%s%s/%s' % (r, a, b))
        for joined in r / a / b, path.joinpath(r, a, b):
            self.assertEqual(joined, exp_res)

    def test_normcase(self):
        self.assertEqual(self.root.normcase(), self.root)

    def test_normpath(self):
        p = self.root / 'a' / '..' / 'b'
        exp_res = self.root / 'b'
        self.assertEqual(p.normpath(), exp_res)

    def test_realpath(self):
        # support is currently limited, just perform a minimal check
        self.assertTrue(isinstance(self.root.realpath(), path))

    def test_expanduser(self):
        self.assertEqual(path('~foo/a').expanduser(), path('/user/foo/a'))

    def test_expandvars(self):
        k, v = make_random_str(), make_random_str()
        try:
            os.environ[k] = v
            template = path('%s${%s}' % (self.root_str, k))
            exp_res = self.root / v
            self.assertEqual(template.expandvars(), exp_res)
        finally:
            try:
                del os.environ[k]
            except KeyError:
                pass

    def test_split_related(self):
        bn = 'a.x.y'
        exp_nb, exp_ext = hdfs.path.splitext(bn)
        p = self.root / bn
        self.assertEqual(p.splitpath(), (self.root, bn))
        for n in p.dirname(), p.parent:
            self.assertEqual(n, self.root)
        for n in p.basename(), p.name:
            self.assertEqual(n, bn)
        self.assertEqual(p.name, bn)
        self.assertEqual(p.namebase, exp_nb)
        stripped, ext = p.splitext()
        for n in ext, p.ext:
            self.assertEqual(n, exp_ext)
        for n in stripped, p.stripext():
            self.assertEqual(n, self.root / exp_nb)

    def test_splitall(self):
        parts = r, a, b = self.root, 'foo', 'bar'
        self.assertEqual(tuple((r / a / b).splitall()), parts)


class ConcretePathFixture(object):

    def setUp(self):
        self.wd_bn = make_random_str()
        with hdfs.hdfs('default', 0) as fs:
            fs.create_directory(self.wd_bn)
            self.wd = fs.get_path_info(self.wd_bn)['name']  # full URI

    def tearDown(self):
        with hdfs.hdfs('default', 0) as fs:
            fs.delete(self.wd)


class TestHdfsPath(ConcretePathFixture, unittest.TestCase):

    def test_cwd(self):
        p = path('%s/%s' % (self.wd, make_random_str()))
        old_cwd = p.getcwd()
        self.assertTrue(isinstance(old_cwd, path))
        p.chdir()
        self.assertEqual(p.cwd, p)
        p.chdir(self.wd)
        self.assertEqual(p.cwd, self.wd)
        p.cwd = old_cwd
        self.assertEqual(p.getcwd(), old_cwd)
        p.close()

    def test_context_mgmt(self):
        with path('%s/%s' % (self.wd, make_random_str())) as p:
            self.assertEqual(p.cwd, p)

    def test_abspath(self):
        p = path(self.wd_bn).abspath()
        self.assertTrue(hdfs.path.isfull(p))

    def test_relpath(self):
        a = path(self.wd)
        b0, b1 = a / 'b0', a / 'b1'
        c = b0 / 'c'
        #--
        self.assertEqual(a.relpathto(c), path('b0') / 'c')
        self.assertEqual(c.relpathto(a), path('..') / '..')
        self.assertEqual(b1.relpathto(c), path('..') / 'b0' / 'c')
        self.assertEqual(c.relpathto(b1), path('..') / '..' / 'b1')
        self.assertEqual(a.relpathto(a), path('.'))
        # unreachable dest
        other_root = path('hdfs://foo:1/')
        self.assertEqual(a.relpathto(other_root), other_root)
        self.assertEqual(other_root.relpathto(a), a)
        # relpath()
        self.assertEqual(a.relpath(), a.cwd.relpathto(a))
        self.assertEqual(c.relpath(a), a.relpathto(c))

    def test_listdir(self):
        dnames = ['foo', 'bar', 'tar']
        fnames = ['%s.ext' % _ for _ in dnames]
        d = path(self.wd)
        for n in dnames:
            hdfs.mkdir(d / n)
        for n in fnames:
            hdfs.dump('TEXT\n', d / n)
        nset = lambda seq: set(_.name for _ in seq)
        self.assertEqual(nset(d.listdir()), set(dnames + fnames))
        self.assertEqual(nset(d.listdir('f*')), set([dnames[0], fnames[0]]))
        self.assertEqual(nset(d.dirs('*ar')), set(dnames[1:]))
        self.assertEqual(nset(d.files('*ar*')), set(fnames[1:]))
        self.assertRaises(OSError, (d / fnames[0]).listdir)

    def test_walk(self):
        root = path(self.wd)
        a = root / 'a'
        b0, b1 = [root / ('b%d' % _) for _ in xrange(2)]
        c = b0 / 'c'
        all_dirs = set([a, b0, b1, c])
        all_files = set([_ / 'foo.ext' for _ in all_dirs])
        for p in all_dirs:
            hdfs.mkdir(p)
        for p in all_files:
            hdfs.dump('TEXT\n', p)
        self.assertEqual(set(root.walk()), all_dirs | all_files)
        self.assertEqual(set(root.walkdirs()), all_dirs)
        self.assertEqual(set(root.walkfiles()), all_files)


class TestIO(ConcretePathFixture, unittest.TestCase):

    def setUp(self):
        super(TestIO, self).setUp()
        self.chunks = ['0123\r\n', '4567\r\n', '89\r\n']
        self.content = ''.join(self.chunks)
        self.p = path(self.wd) / 'foo'
        with self.p.open('w') as f:
            f.write(self.content)
        self.hash = hashlib.md5()
        self.hash.update(self.content)

    def test_bytes(self):
        self.assertEqual(self.p.bytes(), self.content)

    def test_chunks(self):
        size = len(self.chunks[0])
        self.assertEqual(list(self.p.chunks(size)), self.chunks)

    def test_write_bytes(self):
        clone = self.p + '_copy'
        clone.write_bytes(self.content)
        self.assertEqual(clone.bytes(), self.content)
        # TODO: add test for append=True

    def test_text(self):
        self.assertEqual(self.p.text(), self.content.replace('\r', ''))

    def test_write_text(self):
        clone = self.p + '_copy'
        clone.write_text(self.content, linesep='\n')
        self.assertEqual(clone.bytes(), self.content.replace('\r', ''))

    def test_lines(self):
        lines = [_.replace('\r', '') for _ in self.chunks]
        self.assertEqual(self.p.lines(), lines)

    def test_write_lines(self):
        clone = self.p + '_copy'
        clone.write_lines(self.chunks, linesep='\n')
        self.assertEqual(clone.bytes(), self.content.replace('\r', ''))

    def test_hash(self):
        for h in self.p.read_md5(), self.p.read_hash('md5'):
            self.assertEqual(h, self.hash.digest())
        self.assertEqual(self.p.read_hexhash('md5'), self.hash.hexdigest())


class TestFSQuery(ConcretePathFixture, unittest.TestCase):

    def setUp(self):
        super(TestFSQuery, self).setUp()
        self.d = path(self.wd)
        self.p = self.d / 'foo'
        self.same_p = self.d / 'bar' / '..' / 'foo'
        self.content = 'foo\n'
        with self.p.open('w') as f:
            f.write(self.content)

    def test_isabs(self):
        for p in self.d, self.p:
            self.assertTrue(p.isabs())
        self.assertFalse(path('foo').isabs())

    def test_exists(self):
        for p in self.d, self.p:
            self.assertTrue(p.exists())
        self.assertFalse(path(make_random_str()).exists())

    def test_is_something(self):
        for test in self.d.isdir(), self.p.isfile():
            self.assertTrue(test)
        for test in self.p.isdir(), self.d.isfile():
            self.assertFalse(test)
        for p in self.d, self.p:
            for test in p.islink(), p.ismount():
                self.assertFalse(test)

    def test_samefile(self):
        self.assertTrue(self.p.samefile(self.same_p))

    def test_stat_related(self):
        for s in self.p.stat().st_size, self.p.getsize(), self.p.size:
            self.assertEqual(s, len(self.content))
        # minimal tests for get*time()
        for c in 'amc':
            meth = getattr(self.p, 'get%stime' % c)
            prop = getattr(self.p, '%stime' % c)
            self.assertTrue(isinstance(meth(), numbers.Number))
            self.assertTrue(isinstance(prop, numbers.Number))

    def test_access(self):
        for p in self.d, self.p:
            self.assertTrue(p.access(os.W_OK))


class TestMod(ConcretePathFixture, unittest.TestCase):

    def setUp(self):
        super(TestMod, self).setUp()
        self.d = path(self.wd)
        self.p = self.d / 'foo'
        hdfs.dump('foo\n', self.p)

    def test_utime(self):
        new_at, new_mt = self.p.atime - 100, self.p.mtime - 50
        self.p.utime((new_at, new_mt))
        self.assertEqual(self.p.atime, new_at)
        self.assertEqual(self.p.mtime, new_mt)


def suite():
    loader = unittest.TestLoader()
    s = unittest.TestSuite()
    s.addTests([loader.loadTestsFromTestCase(_) for _ in (
        TestPurePath,
        TestHdfsPath,
        TestIO,
        TestFSQuery,
        TestMod,
        )])
    return s


def main():
    tests = suite()
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(tests)


if __name__ == '__main__':
    main()
