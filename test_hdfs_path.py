"""
Test the hdfs_path module.
"""

import os, unittest, uuid

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


class TestHdfsPath(unittest.TestCase):

    def setUp(self):
        wd = make_random_str()
        with hdfs.hdfs('default', 0) as fs:
            fs.create_directory(wd)
            self.wd = fs.get_path_info(wd)['name']  # full URI

    def tearDown(self):
        with hdfs.hdfs('default', 0) as fs:
            fs.delete(self.wd)

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


def suite():
    loader = unittest.TestLoader()
    s = unittest.TestSuite()
    s.addTests([loader.loadTestsFromTestCase(_) for _ in (
        TestPurePath,
        TestHdfsPath,
        )])
    return s


def main():
    tests = suite()
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(tests)


if __name__ == '__main__':
    main()
