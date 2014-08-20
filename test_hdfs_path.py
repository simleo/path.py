"""
Test the hdfs_path module.
"""

import unittest, uuid

import pydoop.hdfs as hdfs
from hdfs_path import HdfsPath as path


def make_random_str(prefix='hdfs_path_', postfix=''):
    return '%s%s%s' % (prefix, uuid.uuid4().hex, postfix)


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

    def test_splitpath(self):
        parent, _ = path(self.wd).splitpath()
        self.assertTrue(isinstance(parent, path))


def suite():
    loader = unittest.TestLoader()
    s = unittest.TestSuite()
    s.addTests([loader.loadTestsFromTestCase(_) for _ in (
        TestHdfsPath,
        )])
    return s


def main():
    tests = suite()
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(tests)


if __name__ == '__main__':
    main()
