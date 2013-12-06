# -*- coding: utf-8 -*-

import os
import sys
import signal
import pycassa
import subprocess
import testing.cassandra
from mock import patch
from time import sleep

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest


class TestCassandra(unittest.TestCase):
    def test_basic(self):
        # start cassandra server
        cassandra = testing.cassandra.Cassandra()
        self.assertIsNotNone(cassandra)
        self.assertEqual(cassandra.server_list(),
                         ['127.0.0.1:%d' % cassandra.cassandra_yaml['rpc_port']])

        # connect to cassandra
        conn = pycassa.pool.ConnectionPool('test', cassandra.server_list())
        self.assertIsNotNone(conn)

        # shutting down
        pid = cassandra.pid
        self.assertTrue(pid)
        os.kill(pid, 0)  # process is alive

        cassandra.stop()
        sleep(1)

        self.assertIsNone(cassandra.pid)
        with self.assertRaises(OSError):
            os.kill(pid, 0)  # process is down

    def test_with_cassandra(self):
        with testing.cassandra.Cassandra() as cassandra:
            self.assertIsNotNone(cassandra)

            # connect to cassandra
            conn = pycassa.pool.ConnectionPool('test', cassandra.server_list())
            self.assertIsNotNone(conn)

            pid = cassandra.pid
            os.kill(pid, 0)  # process is alive

        self.assertIsNone(cassandra.pid)
        with self.assertRaises(OSError):
            os.kill(pid, 0)  # process is down

    def test_multiple_cassandra(self):
        cassandra1 = testing.cassandra.Cassandra()
        cassandra2 = testing.cassandra.Cassandra()
        self.assertNotEqual(cassandra1.pid, cassandra2.pid)

        os.kill(cassandra1.pid, 0)  # process is alive
        os.kill(cassandra2.pid, 0)  # process is alive

    @patch("testing.cassandra.os.listdir")
    def test_cassandra_is_not_found(self, listdir):
        listdir.return_value = []
        with self.assertRaises(RuntimeError):
            testing.cassandra.Cassandra()

    def test_fork(self):
        cassandra = testing.cassandra.Cassandra()
        if os.fork() == 0:
            del cassandra
            cassandra = None
            os.kill(os.getpid(), signal.SIGTERM)  # exit tests FORCELY
        else:
            os.wait()
            sleep(1)
            self.assertTrue(cassandra.pid)
            os.kill(cassandra.pid, 0)  # process is alive (delete mysqld obj in child does not effect)

    def test_stop_on_child_process(self):
        cassandra = testing.cassandra.Cassandra()
        if os.fork() == 0:
            cassandra.stop()
            self.assertTrue(cassandra.pid)
            os.kill(cassandra.pid, 0)  # process is alive (calling stop() is ignored)
            os.kill(os.getpid(), signal.SIGTERM)  # exit tests FORCELY
        else:
            os.wait()
            sleep(1)
            self.assertTrue(cassandra.pid)
            os.kill(cassandra.pid, 0)  # process is alive (calling stop() in child is ignored)

    def test_copy_data_from(self):
        cassandra_home = testing.cassandra.find_cassandra_home()
        args = [os.path.join(cassandra_home, 'bin', 'cassandra'), '-v']

        stdout, _ = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        version = stdout.splitlines()[-1].strip()

        # use copy-data-from/{version}
        data_dir = os.path.join(os.path.dirname(__file__), 'copy-data-from/%s' % version[:3])
        cassandra = testing.cassandra.Cassandra(copy_data_from=data_dir)

        # connect to mysql
        conn = pycassa.pool.ConnectionPool('test', cassandra.server_list())
        values = pycassa.ColumnFamily(conn, 'hello').get('score')

        self.assertEqual({'scott': '1', 'tiger': '2'}, values)

    def test_skipIfNotInstalled_found(self):
        @testing.cassandra.skipIfNotInstalled
        def testcase():
            pass

        self.assertEqual(False, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(False, hasattr(testcase, '__unittest_skip_why__'))

    @patch("testing.cassandra.glob")
    def test_skipIfNotInstalled_notfound(self, glob):
        glob.side_effect = []

        try:
            search_paths = testing.cassandra.SEARCH_PATHS
            testing.cassandra.SEARCH_PATHS = []

            @testing.cassandra.skipIfNotInstalled
            def testcase():
                pass

            self.assertEqual(True, hasattr(testcase, '__unittest_skip__'))
            self.assertEqual(True, hasattr(testcase, '__unittest_skip_why__'))
            self.assertEqual(True, testcase.__unittest_skip__)
            self.assertEqual("Cassandra does not found", testcase.__unittest_skip_why__)
        finally:
            testing.cassandra.SEARCH_PATHS = search_paths

    def test_skipIfNotInstalled_with_args_found(self):
        cassandra_home = testing.cassandra.find_cassandra_home()
        path = os.path.join(cassandra_home, 'bin', 'cassandra')

        @testing.cassandra.skipIfNotInstalled(path)
        def testcase():
            pass

        self.assertEqual(False, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(False, hasattr(testcase, '__unittest_skip_why__'))

    def test_skipIfNotInstalled_with_args_notfound(self):
        @testing.cassandra.skipIfNotInstalled("/path/to/anywhere")
        def testcase():
            pass

        self.assertEqual(True, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(True, hasattr(testcase, '__unittest_skip_why__'))
        self.assertEqual(True, testcase.__unittest_skip__)
        self.assertEqual("Cassandra does not found", testcase.__unittest_skip_why__)

    def test_skipIfNotFound_found(self):
        @testing.cassandra.skipIfNotFound
        def testcase():
            pass

        self.assertEqual(False, hasattr(testcase, '__unittest_skip__'))
        self.assertEqual(False, hasattr(testcase, '__unittest_skip_why__'))

    @patch("testing.cassandra.glob")
    def test_skipIfNotFound_notfound(self, glob):
        glob.side_effect = []

        try:
            search_paths = testing.cassandra.SEARCH_PATHS
            testing.cassandra.SEARCH_PATHS = []

            @testing.cassandra.skipIfNotFound
            def testcase():
                pass

            self.assertEqual(True, hasattr(testcase, '__unittest_skip__'))
            self.assertEqual(True, hasattr(testcase, '__unittest_skip_why__'))
            self.assertEqual(True, testcase.__unittest_skip__)
            self.assertEqual("Cassandra does not found", testcase.__unittest_skip_why__)
        finally:
            testing.cassandra.SEARCH_PATHS = search_paths
