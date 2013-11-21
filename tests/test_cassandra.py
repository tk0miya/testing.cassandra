# -*- coding: utf-8 -*-

import os
import signal
import unittest
import testing.cassandra
from mock import patch
from time import sleep
import pycassa


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
    def testing(self, listdir):
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
        data_dir = os.path.join(os.path.dirname(__file__), 'copy-data-from')
        cassandra = testing.cassandra.Cassandra(copy_data_from=data_dir)

        # connect to mysql
        conn = pycassa.pool.ConnectionPool('test', cassandra.server_list())
        values = pycassa.ColumnFamily(conn, 'hello').get('score')

        self.assertEqual({'scott': '1', 'tiger': '2'}, values)
