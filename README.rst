``testing.cassandra`` automatically setups a cassandra instance in a temporary directory, and destroys it after testing

Install
=======
Use easy_install (or pip)::

   $ easy_install testing.cassandra

And ``testing.cassandra`` requires Cassandra server.


Usage
=====
Create Cassandra instance using ``testing.cassandra.Cassandra``::

  import pycassa
  import testing.cassandra

  # Launch new Cassandra server
  with testing.cassandra.Cassandra as cassandra:
      conn = pycassa.pool.ConnectionPool('test', cassandra.server_list())
      #
      # do any tests using Cassandra...
      #

  # Cassandra server is terminated here


``testing.cassandra`` automatically searchs for cassandra files in ``/usr/local/``.
If you install cassandra to other directory, set ``cassandra_home`` keyword::

  # uses a copy of specified data directory of Cassandra.
  cassandra = testing.cassandra.Cassandra(copy_data_from='/path/to/your/database')


``testing.cassandra.Cassandra`` executes ``cassandra`` on instantiation.
On deleting Cassandra object, it terminates Cassandra instance and removes temporary directory.

If you want a database including column families and any fixtures for your apps,
use ``copy_data_from`` keyword::

  # uses a copy of specified data directory of Cassandra.
  cassandra = testing.cassandra.Cassandra(copy_data_from='/path/to/your/database')


You can specify parameters for Cassandra with ``cassandra_yaml`` keyword::

  # boot Cassandra server listens on 12345 port
  cassandra = testing.cassandra.Cassandra(cassandra_yaml={'rpc_port': 12345})


For example, you can setup new Cassandra server for each testcases on setUp() method::

  import unittest
  import testing.cassandra

  class MyTestCase(unittest.TestCase):
      def setUp(self):
          self.cassandra = testing.cassandra.Cassandra()

      def tearDown(self):
          self.cassandra.stop()


Requirements
============
* Cassandra 1.1, 1.2
* Python 2.6, 2.7
* pycassa
* PyYAML


License
=======
Apache License 2.0


History
=======

1.1.0 (2013-12-06)
-------------------
* Support Cassandra 1.2.x
* Support python 2.6
* Add mysqld.skipIfNotInstalled decorator
* Change behavior: Cassandra#stop() cleans workdir
* Fix workdir does not removed if cassandra does not started

1.0.0 (2013-10-17)
-------------------
* First release
