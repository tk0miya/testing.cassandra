# -*- coding: utf-8 -*-
#  Copyright 2013 Takeshi KOMIYA
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import re
import yaml
import socket
import pycassa
from glob import glob
from shutil import copyfile, copytree

from testing.common.database import (
    Database, SkipIfNotInstalledDecorator, get_unused_port
)

__all__ = ['Cassandra', 'skipIfNotInstalled', 'skipIfNotFound']

SEARCH_PATHS = ['/usr/local/cassandra',
                '/usr/local/apache-cassandra',
                '/usr/local/opt/cassandra']


class Cassandra(Database):
    DEFAULT_SETTINGS = dict(auto_start=2,
                            base_dir=None,
                            cassandra_home=None,
                            pid=None,
                            copy_data_from=None)
    subdirectories = ['conf', 'commitlog', 'data', 'saved_caches', 'tmp']

    def initialize(self, **kwargs):
        self.cassandra_home = self.settings.get('cassandra_home')
        if self.cassandra_home is None:
            self.cassandra_home = find_cassandra_home()

        self.cassandra_bin = self.settings.get('cassandra_bin')
        if self.cassandra_bin is None:
            self.cassandra_bin = os.path.join(self.cassandra_home, 'bin', 'cassandra')

        with open(os.path.join(self.cassandra_confdir, 'cassandra.yaml')) as fd:
            self.cassandra_yaml = yaml.load(fd.read())
            self.cassandra_yaml['commitlog_directory'] = os.path.join(self.base_dir, 'commitlog')
            self.cassandra_yaml['data_file_directories'] = [os.path.join(self.base_dir, 'data')]
            self.cassandra_yaml['saved_caches_directory'] = os.path.join(self.base_dir, 'saved_caches')

            cassandra_version = strip_version(self.cassandra_home)
            if cassandra_version is None or cassandra_version > (1, 2):
                self.cassandra_yaml['start_rpc'] = True

            for key, value in self.settings.get('cassandra_yaml', {}):
                self.settings['cassandra_yaml'][key] = value

        if self.settings['auto_start']:
            if os.path.exists(self.pid_file):
                raise RuntimeError('cassandra is already running (%s)' % self.pid_file)

    @property
    def pid_file(self):
        return os.path.join(self.base_dir, 'tmp', 'cassandra.pid')

    @property
    def cassandra_confdir(self):
        path = os.path.join(self.cassandra_home, 'conf')
        if os.path.exists(path):
            return path
        elif os.path.exists('/usr/local/etc/cassandra'):  # Homebrew
            return '/usr/local/etc/cassandra'
        else:
            raise RuntimeError("could not find confdir of cassandra")

    def server_list(self):
        hostname = '127.0.0.1:%d' % self.cassandra_yaml['rpc_port']
        return [hostname]

    def get_data_directory(self):
        return os.path.join(self.base_dir, 'data')

    def initialize_database(self):
        # conf directory
        orig_dir = os.path.join(self.cassandra_confdir)
        conf_dir = os.path.join(self.base_dir, 'conf')
        for filename in os.listdir(os.path.join(orig_dir)):
            srcpath = os.path.join(orig_dir, filename)
            destpath = os.path.join(conf_dir, filename)
            if not os.path.exists(destpath):
                if filename == 'log4j-server.properties':
                    logpath = os.path.join(self.base_dir, 'tmp', 'system.log')
                    with open(srcpath) as src:
                        with open(destpath, 'w') as dest:
                            property = re.sub('log4j.appender.R.File=.*',
                                              'log4j.appender.R.File=%s' % logpath,
                                              src.read())
                            dest.write(property)
                elif os.path.isdir(srcpath):
                    copytree(srcpath, destpath)
                else:
                    copyfile(srcpath, destpath)

    def get_server_commandline(self):
        return [self.cassandra_bin, '-f']

    def is_server_available(self):
        try:
            sock = socket.create_connection(('127.0.0.1', self.cassandra_yaml['rpc_port']), 1.0)
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            return True
        except:
            return False

    def prestart(self):
        os.environ['CASSANDRA_CONF'] = os.path.join(self.base_dir, 'conf')

        # assign ports to cassandra
        config_keys = ['rpc_port', 'storage_port', 'ssl_storage_port', 'native_transport_port']
        for key in config_keys:
            if key in self.cassandra_yaml:
                self.cassandra_yaml[key] = get_unused_port()

        # replace cassandra-env.sh
        with open(os.path.join(self.base_dir, 'conf', 'cassandra-env.sh'), 'r+t') as fd:
            script = re.sub('JMX_PORT="7199"', 'JMX_PORT="%d"' % get_unused_port(), fd.read())
            fd.seek(0)
            fd.write(script)

        # generate cassandra.yaml
        with open(os.path.join(self.base_dir, 'conf', 'cassandra.yaml'), 'wt') as fd:
            fd.write(yaml.dump(self.cassandra_yaml))

    def poststart(self):
        # create test keyspace
        conn = pycassa.system_manager.SystemManager(self.server_list()[0])
        try:
            conn.create_keyspace('test', pycassa.SIMPLE_STRATEGY, {'replication_factor': '1'})
        except pycassa.InvalidRequestException:
            pass
        conn.close()


class CassandraSkipIfNotInstalledDecorator(SkipIfNotInstalledDecorator):
    name = 'Cassandra'

    def search_server(self):
        find_cassandra_home()  # raise exception if not found


skipIfNotFound = skipIfNotInstalled = CassandraSkipIfNotInstalledDecorator()


def strip_version(dir):
    m = re.search('(\d+)\.(\d+)\.(\d+)', dir)
    if m is None:
        return None
    else:
        return tuple([int(ver) for ver in m.groups()])


def find_cassandra_home():
    cassandra_home = os.environ.get('CASSANDRA_HOME')
    if cassandra_home and os.path.exists(os.path.join(cassandra_home, 'bin', 'cassandra')):
        return cassandra_home

    for dir in SEARCH_PATHS:
        if os.path.exists(os.path.join(dir, 'bin', 'cassandra')):
            return dir

    # search newest cassandra-x.x.x directory
    cassandra_dirs = [dir for dir in glob("/usr/local/*cassandra*") if os.path.isdir(dir)]
    if cassandra_dirs:
        return sorted(cassandra_dirs, key=strip_version)[-1]

    raise RuntimeError("could not find CASSANDRA_HOME")
