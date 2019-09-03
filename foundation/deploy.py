#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fabric import Connection
from os import listdir

# TODO
# - Render template
# - Zookeeper custom start method


class Manager:
    def __init__(self, conn_props, opts):
        self.conn = Connection(**conn_props)
        self.opts = opts

    def deploy(self):
        self.pull_image()
        self.launch_container()
        self.configure()

    def pull_image(self):
        self.conn.run(f"docker pull {self.opts['container_image']}")

    def launch_container(self):
        self.conn.run('docker run -tid '
                      # Network
                      + '--net=host '
                      # Name
                      + f"--name {self.opts['container_name']} "
                      # Image
                      + f"{self.opts['container_image']}")

    def stop_container(self):
        self.conn.run(f"docker stop {self.opts['container_name']}")

    def remove_container(self):
        self.conn.run(f"docker rm -f {self.opts['container_name']}")

    def render_template(self):
        pass

    def configure(self):
        self._make_host_tmp_path()
        self._copy_files_to_host()
        self._copy_files_to_container()
        self._remove_files_from_host()

    def _make_host_tmp_path(self):
        self.conn.run(f"mkdir -p {self.opts['host_conf_path']}")

    def _copy_files_to_host(self):
        for file in listdir(self.opts['local_conf_path']):
            self.conn.put(f"{self.opts['local_conf_path']}{file}",
                          remote=self.opts['host_conf_path'])

    def _copy_files_to_container(self):
        self.conn.run(
            f"docker cp {self.opts['host_conf_path']}zoo.cfg {self.opts['container_name']}:{self.opts['container_conf_path']}"
        )

    def _remove_files_from_host(self):
        self.conn.run(f"rm -rf {self.opts['host_conf_path']}")

    def start(self):
        raise NotImplementedError


class ZookeeperManager(Manager):
    def start(self):
        pass


conn_props = {
    'host': '127.0.0.1',
    'port': 22,
    'user': 'brunneis',
    'connect_kwargs': {
        "key_filename": "id_rsa"
    }
}

opts = {
    'container_image': 'catenae/zookeeper',
    'container_name': 'ancoris-zookeeper',
    'local_conf_path': 'conf/zookeeper/',
    'container_conf_path': '/opt/zookeeper/default/conf/',
    'host_conf_path': '/tmp/ancoris/zookeeper/'
}

manager = ZookeeperManager(conn_props, opts)
manager.deploy()
manager.start()