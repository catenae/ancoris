#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fabric import Connection
from os import listdir
import yaml
import asyncio
from jinja2 import Environment, FileSystemLoader


class ClusterManager:
    def __init__(self, service_name, node_manager_class):
        with open(f'conf/{service_name}.yaml', 'r') as input_file:
            self.opts = yaml.safe_load(input_file)['opts']

        with open('conf/nodes.yaml', 'r') as input_file:
            self.global_opts = yaml.safe_load(input_file)
            nodes = self.global_opts['services'][service_name]['nodes']
            self.opts['nodes'] = nodes

        conn_props = {
            'port': 22,
            'user': self.global_opts['user'],
            'connect_kwargs': {
                "key_filename": self.global_opts['keyfile']
            }
        }

        self.node_managers = []
        for index, node in enumerate(self.opts['nodes']):
            conn_props['host'] = list(node.values())[0]['address']
            conn = Connection(**conn_props)

            node_opts = dict(self.opts)
            node_opts['index'] = index + 1
            node_manager = node_manager_class(node_opts, conn)
            self.node_managers.append(node_manager)

    def deploy(self):
        asyncio.run(self._deploy_parallel())

    async def _deploy_parallel(self):
        tasks = []
        for node_manager in self.node_managers:
            task = asyncio.create_task(node_manager.deploy())
            tasks.append(task)
        for task in tasks:
            await task

class NodeManager:
    def __init__(self, opts, conn):
        self.opts = opts
        self.conn = conn

    async def deploy(self):
        # Remove this
        try:
            self.destroy()
        except Exception:
            pass


        self.pull()
        self.run()
        self.configure()
        self.start()

    def pull(self):
        self.conn.run(f"docker pull {self.opts['container_image']}")

    def run(self):
        self.conn.run('docker run -tid '
                      # Network
                      + '--net=host '
                      # Name
                      + f"--name {self.opts['container_name']} "
                      # Volume
                      + f"--volume {self.opts['host_data_path']}:{self.opts['container_data_path']} "
                      # Image
                      + f"{self.opts['container_image']}")

    def stop(self):
        self.conn.run(f"docker stop {self.opts['container_name']}")

    def destroy(self):
        self.conn.run(f"docker rm -f {self.opts['container_name']}")

    def configure(self):
        self._render_templates()
        self._make_host_tmp_path()
        self._copy_files_to_host()
        self._copy_files_to_container()
        self._remove_files_from_host()
    
    def _render_templates(self):
        for template_filename in listdir(self.opts['local_conf_path']):
            if not '.template' in template_filename:
                continue
            conf_filename = template_filename.split('.template')[0]
            env = Environment(loader=FileSystemLoader(self.opts['local_conf_path']))
            with open(f"{self.opts['local_conf_path']}{conf_filename}", 'w') as output_file:
                output_file.write(env.get_template(template_filename).render(self.opts))

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


class ZookeeperClusterManager(ClusterManager):
    def __init__(self):
        super().__init__('zookeeper', ZookeeperNodeManager)


class ZookeeperNodeManager(NodeManager):
    def start(self):
        self.conn.run(f"docker exec {self.opts['container_name']} zkServer.sh start")

    def add_myid_file(self):
        myid = self.opts['index']
        self.conn.run(f"docker exec {self.opts['container_name']} bash -c 'echo {myid} > {self.opts['data_dir']}/myid'")

    def configure(self):
        super().configure()
        self.add_myid_file()


ZookeeperClusterManager().deploy()