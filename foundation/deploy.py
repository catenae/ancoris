#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fabric import Connection
from os import listdir
from os.path import isfile, isdir
import yaml
import asyncio
from jinja2 import Environment, FileSystemLoader


class ClusterManager:
    def __init__(self, service_name, node_manager_class):
        with open(f'conf/{service_name}.yaml', 'r') as input_file:
            self.opts = yaml.safe_load(input_file)['opts']


        with open('conf/nodes.yaml', 'r') as input_file:
            self.opts['global'] = yaml.safe_load(input_file)

        conn_props = {
            'port': 22,
            'user': self.opts['global']['user'],
            'connect_kwargs': {
                "key_filename": self.opts['global']['keyfile']
            }
        }

        self.node_managers = []
        for index, node in enumerate(self.opts['global']['services'][service_name]['nodes']):
            conn_props['host'] = node['address']
            conn = Connection(**conn_props)

            node_opts = dict(self.opts)
            node_opts['index'] = index
            node_opts['id'] = index + 1
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
        for template_path in self._get_all_template_paths():
            self._render_single_template(template_path)

    def _get_all_template_paths(self):
        for path in self._get_all_file_paths():
            if '.template' in path:
                yield path

    def _get_all_conf_files_paths(self):
        for path in self._get_all_file_paths():
            if not '.template' in path:
                yield path

    def _get_all_file_paths(self, path=None):
        if path is None:
            path = self.opts['local_conf_path']

        for item in listdir(path):
            new_path = path + item
            if isdir(new_path):
                new_path += '/'
                yield from self._get_all_file_paths(new_path)
            elif isfile(new_path):
                yield new_path

    def _render_single_template(self, path):
        dir_path = NodeManager._get_dir_path_from_file_path(path)
        target_filename = NodeManager._get_filename_from_template_path(path)
        env = Environment(loader=FileSystemLoader(dir_path))
        with open(dir_path + target_filename, 'w') as output_file:
            output_file.write(env.get_template(f'{target_filename}.template').render(self.opts))

    @staticmethod
    def _get_filename_from_template_path(path):
        return path.split('/')[-1].split('.template')[0]

    @staticmethod
    def _get_dir_path_from_file_path(path):
        return '/'.join(path.split('/')[:-1]) + '/'

    def _make_host_tmp_path(self):
        self.conn.run(f"mkdir -p {self.opts['host_conf_path']}")

    def _copy_files_to_host(self):
        for path in self._get_all_conf_files_paths():
            remote_rel_dir_path = self._get_rel_dir_path_from_file_path(path)
            host_file_path = self.opts['host_conf_path'] + remote_rel_dir_path
            self.conn.run(f'mkdir -p {host_file_path}')
            self.conn.put(path, remote=host_file_path)

    def _copy_files_to_container(self):
        for path in self._get_all_conf_files_paths():
            rel_path = self._get_rel_path_from_path(path)
            container_rel_dir_path = self._get_dir_path_from_file_path(rel_path)
            container_dir_path = self.opts['container_conf_path'] + container_rel_dir_path
            self.conn.run(f"docker exec {self.opts['container_name']} mkdir -p {container_dir_path}")
            self.conn.run(
                f"docker cp {self.opts['host_conf_path']}{rel_path} {self.opts['container_name']}:{container_dir_path}"
            )

    def _get_rel_path_from_path(self, path):
        return path.split(self.opts['local_conf_path'])[1]

    def _get_rel_dir_path_from_file_path(self, path):
        rel_path = self._get_rel_path_from_path(path)
        return self._get_dir_path_from_file_path(rel_path)

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
        myid = self.opts['id']
        self.conn.run(f"docker exec {self.opts['container_name']} bash -c 'echo {myid} > {self.opts['data_dir']}/myid'")

    def configure(self):
        super().configure()
        self.add_myid_file()


class KafkaClusterManager(ClusterManager):
    def __init__(self):
        super().__init__('kafka', KafkaNodeManager)

class KafkaNodeManager(NodeManager):
    def start(self):
        self.conn.run(f"docker exec {self.opts['container_name']} start.sh")
    
    def set_custom_node_opts(self):
        self.opts['broker_id'] = self.opts['id']

        node_info = self.opts['global']['services']['kafka']['nodes'][self.opts['index']]
        self.opts['internal_host'] = node_info['address']
        self.opts['external_host'] = node_info['address']
        
        zookeeper_nodes = self.opts['global']['services']['zookeeper']['nodes']
        zookeeper_connect = ','.join([f"{node['address']}:{node['port']}" for node in zookeeper_nodes])
        self.opts['zookeeper_connect'] = zookeeper_connect

    def enable_scripts_execution(self):
        self.conn.run(f"docker exec {self.opts['container_name']} chmod -R +x {self.opts['container_conf_path']}bin")

    def configure(self):
        self.set_custom_node_opts()
        super().configure()
        self.enable_scripts_execution()

ZookeeperClusterManager().deploy()
KafkaClusterManager().deploy()