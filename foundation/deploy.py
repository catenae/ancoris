#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fabric import Connection
from os import listdir, makedirs
from os.path import isfile, isdir
import yaml
from jinja2 import Environment, FileSystemLoader
from threading import Thread
import shutil


class ClusterManager:
    def __init__(self, service_name, node_manager_class):
        with open(f'conf/{service_name}.yaml', 'r') as input_file:
            opts = yaml.safe_load(input_file)
        if opts is None:
            opts = {}
        opts['service_name'] = service_name

        with open('conf/nodes.yaml', 'r') as input_file:
            opts['global'] = yaml.safe_load(input_file)

        conn_props = {
            'port': 22,
            'user': opts['global']['user'],
            'connect_kwargs': {
                "key_filename": opts['global']['keyfile']
            }
        }

        self.node_managers = []
        for index, node in enumerate(opts['global']['services'][service_name]['nodes']):
            node_conn_props = dict(conn_props)
            node_conn_props['host'] = node['public_address']

            node_opts = dict(opts)
            node_opts['index'] = index
            node_opts['id'] = index + 1

            node_manager = node_manager_class(node_opts, node_conn_props)
            self.node_managers.append(node_manager)

    def deploy(self):
        self._run_parallel('deploy')

    def start(self):
        self._run_parallel('start')

    def destroy(self):
        self._run_parallel('destroy')

    def clean_data(self):
        self._run_parallel('clean_data')

    def _run_parallel(self, target):
        threads = []
        for node_manager in self.node_managers:
            threads.append(Thread(target=getattr(node_manager, target)))
            threads[-1].start()
        for thread in threads:
            thread.join()


class NodeManager:
    def __init__(self, opts, conn_props):
        self.opts = opts
        self.conn = Connection(**conn_props)

    def deploy(self):
        self.pull()
        self.run()
        self.configure()

    def pull(self):
        self.conn.run(f"docker pull {self.opts['container_image']}")

    def run(self):
        self.conn.run('docker run -tid '
                      # Network
                      + '--net=host '
                      # Name
                      + f"--name {self.opts['container_name']} "
                      # Volume
                      +
                      f"--volume {self.opts['host_data_path']}:{self.opts['container_data_path']} "
                      # Image
                      + f"{self.opts['container_image']}")

    def stop(self):
        self.conn.run(f"docker stop {self.opts['container_name']}")

    def destroy(self):
        try:
            self.stop()
            try:
                self.conn.run(f"docker rm -f {self.opts['container_name']}")
            except Exception:
                return
        except Exception:
            return

    def configure(self):
        self._prepare_local_env()
        self._rendered_templates()
        self._remove_host_tmp_path()
        self._make_host_tmp_path()
        self._copy_files_to_host()
        self._copy_files_to_container()

    def clean_data(self):
        self.conn.run(f"sudo rm -rf {self.opts['host_data_path']}")

    def start(self):
        raise NotImplementedError

    def _get_rendered_service_dir_path(self):
        return f"{self._get_rendered_dir_path()}{self.opts['service_name']}/"

    def _get_rendered_dir_path(self):
        return f"{self.opts['local_conf_path']}rendered/node{self.opts['id']}/"

    def _prepare_local_env(self):
        conf_dir_path = f"{self.opts['local_conf_path']}{self.opts['service_name']}/"
        rendered_service_dir_path = self._get_rendered_service_dir_path()

        try:
            makedirs(rendered_service_dir_path)
        except FileExistsError:
            pass

        try:
            shutil.rmtree(rendered_service_dir_path)
        except FileNotFoundError:
            pass

        shutil.copytree(conf_dir_path, rendered_service_dir_path)

    def _rendered_templates(self):
        for template_path in self._get_all_template_paths():
            self._rendered_single_template(template_path)

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
            path = self._get_rendered_service_dir_path()

        for item in listdir(path):
            new_path = path + item
            if isdir(new_path):
                new_path += '/'
                yield from self._get_all_file_paths(new_path)
            elif isfile(new_path):
                yield new_path

    def _rendered_single_template(self, path):
        dir_path = NodeManager._get_dir_path_from_file_path(path)
        target_filename = NodeManager._get_filename_from_template_path(path)
        env = Environment(loader=FileSystemLoader(dir_path))
        with open(f"{dir_path}{target_filename}", 'w') as output_file:
            output_file.write(env.get_template(f'{target_filename}.template').render(self.opts))

    @staticmethod
    def _get_filename_from_template_path(path):
        return NodeManager._get_filename_from_file_path(path).split('.template')[0]

    @staticmethod
    def _get_filename_from_file_path(path):
        return path.split('/')[-1]

    @staticmethod
    def _get_dir_path_from_file_path(path):
        return '/'.join(path.split('/')[:-1]) + '/'

    def _make_host_tmp_path(self):
        self.conn.run(f"mkdir -p {self.opts['global']['host_tmp_path']}{self.opts['service_name']}")

    def _copy_files_to_host(self):
        for path in self._get_all_conf_files_paths():
            rel_path = self._get_rel_dir_path_from_file_path(path)
            host_dir_path = f"{self.opts['global']['host_tmp_path']}{self.opts['service_name']}/{rel_path}"
            self.conn.run(f'mkdir -p {host_dir_path}')
            self.conn.put(f"{path}", remote=f"{host_dir_path}")

    def _copy_files_to_container(self):
        for path in self._get_all_conf_files_paths():

            rel_path = self._get_rel_path_from_file_path(path)
            host_file_path = f"{self.opts['global']['host_tmp_path']}{self.opts['service_name']}/{rel_path}"

            rel_dir_path = self._get_rel_dir_path_from_file_path(path)
            container_dir_path = self.opts['container_installation_path'] + rel_dir_path

            self.conn.run(
                f"docker exec {self.opts['container_name']} mkdir -p {container_dir_path}")
            self.conn.run(
                f"docker cp {host_file_path} {self.opts['container_name']}:{container_dir_path}"
            )

    def _get_rel_path_from_file_path(self, path):
        discarded_path = f"{self._get_rendered_dir_path()}{self.opts['service_name']}/"
        rel_path = path.split(discarded_path)[1]
        return rel_path

    def _get_rel_dir_path_from_file_path(self, path):
        rel_path = self._get_rel_path_from_file_path(path)
        rel_dir_path = self._get_dir_path_from_file_path(rel_path)
        return '/'.join(rel_dir_path.split('/'))

    def _remove_host_tmp_path(self):
        self.conn.run(f"rm -rf {self.opts['global']['host_tmp_path']}{self.opts['service_name']}")


class ZookeeperClusterManager(ClusterManager):
    def __init__(self):
        super().__init__('zookeeper', ZookeeperNodeManager)


class ZookeeperNodeManager(NodeManager):
    def start(self):
        self.conn.run(f"docker exec {self.opts['container_name']} zkServer.sh start")

    def add_myid_file(self):
        myid = self.opts['id']
        self.conn.run(
            f"docker exec {self.opts['container_name']} bash -c 'echo {myid} > {self.opts['data_dir']}/myid'"
        )

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
        self.opts['internal_host'] = node_info['private_address']
        self.opts['external_host'] = node_info['public_address']

        zookeeper_nodes = self.opts['global']['services']['zookeeper']['nodes']
        zookeeper_connect = ','.join(
            [f"{node['private_address']}:{node['port']}" for node in zookeeper_nodes])
        self.opts['zookeeper_connect'] = zookeeper_connect

    def enable_scripts_execution(self):
        self.conn.run(
            f"docker exec {self.opts['container_name']} chmod -R +x {self.opts['container_installation_path']}bin"
        )

    def configure(self):
        self.set_custom_node_opts()
        super().configure()
        self.enable_scripts_execution()


class SetupClusterManager(ClusterManager):
    def __init__(self):
        super().__init__('setup', SetupNodeManager)


class SetupNodeManager(NodeManager):
    def install_docker(self):
        self.conn.run("sudo yum -y update && " + "sudo yum -y install docker && " +
                      "sudo systemctl start docker && " + "sudo usermod -aG docker ec2-user")

    def deploy(self):
        self.install_docker()


SetupClusterManager().deploy()

zookeeper = ZookeeperClusterManager()
kafka = KafkaClusterManager()

kafka.destroy()
zookeeper.destroy()

zookeeper.clean_data()
kafka.clean_data()

zookeeper.deploy()
kafka.deploy()

zookeeper.start()
kafka.start()