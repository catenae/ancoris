#!/usr/bin/env python
# -*- coding: utf-8 -*-

from node_manager import NodeManager


class KafkaNodeManager(NodeManager):
    def run(self):
        self.conn.run('docker run -di '
                      # Published ports
                      + f"--publish {self.props['node']['private_address']}:{self.props['internal_port']}:{self.props['internal_port']} "
                      + f"--publish {self.props['external_port']}:{self.props['external_port']} "
                      # Name
                      + f"--name {self.props['container_name']} "
                      # Volumes
                      + f"--volume {self.props['host_data_path']}:{self.props['container_data_path']} "
                      + f"--volume {self.props['host_logs_path']}:{self.props['container_logs_path']} "
                      # Restart policy
                      + f"--restart always "
                      # Docker image
                      + f"{self.props['container_image']}")

    def start(self):
        self.conn.run(f"docker exec {self.props['container_name']} start.sh")

    def set_custom_node_props(self):
        zookeeper_nodes = self.props['services']['zookeeper']['nodes']
        zookeeper_connect = ','.join(
            [f"{node['private_address']}:{node['port']}" for node in zookeeper_nodes])
        self.props['zookeeper_connect'] = zookeeper_connect

    def enable_scripts_execution(self):
        self.conn.run(
            f"docker exec {self.props['container_name']} chmod -R +x {self.props['container_installation_path']}bin"
        )

    def configure(self):
        self.set_custom_node_props()
        super().configure()
        self.enable_scripts_execution()
        