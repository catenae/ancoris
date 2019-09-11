#!/usr/bin/env python
# -*- coding: utf-8 -*-

from node_manager import NodeManager


class ZookeeperNodeManager(NodeManager):
    def run(self):
        self.conn.run('docker run -di '
                      # Published ports
                      + f"--publish {self.props['node']['private_address']}:{self.props['client_port']}:{self.props['client_port']} "
                      + f"--publish {self.props['node']['private_address']}:{self.props['server_port']}:{self.props['server_port']} "
                      + f"--publish {self.props['node']['private_address']}:{self.props['leader_port']}:{self.props['leader_port']} "
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
        self.conn.run(f"docker exec {self.props['container_name']} zkServer.sh start")

    def add_myid_file(self):
        myid = self.props['id']
        self.conn.run(
            f"docker exec {self.props['container_name']} bash -c 'echo {myid} > {self.props['data_dir']}/myid'"
        )

    def configure(self):
        super().configure()
        self.add_myid_file()
