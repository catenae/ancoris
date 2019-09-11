#!/usr/bin/env python
# -*- coding: utf-8 -*-

from node_manager import NodeManager


class AncorisWorkerNodeManager(NodeManager):
    def run(self):
        kafka_addresses = [node['private_address'] for node in self.props['services']['kafka']['nodes']]
        kafka_port = self.props['services']['kafka']['props']['internal_port']
        kafka_endpoints = f':{kafka_port},'.join(kafka_addresses) + f':{kafka_port}'
        self.conn.run('docker run -d '
                      # Environment variables
                      + f"--env JSONRPC_HOST={self.props['node']['private_address']} "
                      + f"--env JSONRPC_PORT={self.props['jsonrpc_port']} "
                      + f"--env JSONRPC_SCHEME=http "
                      # Published ports
                      + f"--publish {self.props['node']['private_address']}:{self.props['jsonrpc_port']}:{self.props['jsonrpc_port']} "
                      # Name
                      + f"--name {self.props['container_name']} "
                      # Volumes
                      + f"--volume /var/run/docker.sock:/var/run/docker.sock "
                      + f"--volume /media/adfs:/media/adfs:rw,shared "
                      # Restart policy
                      + f"--privileged "
                      # Restart policy
                      + f"--restart always "
                      # Docker image
                      + f"{self.props['container_image']} "
                      + f"worker.py -k {kafka_endpoints}")