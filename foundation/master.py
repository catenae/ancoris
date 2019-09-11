#!/usr/bin/env python
# -*- coding: utf-8 -*-

from node_manager import NodeManager


class MasterNodeManager(NodeManager):
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
                      # Restart policy
                      + f"--restart always "
                      # Docker image
                      + f"{self.props['container_image']} "
                      + f"master.py -k {kafka_endpoints}")
