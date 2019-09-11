#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cluster_manager import ClusterManager, NodeManager


class SetupClusterManager(ClusterManager):
    def __init__(self):
        super().__init__('setup', SetupNodeManager)


class SetupNodeManager(NodeManager):
    def install_docker(self):
        self.conn.run("sudo yum -y update && " + "sudo yum -y install docker && " +
                      "sudo systemctl start docker && " + "sudo usermod -aG docker ec2-user")

    def deploy(self):
        self.install_docker()
