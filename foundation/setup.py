#!/usr/bin/env python
# -*- coding: utf-8 -*-

from node_manager import NodeManager


class AwsSetupNodeManager(NodeManager):
    def install_docker(self):
        self.conn.run("sudo yum -y update && " + "sudo yum -y install docker && " +
                      "sudo systemctl start docker && " + "sudo usermod -aG docker ec2-user")

    def deploy(self):
        self.install_docker()
