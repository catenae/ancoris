#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cluster_manager import ClusterManager
from kafka import KafkaNodeManager
from zookeeper import ZookeeperNodeManager
from setup import AwsSetupNodeManager
from worker import AncorisWorkerNodeManager


ClusterManager('setup', AwsSetupNodeManager).deploy()

kafka = ClusterManager('kafka', KafkaNodeManager)
kafka.destroy()
kafka.clean_data() # remove

zookeeper = ClusterManager('zookeeper', ZookeeperNodeManager)
zookeeper.destroy()
zookeeper.clean_data() # remove
zookeeper.deploy()
zookeeper.configure()
zookeeper.start()

kafka.deploy()
kafka.configure()
kafka.start()

worker = ClusterManager('worker', AncorisWorkerNodeManager)
worker.destroy()
worker.deploy()
