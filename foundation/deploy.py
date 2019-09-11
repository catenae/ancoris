#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kafka import KafkaClusterManager
from zookeeper import ZookeeperClusterManager
from setup import SetupClusterManager


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
