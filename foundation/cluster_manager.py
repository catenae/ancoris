#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
from threading import Thread


class ClusterManager:
    def __init__(self, service_name, node_manager_class, nodes_file='nodes.yaml'):
        with open(nodes_file, 'r') as input_file:
            props = yaml.safe_load(input_file)
        props['service_name'] = service_name

        # Update global props with service props for direct access
        try:
            props.update(props['services'][service_name]['props'])
        except KeyError:
            pass

        conn_props = {
            'port': 22,
            'user': props['user'],
            'connect_kwargs': {
                "key_filename": props['keyfile']
            }
        }

        self.node_managers = []
        for index, node in enumerate(props['services'][service_name]['nodes']):
            node_conn_props = dict(conn_props)
            node_conn_props['host'] = node['public_address']

            node_props = dict(props)
            node_props['index'] = index
            node_props['id'] = index + 1

            node_manager = node_manager_class(node_props, node_conn_props)
            self.node_managers.append(node_manager)

    def deploy(self):
        self._run_parallel('deploy')

    def configure(self):
        self._run_parallel('configure')

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
