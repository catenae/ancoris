#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, rpc, utils as catenae_utils
from errors import UnsupportedDevice, ContainerError
from docker_manager import Docker
from common import startup_text
from threading import Thread


class AncorisWorker(Link):
    def setup(self):
        self.docker = Docker(self)

    @rpc
    def run(self, **kwargs):
        # El worker decide si acepta el trabajo o no
        # Si lo acepta devuelve el id del contenedor

        # Check resources
        if not AncorisWorker.accept_container(kwargs):
            raise NotImplementedError

        container_auid = catenae_utils.get_uid()
        AncorisWorker.adapt_request(container_auid, kwargs)
        self.launch_thread(self.docker.run, kwargs=kwargs)

        return {'container_auid': container_auid}

    @rpc
    def remove(self, context, container_auid):
        print(container_auid)
        # if I have the container
        self.docker.remove(container_auid)
        # try:
        #     self.launch_thread(self.docker.remove, container_auid)
        # except Exception:
        #     pass

    @staticmethod
    def adapt_request(container_auid, request):
        request['name'] = f'ancoris_{container_auid}'

    @staticmethod
    def accept_container(kwargs):
        return True


if __name__ == "__main__":
    AncorisWorker().start(startup_text=startup_text)