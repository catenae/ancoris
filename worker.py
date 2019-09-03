#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, rpc, Thread, utils as catenae_utils
from errors import UnsupportedDevice, ContainerError
from docker_manager import Docker
from common import startup_text


class AncorisWorker(Link):
    def setup(self):
        self.docker = Docker(self)

    @rpc
    def launch_container(self, **kwargs):
        # El worker decide si acepta el trabajo o no
        # Si lo acepta devuelve el id del contenedor

        # Check resources
        if not AncorisWorker.accept_container(kwargs):
            raise NotImplementedError

        container_uid = catenae_utils.get_uid()
        AncorisWorker.adapt_request(container_uid, kwargs)
        Thread(target=self.docker.run, kwargs=kwargs).start()

        return {'container_uid': container_uid}

    @staticmethod
    def adapt_request(container_uid, request):
        request['name'] = f'ancoris_{container_uid}'

    @staticmethod
    def accept_container(kwargs):
        return True


if __name__ == "__main__":
    AncorisWorker().start(startup_text=startup_text)