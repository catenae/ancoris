#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, rpc, utils as catenae_utils
from random import randint
import json
from enum import Enum
from common import startup_text


class AncorisMaster(Link):
    class JSONRPC_ERRORS(Enum):
        NO_WORKERS_AVAILABLE = -32000

    def _get_worker(self, request):
        try:
            workers = self.instances['by_group']['catenae_ancorisworker']
        except KeyError:
            return

        random_index = randint(0, len(workers) - 1)
        return workers[random_index]

    @rpc
    def launch_container(self, request):
        request_id = catenae_utils.get_uid()

        worker_uid = self._get_worker(request)
        if worker_uid is None:
            error = AncorisMaster.JSONRPC_ERRORS.NO_WORKERS_AVAILABLE
            return error.value, error.name

        response = self.jsonrpc_call(worker_uid, 'launch_container', request, request_id=request_id)
        return response


if __name__ == "__main__":
    AncorisMaster().start(startup_text=startup_text)