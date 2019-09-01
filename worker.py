#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, rpc, Thread, utils as catenae_utils
import docker
from errors import UnsupportedDevice, ContainerError


class AncorisWorker(Link):
    def setup(self):
        self.low_level_client = docker.APIClient(base_url='unix://run/docker.sock', version='auto')
        self.client = docker.DockerClient(base_url='unix://run/docker.sock', version='auto')

    @rpc
    def launch_container(self, **kwargs):
        # El worker decide si acepta el trabajo o no
        # Si lo acepta devuelve el id del contenedor

        # Check resources
        if not AncorisWorker.accept_container(kwargs):
            raise NotImplementedError

        container_uid = catenae_utils.get_uid()
        AncorisWorker.adapt_request(container_uid, kwargs)
        Thread(target=self.run_container, kwargs=kwargs).start()
        return {'container_uid': container_uid}

    @staticmethod
    def adapt_request(container_uid, request):
        request['name'] = f'ancoris_{container_uid}'

        if not ':' in request['image']:
            # Otherwise, the low-level client will download every single image
            request['image'] += ':latest'

    @staticmethod
    def accept_container(kwargs):
        return True

    # Min cores value: 0.01 (1% of the CPU time of one core)
    def run_container(self,
                      image,
                      name,
                      args=None,
                      environment=None,
                      cores=None,
                      memory=None,
                      swap=None,
                      swappiness=None,
                      volumes=None,
                      ports=None,
                      devices=None,
                      execution_id=None,
                      task_id=None,
                      group_id=None,
                      network_disabled=None,
                      network_mode=None,
                      cpu_soft_limit=None,
                      auto_remove=None,
                      auto_restart=None):
        """
        Launch a new container
        """

        self.pull_image(image)

        self.logger.log('Launching container...')

        if args == None:
            args = []
        if environment == None:
            environment = {}
        if not cores:
            cores = 1
        if not memory:
            memory = 128
        if not swap:
            swap = 0
        if not swappiness:
            swappiness = 0
        if volumes == None:
            volumes = []
        if ports == None:
            ports = []
        if devices == None:
            devices = []
        if network_disabled == None:
            network_disabled = False
        if cpu_soft_limit == None:
            cpu_soft_limit = True
        if auto_remove == None:
            auto_remove = True
        if auto_restart == None:
            auto_restart = False

        run_opts = {}
        run_opts['image'] = image
        run_opts['name'] = name
        run_opts['command'] = args

        # Memory
        run_opts['mem_limit'] = str(memory) + 'm'
        run_opts['memswap_limit'] = str(int(swap) + int(memory)) + 'm'
        run_opts['mem_swappiness'] = int(swappiness)

        # CPU
        if cpu_soft_limit:  # Soft CPU limit
            run_opts['cpu_shares'] = int(float(cores) * 200)

        else:  # Hard CPU limit
            run_opts['cpu_period'] = 100000
            run_opts['cpu_quota'] = \
                int(float(cores) * run_opts['cpu_period'])

        # Ports
        if ports:
            run_opts['ports'] = {}
            # The specified ports are mapped to the host machine in a random port
            for port in ports:
                run_opts['ports'][str(port) + '/tcp'] = None

        # Devices
        if devices:
            run_opts['devices'] = []
            for device in devices:
                nvidia_gpu = False
                # https://github.com/NVIDIA/nvidia-docker/wiki/GPU-isolation
                if device['group'] == 'nvidia_gpu':
                    # These two devices also have to be mounted
                    if not nvidia_gpu:
                        run_opts['devices'].extend([
                            "/dev/nvidia-uvm:/dev/nvidia-uvm:rwm",
                            "/dev/nvidiactl:/dev/nvidiactl:rwm"
                        ])
                        nvidia_gpu = True
                    # The NVIDIA GPU
                    device_path = "/dev/" + device['id']
                    run_opts['devices'].append(device_path + ":" + device_path + "rwm")
                else:
                    raise UnsupportedDevice('Only Nvidia GPUs supported')

        # Volumes
        if volumes:
            run_opts['volumes'] = {}
            for volume in volumes:
                host_path = volume['host_path']
                run_opts['volumes'][host_path] = {}
                if 'bind_path' in volume:
                    run_opts['volumes'][host_path]['bind'] = volume['bind_path']
                else:
                    run_opts['volumes'][host_path]['bind'] = '/mnt/' + volume['id']

                if not 'mode' in volume:
                    volume['mode'] = 'ro'
                run_opts['volumes'][host_path]['mode'] = volume['mode']

        # Environment variables
        run_opts['environment'] = environment
        run_opts['environment']['TASK_ID'] = task_id
        run_opts['environment']['GROUP_ID'] = group_id

        # Events
        if auto_remove:
            run_opts['remove'] = True

        if auto_restart:
            run_opts['restart_policy'] = {"Name": "always"}

        # Other options
        if network_mode:
            run_opts['network_mode'] = network_mode
        run_opts['network_disabled'] = network_disabled
        run_opts['detach'] = True
        run_opts['stdin_open'] = True  # Interactive, run bash without exiting, for instance.
        run_opts['tty'] = True  # TTY

        try:
            self.client.containers.run(**run_opts)
        except docker.errors.APIError as e:
            raise ContainerError('Error launching the container: ' + name + ' ' + str(e))

        self.logger.log(f'Container launched! {run_opts}')

    def pull_image(self, image):
        """
        Pull a Docker image
        """

        self.logger.log('Pulling image...')

        try:
            self.low_level_client.pull(image)
        except docker.errors.APIError as e:
            raise ContainerError('Error pulling the image: ' + image + '. ' + str(e))

        self.logger.log('Image pulled!')


if __name__ == "__main__":
    AncorisWorker().start()