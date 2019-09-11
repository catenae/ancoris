import docker
from catenae import Logger


class Docker:
    def __init__(self, link_instance):
        self.low_level_client = docker.APIClient(base_url='unix://run/docker.sock', version='auto')
        self.client = docker.DockerClient(base_url='unix://run/docker.sock', version='auto')
        self.logger = Logger(link_instance)

    def run(self,
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

        self.pull(image)
        self.logger.log('Launching container...')

        if args == None:
            args = []

        if environment == None:
            environment = {}

        # Min cores value: 0.01 (1% of the CPU time of one core)
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

        self.client.containers.run(**run_opts)
        self.logger.log(f'Container launched! {run_opts}', level='debug')

    def pull(self, image):
        # Otherwise, the low-level client will download every single image
        if not ':' in image:
            image += ':latest'
        self.low_level_client.pull(image)

    def stop_all(self):
        containers = self.low_level_client.containers(all=True, filters={'name': 'ancoris_'})
        for container in containers:
            self.stop(container['Id'])

    def stop(container):
        self.low_level_client.stop(container)

    def kill(container):
        self.low_level_client.kill(container)

    def exists(container):
        if self.info(container):
            return True
        return False

    def info(container):
        """
        Returns the available information for an existing container
        which matches exactly the given name.
        """
        containers_info = self.low_level_client.containers(all=True, filters={'name': container})
        for container_info in containers_info:
            if container == container_info['Id']:
                return container_info
            for name in container_info['Names']:
                if f'/{container}' == name:
                    return container_info

    def port_mappings(container):
        # TODO TCP / UDP, MULTI IP?
        container_info = self.info(container)
        mappings = []
        if container_info:
            ports = container_info['Ports']
            for port in ports:
                if 'PublicPort' in port:
                    mappings.append({'container': port['PrivatePort'], 'host': port['PublicPort']})
            return mappings

    def status(container):
        container_info = self.info(container)
        if container_info:
            return container_info['State']

    def logs(container, low_level_client):
        logs = self.low_level_client.logs(container, stream=False)
        if logs:
            return logs.decode('utf-8')
