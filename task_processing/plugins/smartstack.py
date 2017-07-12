import yaml


YOCALHOST_IP = '169.254.255.254'
TASKPROC_SERVICE_FILE = '/nail/etc/services/task_processing/smartstack.yaml'


class MesosLeaderNotFound(Exception):
    pass


class ProxyPortNotFound(Exception):
    pass


def get_mesos_master(cluster, region):
    """ Finds the yocalhost ip:port for the Mesos master via SmartStack
    Args customize granularity, defaults to a dev cluster for safety
    """
    with open(TASKPROC_SERVICE_FILE, 'r') as istream:
        taskproc_conf = yaml.load(istream)

    leader_name = 'mesos_leader_paasta_{cluster}_{region}'.format(
        cluster=cluster,
        region=region,
    )

    if leader_name not in taskproc_conf:
        raise MesosLeaderNotFound(leader_name)
    elif 'proxy_port' not in taskproc_conf[leader_name]:
        raise ProxyPortNotFound
    else:
        proxy_port = taskproc_conf[leader_name]['proxy_port']
        return YOCALHOST_IP + ':' + str(proxy_port)
