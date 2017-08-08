import yaml


def get_cluster_master_by_proxy(
    proxy_prefix,
    cluster,
    services_file='/nail/etc/services/services.yaml',
):
    with open(services_file, 'r') as istream:
        services = yaml.load(istream)

    proxy_name = 'task_processing.' + proxy_prefix + '_' + cluster
    if proxy_name not in services:
        raise KeyError('Cluster master {} was not found'.format(proxy_name))
    else:
        addr_info = services[proxy_name]
        return addr_info['host'] + ':' + str(addr_info['port'])
