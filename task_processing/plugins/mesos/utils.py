def is_pod(task_config):
    if type(task_config).__name__ == 'MesosPodConfig':
        return True
    return False
