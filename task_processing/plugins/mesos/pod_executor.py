
class PodExecutionFramework():
    TASK_CONFIG = MesosPodConfig
    def __init__(self):
        # pass
        self.logger = logging.getLogger(__name__)

        # TODO: Get mesos master ips from smartstack
        self.driver = MesosSchedulerDriver(
            sched=self.execution_framework,
            framework=self.execution_framework.framework_info,
            use_addict=True,
            master_uri=mesos_address,
            implicit_acknowledgements=False,
            principal=principal,
            secret=secret,
        )

        # start driver thread immediately
        self.driver_thread = threading.Thread(
            target=self.driver.run, args=())
        self.driver_thread.daemon = True
        self.driver_thread.start()
