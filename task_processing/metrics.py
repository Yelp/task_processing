try:
    import yelp_meteorite
    METRICS_ENABLED = True
except Exception:
    METRICS_ENABLED = False


class _DummyMetricType:
    """
    Emulates a yelp_meteorite counter, gauge, or timer
    """
    def count(*args, **kwargs):
        pass

    set = count
    record = count
    start = count
    stop = count


_dummy_metric = _DummyMetricType()
_registered_metrics: dict = {}


def create_counter(name, dimensions={}):
    if not METRICS_ENABLED:
        return

    if name not in _registered_metrics:
        counter = yelp_meteorite.create_counter(
            name, default_dimensions=dimensions)
        _registered_metrics[name] = counter


def create_timer(name, dimensions={}):
    if not METRICS_ENABLED:
        return

    if name not in _registered_metrics:
        timer = yelp_meteorite.create_timer(
            name, default_dimensions=dimensions)
        _registered_metrics[name] = timer


def get_metric(name):
    if METRICS_ENABLED:
        return _registered_metrics.get(name)
    else:
        return _dummy_metric
