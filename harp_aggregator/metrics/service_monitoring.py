from microservice_template_core.tools.prometheus_metrics import Gauge, Counter, Summary, Histogram


class Prom:
    UPDATE_AEROSPIKE_CACHE = Summary('update_aerospike_cache_latency_seconds', 'Time spent processing update cache in Aerospike')
    COLLECT_ACTIVE_ALERTS = Summary('collect_active_alerts_latency_seconds', 'Time spent processing SQL requests to collect active alerts')
    REFORMAT_ALERTS = Summary('reformat_alerts_latency_seconds', 'Time spent processing reformat alerts')

    UPDATE_ENVIRONMENT_DICT = Summary('update_environment_dict_latency_seconds', 'Time spent processing update environment dict')
