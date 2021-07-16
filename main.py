import sys

from utils import read_and_parse_config, read_cli_args
from prometheus_api_client import PrometheusConnect
from flow.pipeline_duration import PipelineDuration

print("Reading CLI arguments")
config_path = read_cli_args().config

print("Reading config from: " + config_path)
config = read_and_parse_config(config_path)

print("Connecting to Prometheus - " + config.prometheus.url)
prom = PrometheusConnect(url=config.prometheus.url, disable_ssl=True)
print("Is connection valid: " + str(prom.check_prometheus_connection()))

if config.parameter == 'pipeline_duration':
    pipeline_duration = PipelineDuration(config, prom)
    pipeline_duration.flow()
else:
    print("Parameter: {} is not supported yet.".format(config.parameter))
    sys.exit(-1)
