from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time
import uuid

# 63

dag_run_uuid = uuid.uuid4()


registry = CollectorRegistry()
labelnames = ["number_of_nodes", "number_of_executors", "driver_memory", "executor_memory"]
g = Gauge('k8s_cluster_setup', 'GKE cluster setup', labelnames=labelnames, registry=registry)
g.labels(number_of_nodes="6", number_of_executors="2", driver_memory="512", executor_memory="512").set(time.time())
push_to_gateway('0.0.0.0:9091', job=str(dag_run_uuid), registry=registry)
# push_to_gateway('0.0.0.0:9091', job='batchA', registry=registry)