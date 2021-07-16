from dateutil import parser
from prometheus_api_client import MetricRangeDataFrame
import pandas as pd


class PipelineDuration:
    def __init__(self, config, prom_connect):
        self.config = config,
        self.prom_connect = prom_connect

    def flow(self):
        prom_data = self.__read_dag_durations()

    def __read_prom_data(self, metric, labels, start_date, end_date):
        cluster_config_data = self.prom_connect.get_metric_range_data(
            metric_name=metric,
            # label_config=labels,
            start_time=start_date,
            end_time=end_date
        )
        return MetricRangeDataFrame(cluster_config_data)

    def __read_dag_durations(self):
        start_date = parser.parse(self.config[0].start_date)
        end_date = parser.parse(self.config[0].end_date)
        job_uuid_label = {'job_uuid': '8e4bad0e-9963-4a86-bf3d-9e90f130b4f3'}

        cluster_config_df = self.__read_prom_data('k8s_cluster_setup', job_uuid_label, start_date, end_date)
        dag_duration_df = self.__read_prom_data('airflow_dag_run_duration', job_uuid_label, start_date, end_date)

        ml_input = pd.merge(left=cluster_config_df, right=dag_duration_df, how='inner', on='job_uuid')
        attributes = self.config[0].attributes + ['value_y'] + ['job_uuid']
        subset_ml_input = ml_input[attributes].drop_duplicates()
        subset_ml_input['value_y'] = pd.to_numeric(subset_ml_input['value_y'])
        grouped = subset_ml_input.groupby(['job_uuid']).max()  # last_value = subset_ml_input[subset_ml_input['value_y'] == subset_ml_input['value_y'].max()]
        print(grouped.head(10))
        return grouped  # returns df
