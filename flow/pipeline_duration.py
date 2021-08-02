from dateutil import parser
from prometheus_api_client import MetricRangeDataFrame
import pandas as pd


class PipelineDuration:
    def __init__(self, config, prom_connect):
        self.config = config,
        self.prom_connect = prom_connect

    def flow(self):
        prom_data = self.__read_dag_durations()

    def __read_prom_data(self, metric, labels, start_date, end_date):  # todo delete labels parameter
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
        job_uuid_label = {'job_uuid': '4b537078-d8a0-44fe-b72d-ab972b662801'}  # todo delete

        cluster_config_df = self.__read_prom_data('k8s_cluster_setup', job_uuid_label, start_date, end_date)
        dag_duration_df = self.__read_prom_data('airflow_dag_run_duration', job_uuid_label, start_date, end_date)
        last_dag_status = self.__read_prom_data('airflow_dag_status', job_uuid_label, start_date, end_date)

        last_dag_status_max_timestamps = last_dag_status.sort_index(ascending=False).drop_duplicates(['job_uuid'])

        cluster_config_dag_dur = pd.merge(left=cluster_config_df, right=dag_duration_df, how='inner', on='job_uuid')
        attributes = self.config[0].attributes + ['value_y'] + ['job_uuid']
        subset_cluster_config_dag_dur = cluster_config_dag_dur[attributes].drop_duplicates()
        subset_cluster_config_dag_dur['value_y'] = pd.to_numeric(subset_cluster_config_dag_dur['value_y'])
        grouped_config_dag_dur = subset_cluster_config_dag_dur.groupby(['job_uuid']).max()

        config_dag_dur_status = pd.merge(left=grouped_config_dag_dur, right=last_dag_status_max_timestamps,
                                         how='inner', on='job_uuid')
        attributes = attributes + ['status']
        config_dag_dur_status = config_dag_dur_status[attributes].drop_duplicates()
        config_dag_dur_status = config_dag_dur_status.rename({'value_y': 'dag_duration'}, axis=1)

        print(config_dag_dur_status.head(10))
        return config_dag_dur_status  # returns df
