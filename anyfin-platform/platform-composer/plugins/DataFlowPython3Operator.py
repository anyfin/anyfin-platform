"""
Should be deployed under the composer's plugins directory, e.g. gs://europe-west1-production-com-369e4b80-bucket/plugins
"""

from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook
from typing import List, Dict
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.dataflow_operator import GoogleCloudBucketHelper

import re

class DataFlow3Hook(DataFlowHook):
    def start_python_dataflow(
        self,
        job_name: str,
        variables: Dict,
        dataflow: str,
        py_options: List[str],
        append_job_name: bool = True,
        py_interpreter: str = "python3"
    ):
        name = self._build_dataflow_job_name(job_name, append_job_name)
        variables['job_name'] = name
        def label_formatter(labels_dict):
            return ['--labels={}={}'.format(key, value)
                    for key, value in labels_dict.items()]
        self._start_dataflow(variables, name, [py_interpreter] + py_options + [dataflow],
                             label_formatter)
class DataFlowPython3Operator(DataFlowPythonOperator):
    def execute(self, context):
        """Execute the python dataflow job."""
        bucket_helper = GoogleCloudBucketHelper(
            self.gcp_conn_id, self.delegate_to)
        self.py_file = bucket_helper.google_cloud_to_local(self.py_file)
        hook = DataFlow3Hook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            poll_sleep=self.poll_sleep)
        dataflow_options = self.dataflow_default_options.copy()
        dataflow_options.update(self.options)
        # Convert argument names from lowerCamelCase to snake case.
        camel_to_snake = lambda name: re.sub(
            r'[A-Z]', lambda x: '_' + x.group(0).lower(), name)
        formatted_options = {camel_to_snake(key): dataflow_options[key]
                             for key in dataflow_options}
        hook.start_python_dataflow(
            self.job_name, formatted_options,
            self.py_file, self.py_options, py_interpreter="python3")