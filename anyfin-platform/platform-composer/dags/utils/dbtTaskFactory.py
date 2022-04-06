import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


class DbtTaskFactory:
    def __init__(self, dbt_dir: str, dag: DAG, tag=None):
        """
        Args:
            dbt_dir: home dir for dbt
            dag: DAG object where we want to add new tasks
            tag: if provided, only models with this tag will be added to tasks list
        """
        self.dbt_dir = dbt_dir
        self.tag = tag
        self.dag = dag

    def load_manifest(self):
        local_filepath = f"{self.dbt_dir}/target/manifest.json"
        with open(local_filepath) as f:
            data = json.load(f)
        return data

    def make_dbt_task(self, node, dbt_verb):
        """Returns an Airflow operator either run and test an individual model"""
        global_cli_flags = "--no-write-json"
        model = node.split(".")[-1]

        if dbt_verb == "run":
            dbt_task = BashOperator(
                task_id=node,
                bash_command=f"dbt {global_cli_flags} {dbt_verb} --project-dir {self.dbt_dir} --profiles-dir {self.dbt_dir} --target prod --select {model}",
                dag=self.dag,
            )
            return dbt_task

    def generate_tasks_from_manifest(self):
        """Generate tasks with dependencies based on manifest file with provided tag

        Returns
        -------
        dict of tasks with dependencies {"task_id": task_object}
        """
        data = self.load_manifest()

        dbt_tasks = {}
        if self.tag is not None:
            nodes = {node: value for node, value in data['nodes'].items()
                     if node.split(".")[0] == "model" and self.tag in value["config"]["tags"]}
        else:
            nodes = {node: value for node, value in data['nodes'].items()
                     if node.split(".")[0] == "model"}

        for node, node_info in nodes.items():
            if node_info["config"]["materialized"] == "ephemeral":
                continue
            dbt_tasks[node] = self.make_dbt_task(node, "run")

        for node, node_info in nodes.items():
            if node_info["config"]["materialized"] == "ephemeral":
                continue

            # Set all model -> model dependencies
            dependencies = node_info["depends_on"]["nodes"]
            dependencies = list(set(dependencies))
            while dependencies:
                upstream_node = dependencies.pop()

                if upstream_node not in nodes:
                    continue

                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type != "model":
                    continue

                if nodes[upstream_node]["config"]["materialized"] == "ephemeral":
                    dependencies += nodes[upstream_node]["depends_on"]["nodes"]
                    dependencies = list(set(dependencies))
                else:
                    dbt_tasks[upstream_node] >> dbt_tasks[node]
        return self.dag.task_dict

    def generate_task_group_from_manifest(self, task_group_id: str):
        """Generate task group with tasks from manifest file with provided tag

        Returns
        -------
        TaskGroup object with tasks with dependencies
        """
        with TaskGroup(task_group_id, dag=self.dag) as task_group:
            self.generate_tasks_from_manifest()
        return task_group
