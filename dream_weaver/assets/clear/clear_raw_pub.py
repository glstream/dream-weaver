### dagster_databricks_pipes.py

import os
import sys

from dagster_databricks import PipesDatabricksClient

from dagster import AssetExecutionContext, Definitions, EnvVar, asset
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


@asset
def test_clear_raw(
    context: AssetExecutionContext, pipes_databricks: PipesDatabricksClient
):
    
    notebook_path = "/Workspace/Shared/test_clear_raw" 
    cluster_id = '1116-223110-5uqpy9li'
    task = jobs.SubmitTask.from_dict(
        {
            # The cluster settings below are somewhat arbitrary. Dagster Pipes is
            # not dependent on a specific spark version, node type, or number of
            # workers.
            # "new_cluster": {
            #     "spark_version": "15.4.x-scala2.12",
            #     "node_type_id": "e2-highmem-2",
            #     "num_workers": 0,
            #     "cluster_log_conf": {
            #         "dbfs": {"destination": "dbfs:/cluster-logs-dir-noexist"},
            #     },
            # },
            "existing_cluster_id": cluster_id,
            "libraries": [
                # Include the latest published version of dagster-pipes on PyPI
                # in the task environment
                {"pypi": {"package": "dagster-pipes"}},
            ],
            "task_key": "notebook_task",
            "notebook_task": {
                "notebook_path": notebook_path,  # Path to the Databricks notebook
                "base_parameters": {
                    # Optional parameters for the notebook
                },
            },
        }
    )

    # print("This will be forwarded back to Dagster stdout")
    # print("This will be forwarded back to Dagster stderr", file=sys.stderr)

    # extras = {"some_parameter": 100}

    return pipes_databricks.run(
        task=task,
        context=context,
        # extras=extras,
    ).get_materialize_result()


@asset(deps=[test_clear_raw])
def test_clear_pub(
    context: AssetExecutionContext, pipes_databricks: PipesDatabricksClient
):
    
    notebook_path = "/Workspace/Shared/test_clear_pub"
    cluster_id = 'my_cluster_id'
    task = jobs.SubmitTask.from_dict(
        {
            "existing_cluster_id": cluster_id,
            "libraries": [
                # Include the latest published version of dagster-pipes on PyPI
                # in the task environment
                {"pypi": {"package": "dagster-pipes"}},
            ],
            "task_key": "notebook_task",
            "notebook_task": {
                "notebook_path": notebook_path,  # Path to the Databricks notebook
                "base_parameters": {
                    # Optional parameters for the notebook
                },
            },
        }
    )

    # print("This will be forwarded back to Dagster stdout")
    # print("This will be forwarded back to Dagster stderr", file=sys.stderr)

    # extras = {"some_parameter": 100}

    return pipes_databricks.run(
        task=task,
        context=context,
        # extras=extras,
    ).get_materialize_result()

