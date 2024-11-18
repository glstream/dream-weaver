from dagster import Definitions, load_assets_from_package_module, ResourceDefinition
# from dream_weaver import assets, dagster_databricks_pipes

from .assets import crystal, clear, hacker
from .constants import CRYSTAL, CLEAR, HACKER

from dagster_databricks import PipesDatabricksClient
from databricks.sdk import WorkspaceClient
import os

clear_assets = load_assets_from_package_module(clear, group_name=CLEAR)
crystal_assets = load_assets_from_package_module(crystal, group_name=CRYSTAL)
hacker_assets = load_assets_from_package_module(hacker, group_name=HACKER)

all_assets = [
    *clear_assets,
    *crystal_assets,
    *hacker_assets
]



pipes_databricks_resource = PipesDatabricksClient(
    client=WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
)


defs = Definitions(
    assets=all_assets,
    resources={"pipes_databricks": pipes_databricks_resource}
)
