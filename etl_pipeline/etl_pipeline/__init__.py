from dagster import Definitions, load_assets_from_modules

from .assets import raw, mart
from .resources import mysql, minio, postgres
from .jobs import raw_layer_job, mart_layer_job
from .schedules import raw_layer_schedule, mart_layer_schedule


all_assets = load_assets_from_modules([raw, mart])
all_jobs = [raw_layer_job, mart_layer_job]
all_schedules = [raw_layer_schedule, mart_layer_schedule]

defs = Definitions(
    assets=all_assets,
    resources={
        "mysql_io_manager": mysql,
        "minio_io_manager": minio,
        "psql_io_manager": postgres
    },
    jobs=all_jobs,
    schedules=all_schedules
)