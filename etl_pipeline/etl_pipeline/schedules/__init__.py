from dagster import RunRequest, schedule
from dagster import ScheduleEvaluationContext
from ..partitions import monthly_partitions
from .. import constants
from ..jobs import (
    raw_layer_job,
    mart_layer_job
)

@schedule(
    job=raw_layer_job,
    cron_schedule="*/2 * * * *",
    execution_timezone="Asia/Ho_Chi_Minh"
)
def raw_layer_schedule(context: ScheduleEvaluationContext):
    return RunRequest(
        tags={
            "dagster/asset_partition_range_start": "2017-01-01",
            "dagster/asset_partition_range_end": "2017-12-01",
        }
    )
    
@schedule(
    job=mart_layer_job,
    cron_schedule="*/3 * * * *",
    execution_timezone="Asia/Ho_Chi_Minh"
)
def mart_layer_schedule(context: ScheduleEvaluationContext):
    return RunRequest(
        tags={
            "dagster/asset_partition_range_start": "2017-01-01",
            "dagster/asset_partition_range_end": "2017-12-01",
        }
    )