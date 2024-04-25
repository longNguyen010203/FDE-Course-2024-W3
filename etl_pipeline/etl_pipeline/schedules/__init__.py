from dagster import ScheduleDefinition
from ..jobs import (
    raw_layer_job,
    mart_layer_job
)


raw_layer_schedule = ScheduleDefinition(
    job=raw_layer_job,
    cron_schedule="*/2 * * * *",
    execution_timezone="Asia/Ho_Chi_Minh"
)

mart_layer_schedule = ScheduleDefinition(
    job=mart_layer_job,
    cron_schedule="*/3 * * * *",
    execution_timezone="Asia/Ho_Chi_Minh"
)