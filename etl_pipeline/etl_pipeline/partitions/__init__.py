from dagster import MonthlyPartitionsDefinition

from ..assets import constants


monthly_partitions = MonthlyPartitionsDefinition(
    start_date=constants.START_DATE
)