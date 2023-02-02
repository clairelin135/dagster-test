from dagster import (
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    define_asset_job,
    asset,
    repository,
    WeeklyPartitionsDefinition,
    load_assets_from_current_module,
    AssetSelection,
    AssetKey,
    RunsFilter,
    DagsterRunStatus,
    sensor,
    SkipReason,
)
from dagster._core.definitions.multi_dimensional_partitions import (
    MultiPartitionsDefinition,
    MultiPartitionKey,
)

time_window_partitions = DailyPartitionsDefinition(start_date="2015-05-05")
static_partitions = StaticPartitionsDefinition(["a", "b", "c", "d", "e", "f", "g", "x"])
composite = MultiPartitionsDefinition(
    {
        "abc": static_partitions,
        "date": time_window_partitions,
    }
)


@asset(partitions_def=composite)
def asset1(context):
    context.log.info(context.partition_key)
    context.log.info(context.run.tags)
    return 1


@asset(partitions_def=composite)
def asset2(asset1):
    return 2


@asset(
    partitions_def=MultiPartitionsDefinition(
        {
            "yyyyyyy": static_partitions,
            "foo": static_partitions,
        }
    )
)
def aosdaoid(context):
    return 1


@asset(partitions_def=DailyPartitionsDefinition(start_date="2019-01-01"))
def daily_asset():
    return 1


@asset(partitions_def=StaticPartitionsDefinition(["a", "b", "c", "d"]))
def static_asset():
    return 1


@asset(
    partitions_def=MultiPartitionsDefinition(
        {
            "yyyyyyy": static_partitions,
            "foo": static_partitions,
        }
    )
)
def asset3():
    return 1
