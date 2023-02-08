from dagster import Definitions, load_assets_from_current_module
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
    DynamicPartitionsDefinition,
    op,
    job,
    build_asset_reconciliation_sensor,
)
import random
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


@asset
def unpartitioned_asset():
    return 2


@asset(partitions_def=composite)
def multipartitioned_asset(context):
    context.log.info(context.partition_key)
    context.log.info(context.run.tags)
    return 1


@asset(partitions_def=StaticPartitionsDefinition(["a", "b", "c", "d"]))
def static_partitioned_asset():
    return 1


@asset(partitions_def=DailyPartitionsDefinition(start_date="2019-01-01"))
def daily_asset():
    return 1


foo_partitions_def = DynamicPartitionsDefinition(name="foo")


@asset(partitions_def=foo_partitions_def)
def foo_dynamic_asset():
    return 1


@asset(partitions_def=foo_partitions_def)
def foo_downstream_asset(foo_dynamic_asset):
    return 1


@asset
def unpartitioned_downstream_of_foo(foo_dynamic_asset):
    return 1


@op
def op1(context):
    foo_partitions_def.add_partitions(["1", "2", "3", "4"], context.instance)


@job
def create_foo_dynamic_partitions_job():
    op1()


quux = DynamicPartitionsDefinition(name="quux")


@asset(partitions_def=quux)
def quux_dynamic_asset(context):
    return 1


quux_asset_job = define_asset_job("quux_asset_job", [quux_dynamic_asset], partitions_def=quux)


@sensor(job=quux_asset_job)
def add_quux_partitions_sensor(context):
    pk = str(random.randint(0, 5))
    quux.add_partitions([pk], context.instance)
    return quux_asset_job.run_request_for_partition(pk, instance=context.instance)


defs = Definitions(
    assets=load_assets_from_current_module(),
    jobs=[create_foo_dynamic_partitions_job, quux_asset_job],
    sensors=[
        add_quux_partitions_sensor,
        build_asset_reconciliation_sensor(
            AssetSelection.keys(
                "foo_dynamic_asset", "foo_downstream_asset", "unpartitioned_downstream_of_foo"
            )
        ),
    ],
)
