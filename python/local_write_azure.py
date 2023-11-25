import os
import datetime as dt
import uuid
import pyarrow.dataset as ds
import pyarrow as pa


account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
client = os.getenv("AZURE_CLIENT_ID")
tenant = os.getenv("AZURE_TENANT_ID")
secret = os.getenv("AZURE_CLIENT_SECRET")

storage_options = {"account_name": account_name, "client_id": client, "client_secret": secret, "tenant_id": tenant, "max_buffer_size": f"{100 * 1024}"}

GENERATED_SCHEMA = pa.schema(
    [
        pa.field("date", pa.timestamp("us")),
        pa.field("code", pa.string()),
        pa.field("year", pa.int64()),
    ]
)

NYC_TAXI_SCHEMA = pa.schema(
    [
        pa.field('vendorID', pa.string()),
        pa.field('lpepPickupDatetime', pa.string()),
        pa.field('lpepDropoffDatetime', pa.string()),
        pa.field('passengerCount', pa.string()),
        pa.field('tripDistance', pa.string()),
        pa.field('puLocationId', pa.string()),
        pa.field('doLocationId', pa.string()),
        pa.field('pickupLongitude', pa.string()),
        pa.field('pickupLatitude', pa.string()),
        pa.field('dropoffLongitude', pa.string()),
        pa.field('dropoffLatitude', pa.string()),
        pa.field('rateCodeID', pa.string()),
        pa.field('storeAndFwdFlag', pa.string()),
        pa.field('paymentType', pa.string()),
        pa.field('fareAmount', pa.string()),
        pa.field('extra', pa.string()),
        pa.field('mtaTax', pa.string()),
        pa.field('improvementSurcharge', pa.string()),
        pa.field('tipAmount', pa.string()),
        pa.field('tollsAmount', pa.string()),
        pa.field('ehailFee', pa.string()),
        pa.field('totalAmount', pa.string()),
        pa.field('tripType', pa.string()),
        pa.field('year_month', pa.string())
    ]
)

GENERATED_SOURCE = "/Users/nikolay.ulmasov/work/REPOS/r3stl355/delta-rs/justfile/data/generated"
NYC_TAXI_SOUCE = "/Users/nikolay.ulmasov/work/REPOS/r3stl355/delta-rs/justfile/data/nyc_green_trips/nyc_taxi_green.parquet"
NYC_TAXI_SOUCE_PYARROW = "/Users/nikolay.ulmasov/work/REPOS/r3stl355/delta-rs/justfile/data/nyc_green_trips/nyc_taxi_green-pyarrow.parquet"

BASE_TARGET_URI = "abfs://temp/nyc_taxi"

def write_nyc_to_azure(local_path, loops, base_target_uri, schema, partion_by):
    from deltalake import write_deltalake
    dataset = ds.dataset(local_path)

    for i in range(0, loops):
        print(f"- Loop {i}: [pid: {os.getpid()}] {dt.datetime.now()}")
        write_deltalake(
            data=dataset.to_batches(),
            table_or_uri=base_target_uri + f"_delta_rs_{i}",
            mode="overwrite",
            storage_options=storage_options,
            schema=schema,
            partition_by=partion_by,
        )

def write_nyc_to_azure_pyarrow(local_path, loops, base_target_uri, schema, partition_by):
    import pyarrow.fs as pa_fs

    dataset = ds.dataset(local_path)

    basename_template = f"bla-{uuid.uuid4()}-{{i}}.parquet"
    partition_schema = pa.schema([schema.field(name) for name in partition_by])
    partitioning = ds.partitioning(partition_schema, flavor="hive")
    file_options = ds.ParquetFileFormat().make_write_options(use_compliant_nested_type=False)

    #filesystem = pa_fs.PyFileSystem(pa_fs.FileSystemHandler(base_target_uri + "_pyarrow", storage_options))
    
    for i in range(0, loops):
        print(f"- Loop {i}: [pid: {os.getpid()}] {dt.datetime.now()}")
        
        ds.write_dataset(
            dataset,
            base_dir="/",
            basename_template=basename_template,
            format="parquet",
            partitioning=partitioning,
            schema=schema,
            file_visitor=None,
            existing_data_behavior="overwrite_or_ignore",
            file_options=file_options,
            max_open_files=1024,
            max_rows_per_file=10 * 1024 * 1024,
            min_rows_per_group=64 * 1024,
            max_rows_per_group=128 * 1024,
            filesystem=filesystem,
            max_partitions=None,
        )


def write_nyc_to_azure_adlfs(local_path, loops, base_target_uri, schema, partition_by):
    # https://pypi.org/project/adlfs/
    #   https://github.com/fsspec/adlfs/blob/092685f102c5cd215550d10e8347e5bce0e2b93d/adlfs/spec.py#L120

    from adlfs import AzureBlobFileSystem, AzureDatalakeFileSystem

    dataset = ds.dataset(local_path)

    basename_template = f"bla-{uuid.uuid4()}-{{i}}.parquet"
    partition_schema = pa.schema([schema.field(name) for name in partition_by])
    partitioning = ds.partitioning(partition_schema, flavor="hive")
    file_options = ds.ParquetFileFormat().make_write_options(use_compliant_nested_type=False)

    filesystem = AzureBlobFileSystem(account_name=account_name, tenant_id=tenant, client_id=client, client_secret=secret, anon=False, max_concurrency=1024)

    for i in range(0, loops):
        print(f"- Loop {i}: {dt.datetime.now()}")
        ds.write_dataset(
            data=dataset,
            base_dir=f"temp/nyc_taxi_adlfs_{i}",
            basename_template=basename_template,
            format="parquet",
            partitioning=partitioning,
            schema=schema,
            file_visitor=None,
            existing_data_behavior="overwrite_or_ignore",
            file_options=file_options,
            max_open_files=1024,
            max_rows_per_file=10 * 1024 * 1024,
            min_rows_per_group=64 * 1024,
            max_rows_per_group=128 * 1024,
            filesystem=filesystem,
            max_partitions=None
        )

if __name__ == "__main__":
    # target_uri = "abfs://temp/generated"
    base_target_uri = "abfs://temp/nyc_taxi"
    loops = 100
    write_nyc_to_azure(NYC_TAXI_SOUCE_PYARROW, loops, BASE_TARGET_URI, NYC_TAXI_SCHEMA , ["year_month"])
    # write_nyc_to_azure_pyarrow(NYC_TAXI_SOUCE_PYARROW, loops, BASE_TARGET_URI, NYC_TAXI_SCHEMA , ["year_month"])
    # write_nyc_to_azure_adlfs(NYC_TAXI_SOUCE_PYARROW, loops, BASE_TARGET_URI, NYC_TAXI_SCHEMA , ["year_month"])