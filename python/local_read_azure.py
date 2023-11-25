import os
# import polars as pl
import pyarrow.dataset as ds
from deltalake import DeltaTable
# import pyarrow as pa

    # https://delta-io.github.io/delta-rs/python/usage.html#querying-delta-tables

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
client = os.getenv("AZURE_CLIENT_ID")
tenant = os.getenv("AZURE_TENANT_ID")
secret = os.getenv("AZURE_CLIENT_SECRET")

storage_options = {"account_name": account_name, "client_id": client, "client_secret": secret, "tenant_id": tenant}

table = DeltaTable(
    table_uri="abfs://temp/testDelta",
    storage_options=storage_options,
)
dataset = table.to_pyarrow_dataset()

# Count rows
row_count = dataset.count_rows()
print(f"There are {row_count:,} rows")

# Convert to Pandas
# condition = (ds.field("year") == "2021") & (ds.field("value") > "4")
# dataset.to_table(filter=condition, columns=["value"]).to_pandas()

# Qeury with DuckDB
# import duckdb
# ex_data = duckdb.arrow(dataset)
# ex_data.filter("year = 2021 and value > 4").project("value")