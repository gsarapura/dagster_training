"""ETL Tutorial"""
from dagster_duckdb import DuckDBResource

import dagster as dg

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
)
def products(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """
    First, we will create an asset that creates a DuckDB table to hold data from 
    the products CSV. This asset takes the duckdb resource defined earlier 
    and returns a MaterializeResult object. Additionally, this asset contains 
    metadata in the @dg.asset decorator parameters to help categorize the asset, 
    and in the return block to give us a preview of the asset in the Dagster UI.
    """
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace table products as (
                select * from read_csv_auto('data/products.csv')
            )
            """
        )

        preview_query = "select * from products limit 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("select count(*) from products").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
)
def sales_reps(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """
    The code for the sales reps asset is similar to the product asset code. 
    In the definitions.py file, copy the following code below the product asset code:
    """
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace table sales_reps as (
                select * from read_csv_auto('data/sales_reps.csv')
            )
            """
        )

        preview_query = "select * from sales_reps limit 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("select count(*) from sales_reps").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
)
def sales_data(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """
    To add the sales data asset, copy the following code into your 
    definitions.py file below the sales reps asset:
    """
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            drop table if exists sales_data;
            create table sales_data as select * from read_csv_auto('data/sales_data.csv')
            """
        )

        preview_query = "SELECT * FROM sales_data LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("select count(*) from sales_data").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="joins",
    deps=[sales_data, sales_reps, products],
)
def joined_data(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """
    As you can see, the new joined_data asset looks a lot 
    like our previous ones, with a few small changes. We 
    put this asset into a different group. To make this a
    sset dependent on the raw tables, we add the asset 
    keys to the deps parameter in the asset definition.
    """
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace view joined_data as (
                select 
                    date,
                    dollar_amount,
                    customer_name,
                    quantity,
                    rep_name,
                    department,
                    hire_date,
                    product_name,
                    category,
                    price
                from sales_data
                left join sales_reps
                    on sales_reps.rep_id = sales_data.rep_id
                left join products
                    on products.product_id = sales_data.product_id
            )
            """
        )

        preview_query = "select * from joined_data limit 10"
        preview_df = conn.execute(preview_query).fetchdf()

        row_count = conn.execute("select count(*) from joined_data").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )


defs = dg.Definitions(
    assets=[products,
        sales_reps,
        sales_data,
        joined_data
    ],
    resources={"duckdb": DuckDBResource(database="data/mydb.duckdb")},
)
