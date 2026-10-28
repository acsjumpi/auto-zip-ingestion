# Silver: one current row per key.
# HIST/ADD/CHG upserts and key-only deletes are merged into one ordered change feed and applied with AUTO CDC:
#   * order within a load: HIST/ADD (1) -> CHG (2) -> DEL (3); across loads by load_id
#   * exact duplicate rows (within a file, or the same record in ADD and CHG) collapse onto the same key,
#     which replaces a separate dedup + "stitch" pre-processing step

import dlt
from pyspark.sql import functions as F

KEY = spark.conf.get("key_column")

FILE_RANK = (F.when(F.col("file_type") == "CHG", 2)
              .when(F.col("file_type") == "DEL", 3)
              .otherwise(1))


@dlt.view(comment="Ordered change feed: HIST/ADD/CHG upserts + DEL deletes")
@dlt.expect_or_drop("key_present", f"`{KEY}` IS NOT NULL")
def change_feed():
    upserts = spark.readStream.table("bronze_records").withColumn("_op", F.lit("UPSERT"))
    deletes = spark.readStream.table("bronze_deletes").withColumn("_op", F.lit("DELETE"))
    return (upserts.unionByName(deletes, allowMissingColumns=True)
            .withColumn("_seq", F.struct(F.col("load_id"), FILE_RANK.alias("file_rank"))))


dlt.create_streaming_table(
    name="silver_current",
    comment="Current record per key (SCD1). CDF on for Lakebase synced tables / downstream incremental reads.",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.columnMapping.mode": "name"},
    cluster_by=[KEY],
)

dlt.create_auto_cdc_flow(
    target="silver_current",
    source="change_feed",
    keys=[KEY],
    sequence_by="_seq",
    apply_as_deletes=F.expr("_op = 'DELETE'"),
    except_column_list=["_op", "_seq"],
    stored_as_scd_type=1,
)
