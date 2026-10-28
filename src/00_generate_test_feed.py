# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — OPTIONAL: generate a synthetic .zip delivery for testing (not part of the production flow)
# MAGIC
# MAGIC Skip this notebook if you already have real zip files — upload those to the `landing` volume instead.
# MAGIC
# MAGIC Builds two loads in the `landing` volume from fully synthetic data (no input files needed):
# MAGIC
# MAGIC | load_id | files | contents |
# MAGIC |---|---|---|
# MAGIC | `2026-05_hist` | `HIST_01.zip`, `HIST_02.zip` | `hist_rows` historic records |
# MAGIC | `2026-06` | `ADD_01.zip`, `ADD_02.zip` | `add_rows` new records |
# MAGIC | `2026-06` | `CHG_01.zip` | ~3% of historic keys with changed values **+ exact copies of ADD rows (cross-file dups) + intra-file dups** |
# MAGIC | `2026-06` | `DEL_01.zip` | ~0.5% of historic keys, key column only |
# MAGIC
# MAGIC Expected end-state counts are written to `gen_expected` so the pipeline result can be checked.
# MAGIC The key column is named `id` (the bundle's default `key_column`).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "zip_ingest")
dbutils.widgets.text("hist_rows", "2000000")
dbutils.widgets.text("add_rows", "1000000")
dbutils.widgets.text("columns", "40")

import os, shutil, tempfile, zipfile, time, json
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F

CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
assert CATALOG and SCHEMA, "Fill in the catalog and schema widgets"
HIST, ADD, NCOLS = (int(dbutils.widgets.get(w)) for w in ("hist_rows", "add_rows", "columns"))
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
GEN_TMP = f"{LANDING}/_gen_tmp"          # leading "_" -> ignored by the unzip discovery
LOCAL = tempfile.mkdtemp(prefix="zipgen_")  # zips are built on local disk (volumes don't support random writes)

# COMMAND ----------

# MAGIC %md ## Synthetic records (Spark)

# COMMAND ----------

def records(start, n):
    """n records with a string key `id`, NCOLS deterministic attribute columns and an `updated_at` date."""
    df = spark.range(start, start + n)
    attrs = [F.substring(F.sha2(F.concat_ws("|", F.col("id").cast("string"), F.lit(str(i))), 256), 1, 12).alias(f"attr_{i:02d}")
             for i in range(1, NCOLS + 1)]
    return df.select(F.format_string("K%016d", "id").alias("id"), *attrs,
                     F.lit("2026-05-31").alias("updated_at"), F.col("id").alias("_n"))

hist = records(0, HIST)
adds = records(HIST, ADD)

dels = hist.where(F.col("_n") % 200 == 0).select("id")                     # 0.5% of historic keys
chg = (hist.where(F.col("_n") % 100 >= 97)                                 # 3% of historic keys (disjoint from deletes)
       .withColumn("attr_01", F.upper("attr_01"))
       .withColumn("updated_at", F.lit("2026-06-30")))
cross_dups = adds.where(F.col("_n") % 1000 < 6)                            # exact copies of ADD rows inside CHG files
intra_dups = chg.where(F.col("_n") % 7 == 0)                               # duplicated rows inside CHG files
cols = [c for c in hist.columns if c != "_n"]
chg_all = chg.unionByName(cross_dups).unionByName(intra_dups).select(cols)

for name, df, parts in [("hist", hist.select(cols), 2), ("add", adds.select(cols), 2),
                        ("chg", chg_all, 1), ("del", dels, 1)]:
    df.repartition(parts).write.mode("overwrite").option("header", True).csv(f"{GEN_TMP}/{name}")

n = {k: v.count() for k, v in {"hist": hist, "adds": adds, "dels": dels, "chg_keys": chg,
                               "cross_dups": cross_dups, "intra_dups": intra_dups}.items()}
n["expected_silver_after_hist"] = n["hist"]
n["expected_silver_after_june"] = n["hist"] + n["adds"] - n["dels"]
n["expected_june_upsert_rows_received"] = n["adds"] + n["chg_keys"] + n["cross_dups"] + n["intra_dups"]
n["expected_june_exact_duplicates"] = n["cross_dups"] + n["intra_dups"]
print(json.dumps(n, indent=1))
spark.createDataFrame([(k, int(v)) for k, v in n.items()], "metric string, value long") \
     .write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gen_expected")

# COMMAND ----------

# MAGIC %md ## Build the zips (one CSV member per zip, parallel)

# COMMAND ----------

def build_zip(src_file, zip_path, member):
    """Zip one CSV file locally, then copy the zip to the volume."""
    t0 = time.time()
    local_zip = os.path.join(LOCAL, os.path.basename(zip_path))
    with zipfile.ZipFile(local_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True) as z:
        with z.open(member, "w", force_zip64=True) as out, open(src_file, "rb") as f:
            shutil.copyfileobj(f, out, 16 * 1024 * 1024)
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    shutil.copyfile(local_zip, zip_path)
    os.remove(local_zip)
    return zip_path, os.path.getsize(zip_path), round(time.time() - t0, 1)

def spark_parts(d):
    return sorted(f"{d}/{f}" for f in os.listdir(d) if f.startswith("part-") and f.endswith(".csv"))

jobs = []
for name, load_id, prefix in [("hist", "2026-05_hist", "HIST"), ("add", "2026-06", "ADD"),
                              ("chg", "2026-06", "CHG"), ("del", "2026-06", "DEL")]:
    for i, p in enumerate(spark_parts(f"{GEN_TMP}/{name}")):
        jobs.append((p, f"{LANDING}/{load_id}/{prefix}_{i+1:02d}.zip", f"{prefix}_{i+1:02d}.csv"))

t0 = time.time()
with ThreadPoolExecutor(max_workers=4) as ex:
    for path, size, secs in ex.map(lambda j: build_zip(*j), jobs):
        print(f"{path}  {size/1e6:.1f} MB  {secs}s")
print(f"built {len(jobs)} zips in {time.time()-t0:.0f}s")
dbutils.fs.rm(GEN_TMP, True)
