# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Unzip deliveries into the staging volume
# MAGIC
# MAGIC Auto Loader reads gzip/bzip2-style *compressed files* natively, but **not `.zip` archives**, so this step
# MAGIC replaces a typical "unzip → trim → dedup → stitch parquet" pre-processor with **unzip only**:
# MAGIC
# MAGIC * discovers `.zip` files under `landing/<load_id>/` that are not yet in the `zip_manifest` control table
# MAGIC * streams each member to `staged/<load_id>/<HIST|ADD|CHG|DEL>/` (constant memory, no full read)
# MAGIC * unzips several files in parallel (zlib releases the GIL)
# MAGIC * writes to a `.inprogress` name and renames when complete, so Auto Loader never sees a partial file
# MAGIC * records every zip in `zip_manifest` (idempotent re-runs; pass `reprocess` to force a zip again)
# MAGIC
# MAGIC Trim and dedup move **into the pipeline** (column selection + AUTO CDC), so there is no parquet stitching step.

# COMMAND ----------

dbutils.widgets.text("catalog", "")  # set by the job (bundle variable `catalog`)
dbutils.widgets.text("schema", "zip_ingest")
dbutils.widgets.text("max_workers", "8")
dbutils.widgets.text("reprocess", "")  # comma-separated zip names, e.g. "CHG_01.zip"
dbutils.widgets.text("load_ids", "")   # optional: only these load folders, e.g. "2026-06"

import os, shutil, time, zipfile, uuid, json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
assert CATALOG and SCHEMA, "Set the catalog and schema widgets (the job passes them automatically)"
MAX_WORKERS = int(dbutils.widgets.get("max_workers"))
LOAD_IDS = {s.strip() for s in dbutils.widgets.get("load_ids").split(",") if s.strip()}
REPROCESS = {s.strip() for s in dbutils.widgets.get("reprocess").split(",") if s.strip()}
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
STAGED = f"/Volumes/{CATALOG}/{SCHEMA}/staged"
MANIFEST = f"{CATALOG}.{SCHEMA}.zip_manifest"
FILE_TYPES = {"HIST", "ADD", "CHG", "DEL"}
RUN_ID = str(uuid.uuid4())

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MANIFEST} (
  zip_path STRING, load_id STRING, file_type STRING, member STRING, staged_path STRING,
  zip_bytes BIGINT, csv_bytes BIGINT, status STRING, error STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP, seconds DOUBLE, run_id STRING)
COMMENT 'Control table: one row per extracted zip member (status SUCCESS/FAILED)'
""")

# COMMAND ----------

done = {r.zip_path for r in spark.sql(f"SELECT DISTINCT zip_path FROM {MANIFEST} WHERE status = 'SUCCESS'").collect()}

todo = []
for load_id in sorted(os.listdir(LANDING)):
    if load_id.startswith(("_", ".")) or not os.path.isdir(f"{LANDING}/{load_id}"):
        continue
    if LOAD_IDS and load_id not in LOAD_IDS:
        continue
    for name in sorted(os.listdir(f"{LANDING}/{load_id}")):
        path = f"{LANDING}/{load_id}/{name}"
        file_type = name.split("_")[0].upper()
        if not name.lower().endswith(".zip") or file_type not in FILE_TYPES:
            continue
        if path in done and name not in REPROCESS:
            continue
        todo.append((path, load_id, file_type))

print(f"{len(todo)} zip(s) to extract, {len(done)} already done")
for t in todo:
    print("  ", t[0])

# COMMAND ----------

def extract(path, load_id, file_type):
    rows = []
    out_dir = f"{STAGED}/{load_id}/{file_type}"  # plain folders: "k=v" names would be auto-inferred as partition columns
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.basename(path)[:-4]
    try:
        with zipfile.ZipFile(path) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                started = datetime.now(timezone.utc)
                t0 = time.time()
                final = f"{out_dir}/{stem}__{os.path.basename(info.filename)}"
                tmp = final + ".inprogress"  # not matched by the pipeline's *.csv glob
                with z.open(info) as src, open(tmp, "wb") as dst:
                    shutil.copyfileobj(src, dst, 64 * 1024 * 1024)
                os.replace(tmp, final)
                rows.append((path, load_id, file_type, info.filename, final, os.path.getsize(path),
                             info.file_size, "SUCCESS", None, started, datetime.now(timezone.utc),
                             round(time.time() - t0, 1), RUN_ID))
    except Exception as e:
        now = datetime.now(timezone.utc)
        rows.append((path, load_id, file_type, None, None, os.path.getsize(path), None, "FAILED",
                     str(e)[:1000], now, now, 0.0, RUN_ID))
    return rows

t0 = time.time()
results = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    for rows in ex.map(lambda t: extract(*t), todo):
        results.extend(rows)
        for r in rows:
            print(f"{r[7]:7s} {r[0]} -> {r[4]}  {(r[6] or 0)/1e9:.2f} GB in {r[11]}s {r[8] or ''}")
wall = time.time() - t0

if results:
    spark.createDataFrame(results, spark.table(MANIFEST).schema).write.mode("append").saveAsTable(MANIFEST)

# COMMAND ----------

ok = [r for r in results if r[7] == "SUCCESS"]
summary = {
    "zips": len(todo), "members_ok": len(ok), "failed": len(results) - len(ok),
    "csv_gb": round(sum(r[6] for r in ok) / 1e9, 2), "zip_gb": round(sum({r[0]: r[5] for r in ok}.values()) / 1e9, 2),
    "wall_seconds": round(wall, 1),
}
summary["csv_gb_per_min"] = round(summary["csv_gb"] / (wall / 60), 2) if wall > 1 else None
print(json.dumps(summary, indent=1))
dbutils.jobs.taskValues.set("unzip_summary", summary)
if summary["failed"]:
    raise RuntimeError(f"{summary['failed']} zip(s) failed — see {MANIFEST}")
