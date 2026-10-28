-- Per-load audit: what was received vs. what was deduplicated vs. what was deleted.
-- Replaces manual count checks after a separate dedup step.

CREATE OR REFRESH MATERIALIZED VIEW gold_load_audit
COMMENT 'Per load: files, rows received by type, exact duplicates, distinct keys, deletes'
AS
WITH upserts AS (
  SELECT
    load_id,
    count(DISTINCT _source_file) AS upsert_files,
    count_if(file_type = 'HIST') AS hist_rows,
    count_if(file_type = 'ADD') AS add_rows,
    count_if(file_type = 'CHG') AS chg_rows,
    count(*) AS upsert_rows_received,
    count(DISTINCT _row_hash) AS distinct_records,
    count(*) - count(DISTINCT _row_hash) AS exact_duplicate_rows,
    count(DISTINCT ${key_column}) AS distinct_keys
  FROM bronze_records
  GROUP BY load_id
),
deletes AS (
  SELECT load_id, count(DISTINCT _source_file) AS delete_files, count(*) AS delete_rows,
         count(DISTINCT ${key_column}) AS distinct_delete_keys
  FROM bronze_deletes
  GROUP BY load_id
)
SELECT
  coalesce(u.load_id, d.load_id) AS load_id,
  u.* EXCEPT (load_id),
  coalesce(d.delete_files, 0) AS delete_files,
  coalesce(d.delete_rows, 0) AS delete_rows,
  coalesce(d.distinct_delete_keys, 0) AS distinct_delete_keys
FROM upserts u
FULL OUTER JOIN deletes d ON u.load_id = d.load_id;
