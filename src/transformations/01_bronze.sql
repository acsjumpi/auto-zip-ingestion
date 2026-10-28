-- Bronze: Auto Loader over the extracted CSVs.
-- Each run picks up only files it has not seen (checkpointed) — no control-table bookkeeping needed for ingest.
-- All columns land as STRING; duplicates are kept here for audit.
-- ${key_column} and ${staged_path} come from the pipeline configuration (bundle variables).

CREATE OR REFRESH STREAMING TABLE bronze_records (
  CONSTRAINT key_present EXPECT (${key_column} IS NOT NULL)
)
COMMENT 'Raw HIST/ADD/CHG records, one row per source line, all columns STRING'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT
  *,
  xxhash64(*) AS _row_hash,  -- full-record hash, used to measure exact duplicates
  regexp_extract(_metadata.file_path, '/staged/([^/]+)/([^/]+)/', 1) AS load_id,
  regexp_extract(_metadata.file_path, '/staged/([^/]+)/([^/]+)/', 2) AS file_type,
  _metadata.file_name AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${staged_path}/*/{HIST,ADD,CHG}/*.csv',
  format => 'csv',
  header => true,
  inferColumnTypes => false
);

CREATE OR REFRESH STREAMING TABLE bronze_deletes (
  CONSTRAINT key_present EXPECT (${key_column} IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Raw DEL records (key column only)'
AS SELECT
  ${key_column},
  regexp_extract(_metadata.file_path, '/staged/([^/]+)/', 1) AS load_id,
  'DEL' AS file_type,
  _metadata.file_name AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${staged_path}/*/DEL/*.csv',
  format => 'csv',
  header => true,
  schema => '${key_column} STRING'  -- explicit: the DEL folder is empty until the first incremental load
);
