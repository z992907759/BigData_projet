

-- ex03_sql_table_creation/insertion.sql
-- EX3: insert business reference data ("données de référence")
--
-- Notes:
-- - We use natural IDs provided by the TLC contract/data dictionary.
-- - Surrogate keys (SERIAL *_key) are generated automatically.
-- - ON CONFLICT DO NOTHING makes the script idempotent.

BEGIN;

-- 1) Vendors (TLC Yellow Taxi)
-- Commonly: 1 = Creative Mobile Technologies, LLC ; 2 = VeriFone Inc.
INSERT INTO dwh.dim_vendor (vendor_id) VALUES
  (1),
  (2)
ON CONFLICT (vendor_id) DO NOTHING;

-- 2) Rate codes (TLC contract)
-- 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group
INSERT INTO dwh.dim_rate_code (rate_code_id) VALUES
  (1),
  (2),
  (3),
  (4),
  (5),
  (6)
ON CONFLICT (rate_code_id) DO NOTHING;

-- 3) Payment types (TLC contract)
-- 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip
INSERT INTO dwh.dim_payment_type (payment_type_id) VALUES
  (1),
  (2),
  (3),
  (4),
  (5),
  (6)
ON CONFLICT (payment_type_id) DO NOTHING;

-- 4) Taxi zone lookup (optional but recommended)
-- If your teacher/team provided the "taxi_zone_lookup.csv" file, you can load it here.
-- Your dim_location only stores the numeric LocationID, so we insert LocationID values.
--
-- Option A (recommended): load all LocationID from a CSV staged inside the Postgres container.
--   1) Copy the file into the container:
--        docker cp references/taxi_zone_lookup.csv bigdata-postgres:/tmp/taxi_zone_lookup.csv
--   2) Then run in psql:
--        \copy (SELECT DISTINCT CAST(locationid AS INT) AS location_id FROM csvread) TO ...
--   For simplicity in pure SQL, we just provide a COPY pattern using a temp table.
--
-- Uncomment this block if you have the file inside the container at /tmp/taxi_zone_lookup.csv
--
-- CREATE TEMP TABLE tmp_zone_lookup(
--   locationid TEXT,
--   borough    TEXT,
--   zone       TEXT,
--   service_zone TEXT
-- );
--
-- COPY tmp_zone_lookup(locationid, borough, zone, service_zone)
-- FROM '/tmp/taxi_zone_lookup.csv'
-- WITH (FORMAT csv, HEADER true);
--
-- INSERT INTO dwh.dim_location(location_id)
-- SELECT DISTINCT CAST(locationid AS INT)
-- FROM tmp_zone_lookup
-- WHERE locationid ~ '^[0-9]+$'
-- ON CONFLICT (location_id) DO NOTHING;

COMMIT;