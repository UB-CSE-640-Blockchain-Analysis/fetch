CREATE TABLE "main_hash" (
  "main_address" text PRIMARY KEY,
  "secondary_addresses" text[]
);

CREATE TABLE "secondary_hash" (
  "secondary_address" text PRIMARY KEY,
  "main_address" text
);

CREATE INDEX ON "main_hash" ("main_address");

CREATE INDEX ON "secondary_hash" ("secondary_address");

CREATE TABLE "history" (
  "particular_day_seconds" bigint,
  "block_index" bigint,
  "transaction_index" bigint
);
-- INSERT INTO history VALUES (1650513600000, 3, 2);
-- UPDATE history SET (particular_day_seconds, block_index, transaction_index) = (2650513600000, 3, 2) where particular_day_seconds = 1650513600000;