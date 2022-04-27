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
  "transaction_index" bigint,
  "starting_date" date
);

COPY secondary_hash TO '{destination_directory}/inverse_users.csv'  WITH DELIMITER ',' CSV HEADER;
COPY main_hash TO '{destination_directory}/users.csv'  WITH DELIMITER ',' CSV HEADER;

-- INSERT INTO history VALUES (1650513600000, 3, 2);
-- INSERT INTO history VALUES (0, 0, 0, null);
-- UPDATE history SET (particular_day_seconds, block_index, transaction_index) = (2650513600000, 3, 2) where particular_day_seconds = 1650513600000;