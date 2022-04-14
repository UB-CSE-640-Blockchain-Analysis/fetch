CREATE TABLE "main_hash" (
  "main_address" text PRIMARY KEY,
  "secondary_addresses" text[]
);

CREATE TABLE "secondary_hash" (
  "secondary_address" text PRIMARY KEY,
  "main_address" text
);

CREATE INDEX ON "main_hash" ("main_address");
