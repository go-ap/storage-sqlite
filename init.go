package sqlite

const (
	createActorsQuery = `
CREATE TABLE actors (
  "raw" BLOB,
  "meta" BLOB,
  "iri" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.id')) VIRTUAL NOT NULL constraint actors_key unique,
  "type" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.type')) VIRTUAL NOT NULL,
  "to" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.to')) VIRTUAL,
  "bto" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.bto')) VIRTUAL,
  "cc" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.cc')) VIRTUAL,
  "bcc" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.bcc')) VIRTUAL,
  "published" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.published')) VIRTUAL,
  "updated" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.updated')) VIRTUAL,
  "url" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.url')) VIRTUAL,
  "name" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.name')) VIRTUAL,
  "preferred_username" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.preferredUsername')) VIRTUAL
) STRICT;
-- CREATE INDEX actors_type ON actors(type);
-- CREATE INDEX actors_published ON actors(published);
`

	createActivitiesQuery = `
CREATE TABLE activities (
  "raw" BLOB,
  "meta" BLOB,
  "iri" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.id')) VIRTUAL NOT NULL constraint activities_key unique,
  "type" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.type')) VIRTUAL NOT NULL,
  "to" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.to')) VIRTUAL,
  "bto" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.bto')) VIRTUAL,
  "cc" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.cc')) VIRTUAL,
  "bcc" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.bcc')) VIRTUAL,
  "published" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.published')) VIRTUAL,
  "updated" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.updated')) VIRTUAL,
  "url" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.url')) VIRTUAL,
  "actor" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.actor')) VIRTUAL NOT NULL CONSTRAINT activities_actors_iri_fk REFERENCES actors (iri),
  "object" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.object')) VIRTUAL CONSTRAINT activities_objects_iri_fk REFERENCES objects (iri)
) STRICT;
-- CREATE INDEX activities_type ON activities(type);
-- CREATE INDEX activities_actor ON activities(actor);
-- CREATE INDEX activities_object ON activities(object);
-- CREATE INDEX activities_published ON activities(published);
`

	createObjectsQuery = `
CREATE TABLE objects (
  "raw" BLOB,
  "meta" BLOB,
  "iri" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.id')) VIRTUAL NOT NULL constraint objects_key unique,
  "type" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.type')) VIRTUAL,
  "to" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.to')) VIRTUAL,
  "bto" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.bto')) VIRTUAL,
  "cc" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.cc')) VIRTUAL,
  "bcc" BLOB GENERATED ALWAYS AS (json_extract(raw, '$.bcc')) VIRTUAL,
  "published" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.published')) VIRTUAL,
  "updated" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.updated')) VIRTUAL,
  "url" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.url')) VIRTUAL,
  "name" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.name')) VIRTUAL,
  "summary" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.summary')) VIRTUAL,
  "content" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.content')) VIRTUAL
) STRICT;
-- CREATE INDEX objects_type ON objects(type);
-- CREATE INDEX objects_name ON objects(name);
-- CREATE INDEX objects_content ON objects(content);
-- CREATE INDEX objects_published ON objects(published);
`

	createCollectionsQuery = `
CREATE TABLE collections (
  "published" TEXT default CURRENT_TIMESTAMP,
  "raw" BLOB,
  "items" BLOB,
  "iri" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.id')) VIRTUAL NOT NULL constraint collections_key unique
) STRICT;

-- CREATE TRIGGER collections_updated_published AFTER UPDATE ON collections BEGIN
--   UPDATE collections SET published = strftime('%Y-%m-%dT%H:%M:%fZ') WHERE iri = old.iri;
-- END;`

	tuneQuery = `
-- Use WAL mode (writers don't block readers):
--PRAGMA journal_mode = DELETE;
-- Use memory as temporary storage:
PRAGMA temp_store = 2;
-- Faster synchronization that still keeps the data safe:
--PRAGMA synchronous = 1;
-- Increase cache size (in this case to 64MB), the default is 2MB
PRAGMA cache_size = -64000;
-- from BJohnson's recommendations to use with litestream
PRAGMA journal_mode = wal;
PRAGMA busy_timeout = 5000;
PRAGMA wal_autocheckpoint = 0;
PRAGMA strict=ON;
--PRAGMA synchronous = NORMAL;
`
)
