CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dbo;

CREATE TABLE IF NOT EXISTS staging.event_gallery_files (
    id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dbo.event_gallery_files (
    id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    path TEXT NOT NULL
);

CREATE OR REPLACE PROCEDURE staging.migrate_event_gallery_files()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO dbo.event_gallery_files AS target
    USING staging.event_gallery_files AS source
    ON target.file_name = source.file_name
    WHEN MATCHED THEN
        UPDATE SET path = source.path
    WHEN NOT MATCHED THEN
        INSERT (file_name, path)
        VALUES (source.file_name, source.path);

    TRUNCATE staging.event_gallery_files;
END;
$$;