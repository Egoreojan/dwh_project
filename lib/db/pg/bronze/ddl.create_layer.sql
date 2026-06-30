BEGIN;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.nyt_articles_h0 (
    id serial primary key,
    asset_id int not null,
    url varchar(255) not null,
    source varchar(50) not null,
    publish_date date not null,
    updated timestamp not null,
    section varchar(50) not null,
    title varchar(255) not null,
    abstract text
);

COMMIT;

