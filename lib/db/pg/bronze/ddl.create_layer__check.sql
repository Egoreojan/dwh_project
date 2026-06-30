SELECT
    table_name
FROM information_schema.tables
WHERE table_schema = 'bronze'
    AND table_name = 'nyt_articles_h0';

