-- error message contains path, if extension isn't available
DO $$
BEGIN
   IF EXISTS(SELECT * FROM pg_available_extensions WHERE name = 'test_logical_decoding') THEN
      CREATE EXTENSION test_logical_decoding;
   END IF;
END;
$$;

SELECT 'stop' FROM stop_logical_replication('regression_slot');
SELECT 'init' FROM init_logical_replication('regression_slot', 'test_decoding');
COPY (SELECT data FROM start_logical_replication('regression_slot', 'now', 'include-xids', '0')) TO STDOUT;
