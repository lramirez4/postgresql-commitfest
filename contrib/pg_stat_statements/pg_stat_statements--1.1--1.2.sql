/* contrib/pg_stat_statements/pg_stat_statements--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_stat_statements UPDATE" to load this file. \quit

/* First we have to remove them from the extension */
ALTER EXTENSION pg_stat_statements DROP VIEW pg_stat_statements;
ALTER EXTENSION pg_stat_statements DROP FUNCTION pg_stat_statements();

/* Then we can drop them */
DROP VIEW pg_stat_statements;
DROP FUNCTION pg_stat_statements();

/* Now redefine */
CREATE FUNCTION pg_stat_statements(
    OUT userid oid,
    OUT dbid oid,
    OUT query text,
    OUT calls int8,
    OUT total_time float8,
    OUT rows int8,
    OUT shared_blks_hit int8,
    OUT shared_blks_read int8,
    OUT shared_blks_dirtied int8,
    OUT shared_blks_written int8,
    OUT local_blks_hit int8,
    OUT local_blks_read int8,
    OUT local_blks_dirtied int8,
    OUT local_blks_written int8,
    OUT temp_blks_read int8,
    OUT temp_blks_written int8,
    OUT blk_read_time float8,
    OUT blk_write_time float8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE VIEW pg_stat_statements AS
  SELECT userid,
         dbid,
         query,
         calls,
         total_time,
         total_time / calls::float AS avg_time,
         rows,
         CASE WHEN shared_blks_hit + shared_blks_read > 0
           THEN 100.0 * (shared_blks_hit::float / (shared_blks_hit + shared_blks_read))
           ELSE 0 END AS shared_blks_hit_percent,
         shared_blks_hit,
         shared_blks_read,
         shared_blks_dirtied,
         shared_blks_written,
         local_blks_hit,
         local_blks_read,
         local_blks_dirtied,
         local_blks_written,
         temp_blks_read,
         temp_blks_written,
         blk_read_time,
         blk_write_time
  FROM pg_stat_statements();

GRANT SELECT ON pg_stat_statements TO PUBLIC;
