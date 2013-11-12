CREATE EXTENSION hstore_plperl;
CREATE EXTENSION hstore_plperlu;


-- test hstore -> perl
CREATE FUNCTION test1(val hstore) RETURNS int
LANGUAGE plperlu
AS $$
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
elog(INFO, Dumper($_[0]));
return scalar(keys %{$_[0]});
$$;

SELECT test1('aa=>bb, cc=>NULL'::hstore);


-- test hstore[] -> perl
CREATE FUNCTION test1arr(val hstore[]) RETURNS int
LANGUAGE plperlu
AS $$
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
elog(INFO, Dumper($_[0]->[0], $_[0]->[1]));
return scalar(keys %{$_[0]});
$$;

SELECT test1arr(array['aa=>bb, cc=>NULL'::hstore, 'dd=>ee']);


-- test perl -> hstore
CREATE FUNCTION test2() RETURNS hstore
LANGUAGE plperl
AS $$
$val = {a => 1, b => 'boo', c => undef};
return $val;
$$;

SELECT test2();


-- test perl -> hstore[]
CREATE FUNCTION test2arr() RETURNS hstore[]
LANGUAGE plperl
AS $$
$val = [{a => 1, b => 'boo', c => undef}, {d => 2}];
return $val;
$$;

SELECT test2arr();


-- test as part of prepare/execute
CREATE FUNCTION test3() RETURNS void
LANGUAGE plperlu
AS $$
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

$rv = spi_exec_query(q{SELECT 'aa=>bb, cc=>NULL'::hstore AS col1});
elog(INFO, Dumper($rv->{rows}[0]->{col1}));

$val = {a => 1, b => 'boo', c => undef};
$plan = spi_prepare(q{SELECT $1::text AS col1}, "hstore");
$rv = spi_exec_prepared($plan, {}, $val);
elog(INFO, Dumper($rv->{rows}[0]->{col1}));
$$;

SELECT test3();


-- test inline
DO LANGUAGE plperlu $$
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

$rv = spi_exec_query(q{SELECT 'aa=>bb, cc=>NULL'::hstore AS col1});
elog(INFO, Dumper($rv->{rows}[0]->{col1}));

$val = {a => 1, b => 'boo', c => undef};
$plan = spi_prepare(q{SELECT $1::text AS col1}, "hstore");
$rv = spi_exec_prepared($plan, {}, $val);
elog(INFO, Dumper($rv->{rows}[0]->{col1}));
$$;


-- test trigger
CREATE TABLE test1 (a int, b hstore);
INSERT INTO test1 VALUES (1, 'aa=>bb, cc=>NULL');
SELECT * FROM test1;

CREATE FUNCTION test4() RETURNS trigger
LANGUAGE plperlu
AS $$
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
elog(INFO, Dumper($_TD->{new}));
if ($_TD->{new}{a} == 1) {
    $_TD->{new}{b} = {a => 1, b => 'boo', c => undef};
}

return "MODIFY";
$$;

CREATE TRIGGER test4 BEFORE UPDATE ON test1 FOR EACH ROW EXECUTE PROCEDURE test4();

UPDATE test1 SET a = a;
SELECT * FROM test1;


DROP TABLE test1;

DROP FUNCTION test1(hstore);
DROP FUNCTION test1arr(hstore[]);
DROP FUNCTION test2();
DROP FUNCTION test2arr();
DROP FUNCTION test3();
DROP FUNCTION test4();


DROP EXTENSION hstore_plperl;
DROP EXTENSION hstore_plperlu;
DROP EXTENSION hstore;
DROP EXTENSION plperl;
DROP EXTENSION plperlu;
