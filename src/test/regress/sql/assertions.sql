CREATE TABLE test1 (a int, b text);

CREATE ASSERTION foo CHECK (1 < 2);
CREATE ASSERTION wrong CHECK (1 + 2);
CREATE ASSERTION wrong CHECK (a < 5);
CREATE ASSERTION a2 CHECK ((SELECT count(*) FROM test1) < 5);
CREATE ASSERTION wrong CHECK ((SELECT count(*) FROM wrong) < 5);

DELETE FROM test1;
INSERT INTO test1 VALUES (1, 'one');
INSERT INTO test1 VALUES (2, 'two');
INSERT INTO test1 VALUES (3, 'three');
INSERT INTO test1 VALUES (4, 'four');
INSERT INTO test1 VALUES (5, 'five');

SELECT constraint_schema, constraint_name FROM information_schema.assertions ORDER BY 1, 2;
\dA

ALTER ASSERTION a2 RENAME TO a3;
ALTER ASSERTION foo RENAME TO a3; -- fails
ALTER ASSERTION wrong RENAME TO wrong2; -- fails

SELECT constraint_schema, constraint_name FROM information_schema.assertions ORDER BY 1, 2;

DROP ASSERTION foo;
DROP ASSERTION wrong;

DROP TABLE test1; -- fails
DROP TABLE test1 CASCADE;

SELECT constraint_schema, constraint_name FROM information_schema.assertions ORDER BY 1, 2;
