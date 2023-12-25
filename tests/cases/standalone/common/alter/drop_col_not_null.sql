CREATE TABLE test(i TIMESTAMP TIME INDEX, j INTEGER NOT NULL);

INSERT INTO test VALUES (1, 1), (2, 2);

SELECT * FROM test;

ALTER TABLE test DROP COLUMN j;

INSERT INTO test VALUES (3);

SELECT * FROM test;

DROP TABLE test;