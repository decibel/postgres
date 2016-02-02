CREATE EXTENSION bloom;

CREATE TABLE tst (
	i	int4,
	t	text
);

\copy tst from 'data/data'

CREATE INDEX bloomidx ON tst USING bloom (i, t) WITH (col1 = 3);

SET enable_seqscan=on;
SET enable_bitmapscan=off;
SET enable_indexscan=off;

SELECT count(*) FROM tst WHERE i = 16;
SELECT count(*) FROM tst WHERE t = '5';
SELECT count(*) FROM tst WHERE i = 16 AND t = '5';

SET enable_seqscan=off;
SET enable_bitmapscan=on;
SET enable_indexscan=on;

EXPLAIN (COSTS OFF) SELECT count(*) FROM tst WHERE i = 16;
EXPLAIN (COSTS OFF) SELECT count(*) FROM tst WHERE t = '5';
EXPLAIN (COSTS OFF) SELECT count(*) FROM tst WHERE i = 16 AND t = '5';

SELECT count(*) FROM tst WHERE i = 16;
SELECT count(*) FROM tst WHERE t = '5';
SELECT count(*) FROM tst WHERE i = 16 AND t = '5';

VACUUM ANALYZE tst;

SELECT count(*) FROM tst WHERE i = 16;
SELECT count(*) FROM tst WHERE t = '5';
SELECT count(*) FROM tst WHERE i = 16 AND t = '5';

VACUUM FULL tst;

SELECT count(*) FROM tst WHERE i = 16;
SELECT count(*) FROM tst WHERE t = '5';
SELECT count(*) FROM tst WHERE i = 16 AND t = '5';

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;
