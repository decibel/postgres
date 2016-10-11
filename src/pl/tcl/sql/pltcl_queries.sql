-- suppress CONTEXT so that function OIDs aren't in output
\set VERBOSITY terse

insert into T_pkey1 values (1, 'key1-1', 'test key');
insert into T_pkey1 values (1, 'key1-2', 'test key');
insert into T_pkey1 values (1, 'key1-3', 'test key');
insert into T_pkey1 values (2, 'key2-1', 'test key');
insert into T_pkey1 values (2, 'key2-2', 'test key');
insert into T_pkey1 values (2, 'key2-3', 'test key');

insert into T_pkey2 values (1, 'key1-1', 'test key');
insert into T_pkey2 values (1, 'key1-2', 'test key');
insert into T_pkey2 values (1, 'key1-3', 'test key');
insert into T_pkey2 values (2, 'key2-1', 'test key');
insert into T_pkey2 values (2, 'key2-2', 'test key');
insert into T_pkey2 values (2, 'key2-3', 'test key');

select * from T_pkey1;

-- key2 in T_pkey2 should have upper case only
select * from T_pkey2;

insert into T_pkey1 values (1, 'KEY1-3', 'should work');

-- Due to the upper case translation in trigger this must fail
insert into T_pkey2 values (1, 'KEY1-3', 'should fail');

insert into T_dta1 values ('trec 1', 1, 'key1-1');
insert into T_dta1 values ('trec 2', 1, 'key1-2');
insert into T_dta1 values ('trec 3', 1, 'key1-3');

-- Must fail due to unknown key in T_pkey1
insert into T_dta1 values ('trec 4', 1, 'key1-4');

insert into T_dta2 values ('trec 1', 1, 'KEY1-1');
insert into T_dta2 values ('trec 2', 1, 'KEY1-2');
insert into T_dta2 values ('trec 3', 1, 'KEY1-3');

-- Must fail due to unknown key in T_pkey2
insert into T_dta2 values ('trec 4', 1, 'KEY1-4');

select * from T_dta1;

select * from T_dta2;

update T_pkey1 set key2 = 'key2-9' where key1 = 2 and key2 = 'key2-1';
update T_pkey1 set key2 = 'key1-9' where key1 = 1 and key2 = 'key1-1';
delete from T_pkey1 where key1 = 2 and key2 = 'key2-2';
delete from T_pkey1 where key1 = 1 and key2 = 'key1-2';

update T_pkey2 set key2 = 'KEY2-9' where key1 = 2 and key2 = 'KEY2-1';
update T_pkey2 set key2 = 'KEY1-9' where key1 = 1 and key2 = 'KEY1-1';
delete from T_pkey2 where key1 = 2 and key2 = 'KEY2-2';
delete from T_pkey2 where key1 = 1 and key2 = 'KEY1-2';

select * from T_pkey1;
select * from T_pkey2;
select * from T_dta1;
select * from T_dta2;

select tcl_avg(key1) from T_pkey1;
select tcl_sum(key1) from T_pkey1;
select tcl_avg(key1) from T_pkey2;
select tcl_sum(key1) from T_pkey2;

-- The following should return NULL instead of 0
select tcl_avg(key1) from T_pkey1 where key1 = 99;
select tcl_sum(key1) from T_pkey1 where key1 = 99;

select 1 @< 2;
select 100 @< 4;

select * from T_pkey1 order by key1 using @<, key2 collate "C";
select * from T_pkey2 order by key1 using @<, key2 collate "C";

-- show dump of trigger data
insert into trigger_test values(1,'insert');

insert into trigger_test_view values(2,'insert');
update trigger_test_view set v = 'update' where i=1;
delete from trigger_test_view;

update trigger_test set v = 'update' where i = 1;
delete from trigger_test;

-- Test composite-type arguments
select tcl_composite_arg_ref1(row('tkey', 42, 'ref2'));
select tcl_composite_arg_ref2(row('tkey', 42, 'ref2'));

-- Test argisnull primitive
select tcl_argisnull('foo');
select tcl_argisnull('');
select tcl_argisnull(null);

-- Test spi_lastoid primitive
create temp table t1 (f1 int);
select tcl_lastoid('t1');
create temp table t2 (f1 int) with oids;
select tcl_lastoid('t2') > 0;

-- test compound return
select * from tcl_test_cube_squared(5);

CREATE FUNCTION bad_record(OUT a text , OUT b text) AS $$return [list cow]$$ LANGUAGE pltcl;
SELECT bad_record();

CREATE OR REPLACE FUNCTION tcl_error(OUT a int, OUT b int) AS $$return {$$ LANGUAGE pltcl;
SELECT crash();

-- test multi-row returns
select * from tcl_test_cube_squared_rows(1,10);

-- test setof returns
select * from tcl_test_sequence(1,10) as a;

-- Test quote
select * from tcl_eval('quote foo bar');
select * from tcl_eval('quote [format %c 39]');
select * from tcl_eval('quote [format %c 92]');

-- Test argisnull
select * from tcl_eval('argisnull');
select * from tcl_eval('argisnull 14');
select * from tcl_eval('argisnull abc');

-- Test return_null
select * from tcl_eval('return_null 14');

-- Test spi_exec
select * from tcl_eval('spi_exec');
select * from tcl_eval('spi_exec -count');
select * from tcl_eval('spi_exec -array');
select * from tcl_eval('spi_exec -count abc');
select * from tcl_eval('spi_exec query loop body toomuch');
select * from tcl_eval('spi_exec "begin; rollback;"');

-- Test spi_execp
select * from tcl_eval('spi_execp');
select * from tcl_eval('spi_execp -count');
select * from tcl_eval('spi_execp -array');
select * from tcl_eval('spi_execp -count abc');
select * from tcl_eval('spi_execp -nulls');
select * from tcl_eval('set prep [spi_prepare "begin; rollback" ""]; spi_execp $prep');

-- test spi_prepare
select * from tcl_eval('spi_prepare');
select * from tcl_eval('spi_prepare a b');
select * from tcl_eval('spi_prepare a "b {"');

-- test elog
select * from tcl_eval('elog');
select * from tcl_eval('elog foo bar');

-- test forced error
select tcl_eval('error "forced error"');

select * from tcl_eval('unset -nocomplain ::tcl_vwait; after 100 {set ::tcl_vwait 1}; vwait ::tcl_vwait; unset -nocomplain ::tcl_vwait');
