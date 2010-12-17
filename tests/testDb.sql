CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.DbStorage_Test_1 (
  id bigint,
  ra double,
  decl double,
  something int,
  final varchar(80)
);
CREATE TABLE IF NOT EXISTS test.DbStorage_Test_2 (
  id bigint(20),
  decl double
);
CREATE TABLE IF NOT EXISTS test.DbTsvStorage_Test_1 LIKE DbStorage_Test_1;
CREATE TABLE IF NOT EXISTS test.Persistence_Test_2 (
  int64Field bigint,
  varcharField varchar(80),
  boolField tinyint,
  intField int(11),
  floatField float,
  doubleField double
);
