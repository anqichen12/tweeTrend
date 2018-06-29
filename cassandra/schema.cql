CREATE KEYSPACE "twitter" WITH REPLICATION =
  { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE twitter;

create table tweet (
  name text,
  date date,
  count int,
  PRIMARY KEY ((date,name),count)
);

