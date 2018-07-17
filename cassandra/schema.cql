CREATE KEYSPACE twitter WITH REPLICATION =
  { 'class' : 'SimpleStrategy', 'replication_factor' : 4 };
  
USE twitter;

create table realtime (
  time timestamp,
  name text,
  count int,
  PRIMARY KEY (name,count)
);


create table result (
  name text,
  count int,
  time timestamp,
  followers int,
  PRIMARY KEY (name,count)
);


create table live (
  name text,
  count int,
  time timestamp,
  followers int,
  PRIMARY KEY (name,time)
);

create table twittertrend (
  name text,
  count int,
  time timestamp,
  PRIMARY KEY (name,time)
);

create table spotifytrend (
  name text,
  followers int,
  time timestamp,
  PRIMARY KEY (name,time)
);

