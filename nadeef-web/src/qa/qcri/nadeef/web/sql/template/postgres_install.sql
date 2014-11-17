create table if not exists project (
  id serial primary key,
  name varchar(255) UNIQUE NOT NULL);

create table if not exists rule (
  name varchar(255) primary key not null,
  type int,
  code text not null,
  java_code text,
  table1 varchar(255) not null,
  table2 varchar(255),
  last_edit_timestamp timestamp,
  project_id integer,
  foreign key (project_id) references project (id));

create table if not exists rule_type (
    name varchar(255) primary key,
    enabled boolean not null);

insert into rule_type (name, enabled)
  select 'UDF', true
  where not exists (select * from rule_type where name = 'UDF');

insert into rule_type (name, enabled)
  select 'FD', true
  where not exists (select * from rule_type where name = 'FD');

insert into rule_type (name, enabled)
  select 'CFD', true
  where not exists (select * from rule_type where name = 'CFD');

insert into rule_type (name, enabled)
  select 'DC', true
  where not exists (select * from rule_type where name = 'DC');

insert into rule_type (name, enabled)
  select 'ER', true
  where not exists (select * from rule_type where name = 'ER');
