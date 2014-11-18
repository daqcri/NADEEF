create table if not exists project (
  name varchar(255) PRIMARY KEY);

create table if not exists rule_type (
  name varchar(255) primary key,
  enabled boolean not null);

create table if not exists rule (
  id serial primary key,
  name varchar(255) not null,
  type varchar(255),
  code text not null,
  java_code text,
  table1 varchar(255) not null,
  table2 varchar(255),
  project_name varchar(255),
  foreign key (project_name) references project (name),
  foreign key (type) REFERENCES rule_type(name)
);


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
