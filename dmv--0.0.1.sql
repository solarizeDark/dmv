create or replace function
wal_read() returns void as 'MODULE_PATHNAME', 'foo'
language c;

create or replace function
create_dmv(text, text) returns void as 'MODULE_PATHNAME', 'create_dmv'
language c;

create table if not exists _dmv_target_relations_
(
	oid bigint,
	relname varchar
);

create table if not exists _dmv_mv_relations_
(
	oid bigint,
	relname varchar,
	query text	
);

create table if not exists _dmv_mv_target_
(
	mv_oid bigint,
	rel_oid bigint
);

create table if not exists _dmv_mv_lsn_
(
	lsn numeric,
	mv_oid bigint
);
