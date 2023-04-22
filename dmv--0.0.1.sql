create or replace function
create_dmv(text, text) returns void as 'MODULE_PATHNAME', 'create_dmv'
language c;

create table if not exists _dmv_target_relations_
(
	rel_oid oid,
	relname varchar
);

create table if not exists _dmv_mv_relations_
(
	rel_oid oid,
	relname varchar,
	query text	
);

create table if not exists _dmv_mv_target_
(
	mv_oid oid,
	rel_oid oid
);

create table if not exists _dmv_mv_lsn_
(
	lsn numeric,
	mv_oid oid
);
