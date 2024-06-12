create or replace function
create_dmv(text, text) returns void as 'MODULE_PATHNAME', 'create_dmv'
language c;

create or replace function
dmv_test() returns void as 'MODULE_PATHNAME', 'dmv_test'
language c;

/* target relations info */
create table if not exists _dmv_target_relations_
(
	rel_oid oid,
	relname varchar
);

/* mat view info */
create table if not exists _dmv_mv_relations_
(
	rel_oid oid,
	relname varchar,
	query text,
	parsed text
);

/* mat view - target relation */
create table if not exists _dmv_mv_target_
(
	mv_oid oid,
	rel_oid oid
);

/* mat view, lsn start from */
create table if not exists _dmv_mv_lsn_
(
	lsn numeric,
	mv_oid oid
);
