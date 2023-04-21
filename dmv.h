#ifndef _DMV_
#define _DMV_

#include "fmgr.h"
#include "postgres.h"

Datum create_dmv(PG_FUNCTION_ARGS);
Oid create_dmv_relation(char * relname, char * query);

void template_insert(Datum * args, bool * nulls, char * relname);
void insert_dmv_target(Oid targetOid, Oid dmvOid);
void insert_target(Oid targetOid, char * relname);
void insert_dmv(Oid dmvOid, char * relname, char * query);
void insert_dmv_lsn(Oid dmvOid);
void handle_query(char * mv_relname, char * query);
bool is_target(Oid relOid);
void single_dmv_loop(Oid dmvOid, Datum dmvLSN);
void wal_read(Oid dmvOid, Datum lsn);

#endif
