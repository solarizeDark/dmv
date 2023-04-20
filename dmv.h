#ifndef _DVM_
#define _DVM_

#include "fmgr.h"

Datum foo(PG_FUNCTION_ARGS);
Datum create_dmv(PG_FUNCTION_ARGS);
void template_insert(Datum * args, bool * nulls, char * relname);
void insert_dmv_target(Oid targetOid, Oid dmvOid);
void insert_target(Oid targetOid, char * relname);
void insert_dmv(Oid dmvOid, char * relname, char * query);
void insert_dmv_lsn(Oid dmvOid);
void handle_query(char * mv_relname, char * query);
Oid create_dmv_relation(char * relname, char * query);

#endif
