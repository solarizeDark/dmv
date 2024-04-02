#ifndef _DMV_
#define _DMV_

#include "fmgr.h"
#include "postgres.h"

typedef struct
{
	Oid dmvOid;
	Datum lsn;
} BGWorkerArgs;

extern pid_t wal_reader_pid;

typedef struct tupleP
{
    int32 c1;
} tupleP_t;

typedef tupleP_t* tupleP_p;

Datum create_dmv(PG_FUNCTION_ARGS);
Oid create_dmv_relation(char * relname, char * query);
Datum dmv_test(PG_FUNCTION_ARGS);

void template_insert(Datum * args, bool * nulls, char * relname);
void insert_dmv_target(Oid targetOid, Oid dmvOid);
void insert_target(Oid targetOid, char * relname);
void insert_dmv(Oid dmvOid, char * relname, char * query);
void insert_dmv_lsn(Oid dmvOid);
void handle_query(char * mv_relname, char * query);
bool is_target(Oid relOid);
// void wal_read(Datum main_arg);
Oid get_target_relation();

#endif
