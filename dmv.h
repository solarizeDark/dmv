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
// BGWorkerArgs *bgworkerArgs;

Datum create_dmv(PG_FUNCTION_ARGS);
Oid create_dmv_relation(char * relname, char * query);

void template_insert(Datum * args, bool * nulls, char * relname);
void insert_dmv_target(Oid targetOid, Oid dmvOid);
void insert_target(Oid targetOid, char * relname);
void insert_dmv(Oid dmvOid, char * relname, char * query);
void insert_dmv_lsn(Oid dmvOid);
void handle_query(char * mv_relname, char * query);
bool is_target(Oid relOid);
void* single_dmv_loop(void* arg);
void wal_read();//Oid dmv_oid, Datum lsn);

#endif
