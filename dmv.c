#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

#include "postgres.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "parser/parser.h"
#include "parser/parse_node.h"
#include "nodes/pg_list.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/pg_lsn.h"
#include "postmaster/bgworker.h"

#include "access/amapi.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "dmv.h"

#define TARGET 					":relname"
#define DMV_TARGET_RELATIONS 	"_dmv_target_relations_"		/* oid, name of base relations 		*/
#define DMV_MV_RELATIONS 		"_dmv_mv_relations_"			/* oid, name, forming query of dmv 	*/
#define DMV_MV_TARGET	 		"_dmv_mv_target_"				/* many to many dmv - base rel		*/
#define DMV_MV_LSN				"_dmv_mv_lsn_"					/* WAL reading start point 			*/
#define INT8OID					20

PG_FUNCTION_INFO_V1(dmv_test);
Datum dmv_test(PG_FUNCTION_ARGS) {
	Relation rel;
    HeapTuple tup;
    TableScanDesc scan;
    Oid tbl_oid = 164403;
	elog(NOTICE, "[wal_read]\tTEST");

    rel = table_open(tbl_oid, AccessShareLock);
	elog(NOTICE, "[wal_read]\tTESTEND");

    scan = table_beginscan(rel, GetTransactionSnapshot(), 0, NULL);

    while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        tupleP_p record = (tupleP_p) GETSTRUCT(tup);
		elog(NOTICE, "[T]\tc_col: %d", record->c1);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);
}

PG_FUNCTION_INFO_V1(create_dmv);
Datum lsn;

void template_insert(Datum * args, bool * nulls, char * relname)
{
	Relation rel;
	Oid oid;
	HeapTuple tup;

	oid = DatumGetObjectId(DirectFunctionCall1(to_regclass, CStringGetTextDatum(relname)));

	rel = table_open(oid, RowExclusiveLock);
	tup = heap_form_tuple(RelationGetDescr(rel), args, nulls);
	/* 
		CatalogTupleInsert
		https://doxygen.postgresql.org/indexing_8c_source.html#l00233
	*/
	CatalogTupleInsert(rel, tup);

	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);
}

/* inserting to metadata table oid and name of tables on which view is based */
void insert_target(Oid targetOid, char * relname)
{
	Datum values[2];
	bool nulls[2];

	memset(nulls, false, sizeof(nulls));

	elog(NOTICE, "[insert_target]\t targetOid: %d", targetOid);

	values[0] = ObjectIdGetDatum(targetOid);
	values[1] = CStringGetTextDatum(relname);

	template_insert(values, nulls, DMV_TARGET_RELATIONS);
}

/* inserting to metadata table oid, name and query of view creation */
void insert_dmv(Oid dmvOid, char * relname, char * query)
{
	// elog(NOTICE, "%s\n", "insert_dmv");
	Oid oid;
	oid = DirectFunctionCall1(to_regclass, CStringGetTextDatum(relname));

	Datum values[3];
	bool nulls[3];

	memset(nulls, false, sizeof(nulls));

	values[0] = ObjectIdGetDatum(dmvOid);
	values[1] = CStringGetTextDatum(relname);
	values[2] = CStringGetTextDatum(query);

	template_insert(values, nulls, DMV_MV_RELATIONS);
}

/*
	base relations to view relations
*/
void insert_dmv_target(Oid targetOid, Oid dmvOid)
{
	Datum values[2];
	bool nulls[2];

	memset(nulls, false, sizeof(nulls));

	values[0] = ObjectIdGetDatum(dmvOid);
	values[1] = ObjectIdGetDatum(targetOid);

	template_insert(values, nulls, DMV_MV_TARGET);
}

/* metadata with lsn and view relation */
void insert_dmv_lsn(Oid dmvOid)
{
	Datum values[2];
	bool nulls[2];

	memset(nulls, false, sizeof(nulls));

	/* 
		pg_current_wal_lsn
		https://doxygen.postgresql.org/xlogfuncs_8c_source.html#l00279 
	*/
	lsn = (Datum) DirectFunctionCall1(pg_current_wal_lsn, NULL);
	// elog(NOTICE, "[insert_dmv_lsn]\t%ld", DatumGetUInt64(lsn));
	double lsnDouble = (double) DatumGetUInt64(lsn);

	values[0] = DirectFunctionCall1(float8_numeric, Float8GetDatum(lsnDouble));
	values[1] = ObjectIdGetDatum(dmvOid);

	template_insert(values, nulls, DMV_MV_LSN);
}

void handle_query(char * mv_relname, char * query)
{
	Oid targetRelationOid;
	Oid dmvRelationOid;
	ListCell *cell;
	List *nodes = raw_parser(query, RAW_PARSE_DEFAULT);
	RawStmt *stmt = linitial_node(RawStmt, nodes);
	char *namecpy;
	char *token;
	bool flag = false;

	if (!IsA(stmt->stmt, SelectStmt))
	{
		elog(ERROR, "invalid query");
	}

	foreach(cell, nodes)
	{
		namecpy = nodeToString(lfirst(cell));
	}

	dmvRelationOid = create_dmv_relation(mv_relname, query);

	token = strtok(namecpy, " ");
	// elog(NOTICE, "token: %s", token);

	while (token)
	{

		if (strcmp(token, TARGET) == 0)
		{
			flag = true;
		}

		token = strtok(NULL, " ");
		// elog(NOTICE, "token: %s", token);

		if (flag)
		{
			targetRelationOid = DatumGetObjectId(DirectFunctionCall1(to_regclass, CStringGetTextDatum(token)));

			// relation existence check
			if (RelationIsValid(targetRelationOid)) {
				insert_target(targetRelationOid, token);
				insert_dmv_target(targetRelationOid, dmvRelationOid);
			} else {
				elog(ERROR, "Invalid relname: %s", token);
			}

			flag = false;
	 	}

	}

}

/* dmv relation creation, returns oid of created table */
Oid create_dmv_relation(char * relname, char * query)
{
	Oid createdRelOid;

	StringInfo createQuery = makeStringInfo();
	appendStringInfo(createQuery, "create table %s as %s;", relname, query);

	SPI_connect();
	SPI_exec(createQuery->data, 1);
	SPI_finish();

	createdRelOid = DatumGetObjectId(DirectFunctionCall1(to_regclass, CStringGetTextDatum(relname)));

	insert_dmv(createdRelOid, relname, query);
	insert_dmv_lsn(createdRelOid);
	return createdRelOid;
}

bool is_target(Oid oid)
{
	elog(NOTICE, "is_target called");

	StringInfo createQuery = makeStringInfo();
	elog(NOTICE, "makeStringInfo");
	
	appendStringInfoString(createQuery, "select rel_oid from _dmv_mv_target_ where rel_oid = $1");
	elog(NOTICE, "appendStringInfoString");


	// bigint OID
	Oid argType[] = { INT8OID };
	Datum argVals[] = { ObjectIdGetDatum(oid) };
	
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "Cannot connect to SPI manager");
	elog(NOTICE, "SPI_connect");
		
	bool res;
	int ret = SPI_execute_with_args(createQuery->data, 1, argType, argVals, NULL, true, 1);
	elog(NOTICE, "SPI_execute_with_args");

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "Failed to execute query");
	res = (SPI_processed != 0);
	elog(NOTICE, "SPI_processed: %d", SPI_processed);
	SPI_finish();
	elog(NOTICE, "SPI_finish");

	PopActiveSnapshot();

	return res;
}

/* args: dmv name, dmv query */
Datum create_dmv(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_PP(0);
	text *query = PG_GETARG_TEXT_PP(1);

	char *relnameCString = text_to_cstring(relname);
	char *queryCString = text_to_cstring(query);
	handle_query(relnameCString, queryCString);
}

/* temp */
// Oid get_target_relation()
// {

// }