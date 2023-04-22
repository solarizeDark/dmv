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

#include "dmv.h"

#define TARGET 					":relname"
#define DMV_TARGET_RELATIONS 	"_dmv_target_relations_"
#define DMV_MV_RELATIONS 		"_dmv_mv_relations_"
#define DMV_MV_TARGET	 		"_dmv_mv_target_"
#define DMV_MV_LSN				"_dmv_mv_lsn_"
#define INT8OID					20

bool WALread = false;
PG_MODULE_MAGIC;
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
	CatalogTupleInsert(rel, tup);

	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);
}

/*
	inserting to metadata table oid and name of tables on which view is based
*/
void insert_target(Oid targetOid, char * relname)
{
	Datum values[2];
	bool nulls[2];

	memset(nulls, false, sizeof(nulls));

	values[0] = ObjectIdGetDatum(targetOid);
	values[1] = CStringGetTextDatum(relname);

	template_insert(values, nulls, DMV_TARGET_RELATIONS);
}

/*
	inserting to metadata table oid, name and query of view creation
*/
void insert_dmv(Oid dmvOid, char * relname, char * query)
{

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

/*
	metadata with lsn and view relation
*/
void insert_dmv_lsn(Oid dmvOid)
{
	Datum values[2];
	bool nulls[2];

	memset(nulls, false, sizeof(nulls));

	lsn = (DirectFunctionCall1(pg_current_wal_lsn, NULL));
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

	while (token)
	{

		if (strcmp(token, TARGET) == 0)
		{
			flag = true;
		}

		token = strtok(NULL, " ");

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

	if (!WALread)
	{
		wal_read(dmvRelationOid, lsn);
		WALread = true;
	}
}

/*
	mat view relation creation
	returns oid of created table
*/
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
	StringInfo createQuery = makeStringInfo();
	appendStringInfoString(createQuery, "select rel_oid from _dmv_mv_target_ where rel_oid = $1");

	// bigint OID
	Oid argType[] = { INT8OID };
	Datum argVals[] = { DatumGetObjectId(oid) };

	SPI_connect();
	SPI_execute_with_args(createQuery->data, 1, argType, argVals, NULL, true, 1);
	bool res = (SPI_processed == 1);

	SPI_finish();

	return res;
}

Datum create_dmv(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_PP(0);
	text *query = PG_GETARG_TEXT_PP(1);

	char *relnameCString = text_to_cstring(relname);
	char *queryCString = text_to_cstring(query);

	handle_query(relnameCString, queryCString);
}
