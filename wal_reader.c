#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postgres.h"

#include "access/xlogdefs.h"
#include "access/xlog_internal.h"
#include "access/heapam_xlog.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "fmgr.h"
#include "funcapi.h"
#include "storage/lockdefs.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"

#include "dmv.h"

PG_FUNCTION_INFO_V1(foo);

typedef struct PrivateData
{
	char* 		file_path;
	TimeLineID	tli;
} PrivateData;

uint64 segno = 0;
#define T_OID 16388	// t relation OID hardcoded	

Oid tempOid;
Relation tempRel;

/*
	xlogreader.h call back funcs for reader
	seg	WALOpenSegment field of reader
*/
void
WALSegmentOpen (XLogReaderState *xlogreader, XLogSegNo nextSegNo, TimeLineID *tli)
{
	PrivateData *private_data = (PrivateData*) xlogreader->private_data;
	xlogreader->seg.ws_file = open(private_data->file_path, O_RDONLY | PG_BINARY, 0);
	if (xlogreader->seg.ws_file < 0)
	{
		fprintf(stderr, "file open error\n");
		exit(1);
	}
}

void
WALSegmentClose (XLogReaderState *xlogreader)
{
	close(xlogreader->seg.ws_file);
	xlogreader->seg.ws_file = -1;
}

/*
	will be called by xlogreadrecord through bunch of funcs
	readBuf is used to set state.readBuf
*/
int
XLogReadPageBlock (XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
	PrivateData *private_data = (PrivateData *) xlogreader->private_data;
	WALReadError errinfo;
	bool wal_read_res = WALRead(xlogreader, readBuf, targetPagePtr, XLOG_BLCKSZ, private_data->tli, &errinfo); 
	if (!WALRead(xlogreader, readBuf, targetPagePtr, XLOG_BLCKSZ, private_data->tli, &errinfo))
	{
		elog(ERROR, "read page block error");
		exit(1);
	}
	/*printf("%s\n", XLogRecGetData(xlogreader));
	for (int i = 0; i <= XLogRecMaxBlockId(xlogreader); i++)
	{
		printf("%d\n", i);
	}
	*/
	return XLOG_BLCKSZ;
}

void
getInfo (XLogReaderState *xlogreader)
{
	/* 
		resource manager id from XLogRecord header of last read record by XLogReadRecord
	*/
	uint8 xlrmid = XLogRecGetRmid(xlogreader);
	if (xlrmid == RM_HEAP_ID)
	{
		/*
			XLR_RMGR_INFO_MASK 0xf0
		*/
		uint8 xlinfo = XLogRecGetInfo(xlogreader) & ~XLR_RMGR_INFO_MASK;
		xlinfo &= XLOG_HEAP_OPMASK;
		switch(xlinfo)
		{
			case XLOG_HEAP_INSERT:
			{				
				for (int i = 0; i <= XLogRecMaxBlockId(xlogreader); i++)
				{
					RelFileNode rlocator;	// tablespace, db, relation oids, synonim for RelFileLocator
					ForkNumber fork;			
					BlockNumber block;		// page number
					Buffer readableBuf;

					if (!XLogRecGetBlockTagExtended(xlogreader, i, &rlocator, &fork, &block, NULL)) continue;	// rlocator.relNumber - Oid
					if (fork != MAIN_FORKNUM) continue;

					if (rlocator.relNode == T_OID) {

						HeapTupleData readableTup;
						ItemPointerData readablePointer;	// storage for OffsetNumber and BlockIdData

						// lsn is XLogRecPtr uint64,  uint32

						xl_heap_insert *insert_info = (xl_heap_insert *) XLogRecGetData(xlogreader);
						ItemPointerSet(&readablePointer, block, insert_info->offnum); 
						ItemPointerCopy(&readablePointer, &(readableTup.t_self));

						// getting relation which changes we are reading
						// AccessShareLock for read 
						Relation readableRelation = table_open(T_OID, AccessShareLock);
		
						if (heap_fetch(readableRelation, SnapshotAny, &readableTup, &readableBuf, false))
						{
							CatalogTupleInsert(tempRel, &readableTup);
							ReleaseBuffer(readableBuf);
						}

						table_close(readableRelation, AccessShareLock);

						/*Datum rLSN = UInt64GetDatum(xlogreader->record->lsn);
						Datum dLen = UInt32GetDatum(XLogRecGetDataLen(xlogreader));

						Datum row[2] = { rLSN, dLen };

						bool nulls[2];
						memset(nulls, false, sizeof(nulls));						 

						HeapTuple tuple = heap_form_tuple(RelationGetDescr(tempRel), row, nulls);

						CatalogTupleInsert(tempRel, tuple);
						heap_freetuple(tuple);
						*/
					}

				}
				break;
			}
		}		
	}
}

void
blockInfo (TimeLineID tli, PrivateData *private_data)
{
	XLogRecord 		*record;
	XLogReaderState	*xlogreader;
	XLogRecPtr		first_rec;
	XLogRecPtr		cur_rec;
	char			*errmsg;
	
	XLogReaderRoutine callbacks = { &XLogReadPageBlock, &WALSegmentOpen, &WALSegmentClose };

	// macro to get RecPtr
	XLogSegNoOffsetToRecPtr(segno, 0, DEFAULT_XLOG_SEG_SIZE, first_rec);

	xlogreader = XLogReaderAllocate(DEFAULT_XLOG_SEG_SIZE, NULL, &callbacks, private_data);

	// finds first lsn >= first_rec
	cur_rec = XLogFindNextRecord(xlogreader, first_rec);

	if (XLogRecPtrIsInvalid(cur_rec))
	{
		elog(NOTICE, "no log entries");
		exit(0);
	}

	// just sets xlogreader fields including NextRecPtr
	XLogBeginRead(xlogreader, cur_rec);

	do
	{
		// callback inside
		record = XLogReadRecord(xlogreader, &errmsg);
		if (record == NULL)
			break;

		if (errmsg)
			elog(NOTICE, "%s", errmsg);

		getInfo(xlogreader);
	} while (record != NULL);

	XLogReaderFree(xlogreader);
}

Datum wal_read(PG_FUNCTION_ARGS)
{

	// temp table routine
	tempOid = DatumGetObjectId(DirectFunctionCall1(to_regclass, CStringGetTextDatum("temp")));
	tempRel = table_open(tempOid, RowExclusiveLock);
	
	//elog(NOTICE, "sdffsd");

	PrivateData pr_data;
	char *file_name = "000000010000000000000001";
	pr_data.file_path = strcat(strcat(getenv("PGDATA"), "/pg_wal/"), file_name);
	pr_data.tli = 0;

	// sets tli and segno
	XLogFromFileName(file_name, &pr_data.tli, &segno, DEFAULT_XLOG_SEG_SIZE);
	blockInfo(pr_data.tli, &pr_data);

	table_close(tempRel, RowExclusiveLock);
}
