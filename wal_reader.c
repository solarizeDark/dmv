#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

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
#include "utils/pg_lsn.h"

#include "dmv.h"

typedef struct PrivateData
{
	char* 		file_path;
	TimeLineID	tli;
} PrivateData;

uint64 segno = 0;
char *PG_WAL_DIR;

void set_pg_wal_dir()
{ 
	StringInfo strInfo = makeStringInfo();
	appendStringInfoString(strInfo, getenv("PGDATA"));
	appendStringInfoString(strInfo, "/pg_wal/");
	PG_WAL_DIR = strInfo->data;
}

/*
	xlogreader.h call back funcs for reader
	seg	WALOpenSegment field of reader
*/

void WALSegmentOpen (XLogReaderState *xlogreader, XLogSegNo nextSegNo, TimeLineID *tli)
{
	PrivateData *private_data = (PrivateData*) xlogreader->private_data;
	xlogreader->seg.ws_file = open(private_data->file_path, O_RDONLY | PG_BINARY, 0);
	if (xlogreader->seg.ws_file < 0)
	{
		fprintf(stderr, "file open error\n");
		exit(1);
	}
}

void WALSegmentClose (XLogReaderState *xlogreader)
{
	close(xlogreader->seg.ws_file);
	xlogreader->seg.ws_file = -1;
}

/*
	will be called by xlogreadrecord through bunch of funcs
	readBuf is used to set state.readBuf
*/
int XLogReadPageBlock(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
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

void getInfo (XLogReaderState *xlogreader)
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
					// tablespace, db, relation oids, synonim for RelFileLocator
					RelFileNode rlocator;
					ForkNumber fork;
					// page number			
					BlockNumber block;
					Buffer readableBuf;

					// rlocator.relNumber - Oid
					if (!XLogRecGetBlockTagExtended(xlogreader, i, &rlocator, &fork, &block, NULL)) continue;
					if (fork != MAIN_FORKNUM) continue;

					if (is_target(rlocator.relNode)) {

						HeapTupleData readableTup;
						// storage for OffsetNumber and BlockIdData
						ItemPointerData readablePointer;

						// lsn is XLogRecPtr uint64,  uint32

						xl_heap_insert *insert_info = (xl_heap_insert *) XLogRecGetData(xlogreader);
						ItemPointerSet(&readablePointer, block, insert_info->offnum); 
						ItemPointerCopy(&readablePointer, &(readableTup.t_self));

						// getting relation which changes we are reading
						// AccessShareLock for read 
						Relation readableRelation = table_open(rlocator.relNode, AccessShareLock);
		
						if (heap_fetch(readableRelation, SnapshotAny, &readableTup, &readableBuf, false))
						{
							// CatalogTupleInsert(tempRel, &readableTup);
							ReleaseBuffer(readableBuf);
						}

						table_close(readableRelation, AccessShareLock);
					}

				}
				break;
			}
		}		
	}
}


XLogRecPtr blockInfo(TimeLineID tli, PrivateData *private_data)
{
	XLogRecord 		*record;
	XLogReaderState	*xlogreader;
	XLogRecPtr		first_rec;
	XLogRecPtr		cur_rec;
	char			*errmsg;
	XLogRecPtr 		last_rec;
	
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
			elog(ERROR, "%s", errmsg);

		getInfo(xlogreader);
	} while (record != NULL);

	last_rec = xlogreader->ReadRecPtr;
	XLogReaderFree(xlogreader);

	return last_rec;
}

void single_dmv_loop(Oid dmvOid, Datum dmvLSN)
{
	elog(NOTICE, "Oid: %d", dmvOid);
	elog(NOTICE, "LSN: %ld", DatumGetLSN(dmvLSN));

	XLogRecPtr lastRec;
	char *fileName = DatumGetCString(DirectFunctionCall1(pg_walfile_name, dmvLSN));

	elog(NOTICE, "lastRec: %ld", lastRec);
	elog(NOTICE, "fileName preloop: %s", fileName);
	
	while (1)
	{
		elog(NOTICE, "loop LSN: %ld", DatumGetLSN(DirectFunctionCall1(pg_switch_wal, NULL)));
		if (DatumGetLSN(DirectFunctionCall1(pg_switch_wal, NULL)) >= lastRec)
		{
			
			PrivateData pr_data;
			pr_data.file_path = strcat(PG_WAL_DIR, fileName);
			pr_data.tli = 0;

			// sets tli and segno
			XLogFromFileName(fileName, &pr_data.tli, &segno, DEFAULT_XLOG_SEG_SIZE);
			lastRec = blockInfo(pr_data.tli, &pr_data);		

			fileName = text_to_cstring(DatumGetTextP(DirectFunctionCall1(pg_walfile_name, lastRec)));

			elog(NOTICE, "lastRec: %ld", lastRec);
			elog(NOTICE, "fileName preloop: %s", fileName);
		} 
		else
		{
			/* 
				if needed
				update_dmv_lsn(dmvOid, lastRec);				
			*/
		}
	}

}

void wal_read(Oid dmvOid, Datum LSN)
{
	set_pg_wal_dir();
	single_dmv_loop(dmvOid, LSN);
}
