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
	xlogreader.h callback funcs for reader
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
			top 4 bits for rmgr info
			
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
					HeapTupleData readableTup;
					/* 
						storage for OffsetNumber and BlockIdData
						offset - place of linp in page ItemIdData
					*/
					ItemPointerData readablePointer;
					Relation readableRelation;

					// rlocator.relNumber - Oid
					if (!XLogRecGetBlockTagExtended(xlogreader, i, &rlocator, &fork, &block, NULL)) continue;
					if (fork != MAIN_FORKNUM) continue;
					if (is_target(rlocator.relNode)) {
						
						xl_heap_insert *insert_info = (xl_heap_insert *) XLogRecGetData(xlogreader);
						// setting of pointer to exact block & offset
						ItemPointerSet(&readablePointer, block, insert_info->offnum); 
						// pointer copied to a structure with tuple info
						ItemPointerCopy(&readablePointer, &(readableTup.t_self));

						// getting relation which changes we are reading
						// AccessShareLock for read 
						readableRelation = table_open(rlocator.relNode, AccessShareLock);
		
						// tid scan goes here by t_self in readableTup
						if (heap_fetch(readableRelation, SnapshotAny, &readableTup, &readableBuf, false))
						{
							// CatalogTupleInsert(tempRel, &readableTup);
							ReleaseBuffer(readableBuf);
						}

						table_close(readableRelation, AccessShareLock);
					}

				}
			}
		}		
	}
}


XLogRecPtr blockInfo(TimeLineID tli, PrivateData *private_data, XLogRecPtr firstRec)
{
	XLogRecord 		*record;
	XLogReaderState	*xlogreader;
	XLogRecPtr		curRec;
	// last read rec
	XLogRecPtr		lastRec;
	char			*errmsg;
	
	XLogReaderRoutine callbacks = { &XLogReadPageBlock, &WALSegmentOpen, &WALSegmentClose };

	// macro to get RecPtr
	// XLogSegNoOffsetToRecPtr(segno, 0, DEFAULT_XLOG_SEG_SIZE, first_rec);

	xlogreader = XLogReaderAllocate(DEFAULT_XLOG_SEG_SIZE, NULL, &callbacks, private_data);
	elog(NOTICE, "blokcInfo firstRec: %ld", firstRec);
	/*
		firstRec == create dmv function call lsn
		finds first lsn >= firstRec
	*/
	curRec = XLogFindNextRecord(xlogreader, firstRec);
	elog(NOTICE, "blockInfo curRec: %ld", curRec);

	if (XLogRecPtrIsInvalid(curRec))
	{
		elog(NOTICE, "no log entries");
		exit(0);
	}

	// just sets xlogreader fields including NextRecPtr
	XLogBeginRead(xlogreader, curRec);

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

	lastRec = xlogreader->ReadRecPtr;
	XLogReaderFree(xlogreader);

	return lastRec;
}

/*
	dmvLSN - point to start reading WAL from
*/
void single_dmv_loop(Oid dmvOid, Datum dmvLSN)
{
	XLogRecPtr lastRec = DatumGetLSN(dmvLSN);
	StringInfo filePath = makeStringInfo();
	char *fileName = text_to_cstring(DatumGetTextP(DirectFunctionCall1(pg_walfile_name, dmvLSN)));

	while (1)
	{
		XLogRecPtr curLSN = DatumGetLSN(DirectFunctionCall1(pg_current_wal_lsn, NULL));
		if (curLSN >= lastRec)
		{
			initStringInfo(filePath);
			appendStringInfo(filePath, "%s%s", PG_WAL_DIR, fileName);
			
			PrivateData pr_data;
			pr_data.file_path = filePath->data;
			pr_data.tli = 0;

			elog(NOTICE, "loop filename: %s", pr_data.file_path);

			resetStringInfo(filePath);		
			// sets tli and segno
			XLogFromFileName(fileName, &pr_data.tli, &segno, DEFAULT_XLOG_SEG_SIZE);
			lastRec = blockInfo(pr_data.tli, &pr_data, lastRec);		

			fileName = text_to_cstring(DatumGetTextP(DirectFunctionCall1(pg_walfile_name, lastRec)));

		} 
		else
		{
			/* 
				if needed
				update_dmv_lsn(dmvOid, lastRec);				
			*/
		}
		break;
	}

}

void wal_read(Oid dmvOid, Datum LSN)
{
	set_pg_wal_dir();
	single_dmv_loop(dmvOid, LSN);
}
