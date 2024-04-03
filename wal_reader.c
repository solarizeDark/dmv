#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/types.h>
#include <unistd.h>

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
#include "utils/elog.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "pgstat.h"
/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "dmv.h"

typedef struct PrivateData
{
	char* 		file_path;
	TimeLineID	tli;
} PrivateData;

PG_MODULE_MAGIC;

static XLogSegNo segno = 0;
static char PG_WAL_DIR[50];
static int wal_reader_sleep_time = 5; 

void set_pg_wal_dir()
{ 
	StringInfo strInfo = makeStringInfo();
	appendStringInfoString(strInfo, getenv("PGDATA"));
	appendStringInfoString(strInfo, "/pg_wal/");
	strcpy(PG_WAL_DIR, strInfo->data);
	pfree(strInfo);
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
		elog(ERROR, "[WALSegmentOpen]\tfile open error: %s", private_data->file_path);
	} else {
		elog(NOTICE, "[WALSegmentOpen]\treading: %s", private_data->file_path);		
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
		elog(ERROR, "%s", "read page block error");
	}

	return XLOG_BLCKSZ;
}

/*
	get resource manager for last decoded record
	if rm is heap, then switch for heap operation
*/
void getInfo (XLogReaderState *xlogreader)
{
	StartTransactionCommand();

	if (xlogreader == NULL) {
		elog(ERROR, "[getInfo]\tNULL xlogreader");
	}

	Datum lsn = (Datum) DirectFunctionCall1(pg_current_wal_lsn, NULL);
	elog(NOTICE, "[getInfo]\tcurLSN: %ld", DatumGetUInt64(lsn));

	Relation dmvRelation = table_open((Oid) 156211, RowExclusiveLock);

	if (dmvRelation == NULL) {
		elog(ERROR, "Failed to open relation with OID 139822");
	}

	/* 
		XLogRecGet* fetchs info of xlogreader->record which is most recently read DecodedXLogRecord
		
		xlrmid: XLOG resource manager id from XLogRecord header of last read record by XLogReadRecord
		RM_HEAP RM_HEAP2 for heap table operations
	*/
	uint8 xlrmid = XLogRecGetRmid(xlogreader);
	/* look at rmgrlist.h for constants */
	if (xlrmid == RM_HEAP_ID)
	{
		/*
			example at heapam.c heap_redo()
			xlinfo: type of operation
			XLR_RMGR_INFO_MASK 0xF0
			top 4 bits for rmgr info
		*/
		uint8 xlinfo = XLogRecGetInfo(xlogreader) & XLR_RMGR_INFO_MASK;
		/* heapam_xlog.h */
		xlinfo &= XLOG_HEAP_OPMASK;
		elog(NOTICE, "[getInfo]\tswitch");
		switch(xlinfo)
		{
			case XLOG_HEAP_INSERT:
			{
				for (int i = 0; i <= XLogRecMaxBlockId(xlogreader); i++)
				{
					RelFileLocator rlocator; 			/* tablespace, db, relation */
					ForkNumber fork;					/* type of physical file for relation, we need main fork, other forks for metadata */
					BlockNumber block;					/* page number */
					Buffer readableBuf;
					HeapTupleData readableTup;
					ItemPointerData readablePointer;	/* storage for OffsetNumber and BlockIdData, offset - place of linp in page ItemIdData */
					Relation readableRelation;

					/* setting rlocator, fork, block */
					if (!XLogRecGetBlockTagExtended(xlogreader, i, &rlocator, &fork, &block, NULL)) continue;
					if (fork != MAIN_FORKNUM) continue;			/* other forks for metadata */
					if (is_target(rlocator.relNumber) || 1) { 	/* rlocator.relNumber - OID of relation */

						/* record main_data */
						xl_heap_insert *insert_info = (xl_heap_insert *) XLogRecGetData(xlogreader);
						/* setting of pointer to exact block & offset (which is index of linp) */
						ItemPointerSet(&readablePointer, block, insert_info->offnum); 
						/* pointer copied to a structure with tuple info */
						ItemPointerCopy(&readablePointer, &(readableTup.t_self));

						/*
							getting relation which changes we are reading
							AccessShareLock for select (lockdefs.h)
						*/
						readableRelation = table_open(rlocator.relNumber, AccessShareLock);

						/* tid scan goes here by t_self in readableTup */
						if (heap_fetch(readableRelation, SnapshotAny, &readableTup, &readableBuf, false))
						{
							elog(NOTICE, "[getInfo]\tCatalogTupleInsert");
							CatalogTupleInsert(dmvRelation, &readableTup);
							ReleaseBuffer(readableBuf);
						}

						table_close(readableRelation, AccessShareLock);
					}

				}
			}
		}		
	}

	table_close(dmvRelation, RowExclusiveLock);
	/* Commit transaction */
	CommitTransactionCommand();
}

XLogRecPtr blockInfo(TimeLineID tli, PrivateData *private_data, XLogRecPtr firstRec)
{
	elog(NOTICE, "blockInfo started");
	XLogRecord 		*record;
	XLogReaderState	*xlogreader;
	XLogRecPtr		curRec;
	// last read rec
	XLogRecPtr		lastRec;
	char			*errmsg;
	
	XLogReaderRoutine callbacks = { &XLogReadPageBlock, &WALSegmentOpen, &WALSegmentClose };

	// macro to get RecPtr
	// XLogSegNoOffsetToRecPtr(segno, 0, DEFAULT_XLOG_SEG_SIZE, first_rec);

	/* inits reader, sets private_data */
	xlogreader = XLogReaderAllocate(DEFAULT_XLOG_SEG_SIZE, NULL, &callbacks, private_data);

	if (xlogreader == NULL)
	{
		elog(ERROR, "%s", "null reader");
	}

	/* firstRec == create dmv function call lsn, finds first lsn >= firstRec and positions reader to that point */
	curRec = XLogFindNextRecord(xlogreader, firstRec);

	if (XLogRecPtrIsInvalid(curRec))
	{
		elog(NOTICE, "no log entries");
	}

	/* just sets xlogreader fields including NextRecPtr and EndRecPtr, doesnt read WAL yet */
	XLogBeginRead(xlogreader, curRec);

	do
	{
		/* 
			calls XlogReadPageBlock callback inside, 
			returns pointer XLogRecord, which contains xl_info, xl_rmid 
			to decoded block header (DecodedXLogRecord)
		*/
		record = XLogReadRecord(xlogreader, &errmsg); 
		if (record == NULL)
			break;

		if (errmsg)
			elog(ERROR, "[blockInfo]\t%s", errmsg);

		/* fetching info from record that we read */
		getInfo(xlogreader);
	} while (record != NULL);

	lastRec = xlogreader->ReadRecPtr;
	XLogReaderFree(xlogreader);

	return lastRec;
}

void test()
{
	elog(NOTICE, "[TEST]\tTEST1");
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
	
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	Relation rel;
    HeapTuple tup;
    TableScanDesc scan;
    Oid tbl_oid = 164403;
	elog(NOTICE, "[TEST]\tTEST2");

    rel = table_open(tbl_oid, AccessShareLock);

    scan = table_beginscan(rel, GetTransactionSnapshot(), 0, NULL);

    while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        tupleP_p record = (tupleP_p) GETSTRUCT(tup);
		elog(NOTICE, "[T]\tc_col: %d", record->c1);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);
	CommitTransactionCommand();
	elog(NOTICE, "[TEST]\tTESTEND");
}

/* to make function visible */
// void wal_read(Datum main_arg) __attribute__((visibility("default")));
extern PGDLLEXPORT void wal_read(Datum main_arg)
{
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
	BackgroundWorkerUnblockSignals();

	StringInfo filePath = makeStringInfo();

	XLogRecPtr curLSN = DatumGetLSN(DirectFunctionCall1(pg_current_wal_lsn, NULL));
	/* 
		! 
		have to be min(lsn) from _dmv_mv_lsn_ table 
		for restart case 
	*/
	XLogRecPtr lastRec = curLSN;

	elog(NOTICE, "[wal_read]\tpreloop cur rec: %ld", curLSN);
	
	/* getting wal file by lsn */
	char *fileName = text_to_cstring(DatumGetTextP(DirectFunctionCall1(pg_walfile_name, lastRec)));

	for(;;)
	{
		(void) WaitLatch(MyLatch,
				WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				wal_reader_sleep_time * 1000L,
				PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (curLSN >= lastRec)
		{
			/* constructing fullpath to wal file */
			initStringInfo(filePath);
			appendStringInfo(filePath, "%s%s", PG_WAL_DIR, fileName);
			
			elog(NOTICE, "[wal_read]\tlastRec: %ld", lastRec);
			elog(NOTICE, "[wal_read]\ttrying to read: %s", fileName);
			if (access(filePath->data, F_OK) == 0)
			{
				PrivateData pr_data;
				pr_data.file_path = filePath->data;
				pr_data.tli = 0;

				/* sets tli and segno */
				XLogFromFileName(fileName, &pr_data.tli, &segno, DEFAULT_XLOG_SEG_SIZE);

				lastRec = blockInfo(pr_data.tli, &pr_data, lastRec);		
				elog(NOTICE, "[wal_read]\tlastRec after blockInfo: %ld", lastRec);
				if (lastRec == -1) 
				{
					// TODO
				}
			}
			else
			{
				lastRec = curLSN;
			}
		}

		fileName = text_to_cstring(DatumGetTextP(DirectFunctionCall1(pg_walfile_name, ++lastRec)));		
		curLSN = DatumGetLSN(DirectFunctionCall1(pg_current_wal_lsn, NULL));
	}
}

void _PG_init(void)
{
	set_pg_wal_dir();
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(NOTICE, "false process_shared_preload_libraries_in_progress");
		return;
	}

	BackgroundWorker worker;
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_notify_pid = 0;
	
	sprintf(worker.bgw_library_name, "dmv");
	sprintf(worker.bgw_function_name, "wal_read");
	snprintf(worker.bgw_name, BGW_MAXLEN, "__dmv_bgw_walreader__");	
	RegisterBackgroundWorker(&worker);
}
