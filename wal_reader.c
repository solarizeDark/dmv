#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>

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
#include "postmaster/bgworker.h"

#include "dmv.h"

typedef struct PrivateData
{
	char* 		file_path;
	TimeLineID	tli;
} PrivateData;

PG_MODULE_MAGIC;

pid_t wal_reader_pid = -1;
uint64 segno = 0;
char PG_WAL_DIR[50];
static volatile sig_atomic_t gotSignal = false;

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
		elog(NOTICE, "%s", private_data->file_path);
		elog(ERROR, "%s", "file open error\n");
		exit(1);
	} else {
		elog(NOTICE, "reading: %s", private_data->file_path);		
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
		exit(1);
	}

	return XLOG_BLCKSZ;
}

/*
	get resource manager for last decoded record
	if rm is heap, then switch for heap operation
*/
void getInfo (XLogReaderState *xlogreader)
{
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
					if (fork != MAIN_FORKNUM) continue;		/* other forks for metadata */
					if (is_target(rlocator.relNumber)) { 	/* rlocator.relNumber - OID of relation */

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
							CatalogTupleInsert(tempRel, &readableTup);
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

	xlogreader = XLogReaderAllocate(DEFAULT_XLOG_SEG_SIZE, NULL, &callbacks, private_data);	/* inits reader, sets private_data */

	if (xlogreader == NULL)
	{
		elog(NOTICE, "%s", "null reader");
	}

	/* firstRec == create dmv function call lsn, finds first lsn >= firstRec and positions reader to that point */
	curRec = XLogFindNextRecord(xlogreader, firstRec);

	if (XLogRecPtrIsInvalid(curRec))
	{
		elog(NOTICE, "curRec %ld", firstRec);
		elog(NOTICE, "no log entries");
		exit(1);
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
			elog(ERROR, "%s", errmsg);

		/* fetching info from record that we read */
		getInfo(xlogreader);
	} while (record != NULL);

	lastRec = xlogreader->ReadRecPtr;
	XLogReaderFree(xlogreader);

	return lastRec;
}

/*
	dmvLSN - point to start reading WAL from
*/
void* single_dmv_loop(void* args) // (Oid dmvOid, Datum dmvLSN)
{
	// BGWorkerArgs* arg = (BGWorkerArgs*) args;
	
	// XLogRecPtr lastRec = DatumGetLSN(arg->lsn);
	// StringInfo filePath = makeStringInfo();

	// XLogRecPtr curLSN = DatumGetLSN(DirectFunctionCall1(pg_current_wal_lsn, NULL));

	// elog(NOTICE, "preloop lsn: %ld", lastRec);
	// elog(NOTICE, "preloop cur rec: %ld", curLSN);
	
	// char *fileName = text_to_cstring(DatumGetTextP(DirectFunctionCall1(pg_walfile_name, arg->lsn)));

	// while (1)
	// {
	// 	if (curLSN >= lastRec)
	// 	{
	// 		initStringInfo(filePath);
	// 		appendStringInfo(filePath, "%s%s", PG_WAL_DIR, fileName);
				
	// 		PrivateData pr_data;
	// 		pr_data.file_path = filePath->data;
	// 		pr_data.tli = 0;

	// 		// sets tli and segno
	// 		XLogFromFileName(fileName, &pr_data.tli, &segno, DEFAULT_XLOG_SEG_SIZE);
	// 		lastRec = blockInfo(pr_data.tli, &pr_data, lastRec);		
	// 		elog(NOTICE, "last rec: %ld", lastRec);
	// 		if (lastRec == -1) 
	// 		{
	// 			elog(NOTICE, "%s", "sleep");
	// 			sleep(20);
	// 		} else 
	// 		{
	// 			fileName = text_to_cstring(DatumGetTextP(DirectFunctionCall1(pg_walfile_name, lastRec)));		
	// 		}
	// 	} 
	// 	else
	// 	{
	// 		elog(NOTICE, "loop cur rec: %ld", curLSN);
	// 		sleep(20);			
	// 	}

	// 	curLSN = DatumGetLSN(DirectFunctionCall1(pg_current_wal_lsn, NULL));

	// }

}

static void sig_handler(SIGNAL_ARGS)
{
	gotSignal = true;
}

sig_atomic_t got_sigterm = false;
static void reader_sig_term(SIGNAL_ARGS)
{
	got_sigterm = true;
}

void wal_read(Datum main_arg) //Oid dmvOid, Datum LSN)
{
	pqsignal(SIGTERM, reader_sig_term);
	BackgroundWorkerUnblockSignals();
	while (!got_sigterm)
	{
		elog(NOTICE,"%s", "no signal");
		sleep(20);
	}
	
	pthread_t thread;
	// pthread_create(&thread, NULL, single_dmv_loop, (void *)&bgworkerArgs);
	// pthread_join(thread, NULL);
}

void _PG_init(void)
{
	set_pg_wal_dir();
	printf("%s\n", PG_WAL_DIR);
	// ereport(INFO, "FOO");

/*
	BackgroundWorker worker;
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 1;
	sprintf(worker.bgw_library_name, "dmv");
	sprintf(worker.bgw_function_name, "wal_read");
	snprintf(worker.bgw_name, BGW_MAXLEN, "walreader");	
	RegisterBackgroundWorker(&worker);
	wal_reader_pid = MyProcPid;
*/
}
