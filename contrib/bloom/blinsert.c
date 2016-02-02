/*-------------------------------------------------------------------------
 *
 * blinsert.c
 *		Bloom index build and insert functions.
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/bloom/blinsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "bloom.h"

PG_MODULE_MAGIC;

typedef struct
{
	BloomState		blstate;
	MemoryContext	tmpCtx;
	char			data[BLCKSZ];
	int64			count;
} BloomBuildState;

static void
flushBuildBuffer(Relation index, BloomBuildState *buildstate)
{
	Page	page;
	Buffer	buffer = BloomNewBuffer(index);

	GenericXLogStart(index);
	page = GenericXLogRegister(buffer, true);
	memcpy(page, buildstate->data, BLCKSZ);
	GenericXLogFinish();
	UnlockReleaseBuffer(buffer);
}

static void
bloomBuildCallback(Relation index, HeapTuple htup, Datum *values,
					bool *isnull, bool tupleIsAlive, void *state)
{
	BloomBuildState	*buildstate = (BloomBuildState*)state;
	MemoryContext	oldCtx;
	BloomTuple		*itup;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	itup = BloomFormTuple(&buildstate->blstate, &htup->t_self, values, isnull);

	if (BloomPageAddItem(&buildstate->blstate, buildstate->data, itup) == false)
	{
		flushBuildBuffer(index, buildstate);

		CHECK_FOR_INTERRUPTS();

		memset(buildstate->data, 0, BLCKSZ);
		BloomInitPage(buildstate->data, 0);
		buildstate->count = 0;

		if (BloomPageAddItem(&buildstate->blstate, buildstate->data, itup) == false)
			elog(ERROR, "can not add new tuple"); /* should not be here! */
	}
	else
	{
		buildstate->count++;
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

IndexBuildResult *
blbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult   *result;
	double				reltuples;
	BloomBuildState		buildstate;
	Buffer				metaBuffer;
	Page				metaPage;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			RelationGetRelationName(index));

	/* initialize the meta page */
	metaBuffer = BloomNewBuffer(index);
	GenericXLogStart(index);
	metaPage = GenericXLogRegister(metaBuffer, true);
	BloomInitMetapage(metaPage, index);
	GenericXLogFinish();
	UnlockReleaseBuffer(metaBuffer);

	initBloomState(&buildstate.blstate, index);

	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
												"Bloom build temporary context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	memset(buildstate.data, 0, BLCKSZ);
	BloomInitPage(buildstate.data, 0);
	buildstate.count = 0;

	reltuples = IndexBuildHeapScan(heap, index, indexInfo, true,
									bloomBuildCallback, (void *) &buildstate);

	if (buildstate.count > 0)
		flushBuildBuffer(index, &buildstate);

	MemoryContextDelete(buildstate.tmpCtx);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = result->index_tuples = reltuples;

	return result;
}

void
blbuildempty(Relation index)
{
	Buffer		metaBuffer;
	Page		metaPage;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			RelationGetRelationName(index));

	/* initialize the meta page */
	metaBuffer = BloomNewBuffer(index);

	GenericXLogStart(index);
	metaPage = GenericXLogRegister(metaBuffer, true);
	BloomInitMetapage(metaPage, index);
	GenericXLogFinish();

	UnlockReleaseBuffer(metaBuffer);
}

bool
blinsert(Relation index, Datum *values, bool *isnull,
		ItemPointer ht_ctid, Relation heapRel, IndexUniqueCheck checkUnique)
{
	BloomState			blstate;
	BloomTuple		   *itup;
	MemoryContext		oldCtx;
	MemoryContext		insertCtx;
	BloomMetaPageData  *metaData;
	Buffer				buffer,
						metaBuffer;
	Page				page,
						metaPage;
	BlockNumber			blkno = InvalidBlockNumber;
	OffsetNumber		nStart;

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
										"Bloom insert temporary context",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	initBloomState(&blstate, index);
	itup = BloomFormTuple(&blstate, ht_ctid, values, isnull);

	metaBuffer = ReadBuffer(index, BLOOM_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
	metaData = BloomPageGetMeta(BufferGetPage(metaBuffer));

	if (metaData->nEnd > metaData->nStart)
	{
		Page	page;

		blkno = metaData->notFullPage[ metaData->nStart ];

		Assert(blkno != InvalidBlockNumber);
		LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		GenericXLogStart(index);
		page = GenericXLogRegister(buffer, false);

		if (BloomPageAddItem(&blstate, page, itup))
		{
			GenericXLogFinish();
			UnlockReleaseBuffer(buffer);
			ReleaseBuffer(metaBuffer);
			goto away;
		}
		else
		{
			GenericXLogAbort();
			UnlockReleaseBuffer(buffer);
		}
	}
	else
	{
		/* no avaliable pages */
		LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
	}

	/* protect any changes on metapage with a help of CRIT_SECTION */

	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);

	GenericXLogStart(index);
	metaPage = GenericXLogRegister(metaBuffer, false);
	metaData = BloomPageGetMeta(metaPage);

	nStart = metaData->nStart;
	if (metaData->nEnd >nStart &&
		blkno == metaData->notFullPage[ nStart ] )
		nStart++;

	while (metaData->nEnd > nStart)
	{
		blkno = metaData->notFullPage[ nStart ];
		Assert(blkno != InvalidBlockNumber);

		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = GenericXLogRegister(buffer, false);

		if (BloomPageAddItem(&blstate, page, itup))
		{
			GenericXLogFinish();
			UnlockReleaseBuffer(buffer);
			UnlockReleaseBuffer(metaBuffer);
			goto away;
		}
		else
		{
			UnlockReleaseBuffer(buffer);
		}
		nStart++;
	}

	GenericXLogAbort();

	buffer = BloomNewBuffer(index);

	GenericXLogStart(index);
	metaPage = GenericXLogRegister(metaBuffer, false);
	metaData = BloomPageGetMeta(metaPage);
	page = GenericXLogRegister(buffer, true);
	BloomInitPage(page, 0);
	BloomPageAddItem(&blstate, page, itup);

	metaData->nStart = 0;
	metaData->nEnd = 1;
	metaData->notFullPage[ 0 ] = BufferGetBlockNumber(buffer);

	GenericXLogFinish();

	UnlockReleaseBuffer(buffer);
	UnlockReleaseBuffer(metaBuffer);

away:
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	return false;
}
