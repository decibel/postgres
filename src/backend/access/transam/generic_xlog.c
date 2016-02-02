/*-------------------------------------------------------------------------
 *
 * generic_xlog.c
 *	  WAL replay logic for generic XLOG.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			 src/backend/access/transam/generic_xlog.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#define MAX_REGIONS				256
#define MATCH_THRESHOLD			 16
#define MAX_GENERIC_XLOG_PAGES	  3

#define	MOVE_FLAG			 0x8000
#define	LENGTH_MASK			 0x7FFF

/* #define DEBUG_PRINT */

typedef struct
{
	OffsetNumber dstOffset, srcOffset, length;
} Region;

typedef struct
{
	Buffer	buffer;
	char	image[BLCKSZ];
	Region	regions[MAX_REGIONS];
	int		regionsCount;
	bool	overflow;
	char	data[2 * BLCKSZ];
	int		dataLen;
	bool	fullImage;
} PageData;

enum GenericXlogStatus
{
	GXLOG_NOT_STARTED,
	GXLOG_LOGGED,
	GXLOG_UNLOGGED
};


static enum GenericXlogStatus	genericXlogStatus = GXLOG_NOT_STARTED;
static PageData					pages[MAX_GENERIC_XLOG_PAGES];

static int
regionOffsetCmp(const void *a, const void *b)
{
	OffsetNumber off = *((const OffsetNumber *)a);
	const Region *r = (const Region *)b;

	if (off < r->dstOffset)
		return -1;
	else if (off >= r->dstOffset + r->length)
		return 1;
	else
		return 0;
}

static void
initPageData(PageData *pageData, Buffer buffer)
{
	pageData->buffer = buffer;
	memcpy(pageData->image, BufferGetPage(buffer), BLCKSZ);
	pageData->regions[0].srcOffset = 0;
	pageData->regions[0].dstOffset = 0;
	pageData->regions[0].length = BLCKSZ;
	pageData->regionsCount = 1;
	pageData->dataLen = 0;
}

static Region *
findRegion(PageData *pageData, OffsetNumber offset)
{
	Region	*region;
	Assert(offset >= 0 && offset < BLCKSZ);
	region = bsearch(&offset, pageData->regions, pageData->regionsCount,
		sizeof(Region), regionOffsetCmp);

	/* We should always find region... */
	if (!region)
		elog(ERROR, "Can't find region");

	return region;
}

static void
memoryMove(PageData *pageData, OffsetNumber dstOffset, OffsetNumber srcOffset,
	OffsetNumber length)
{
	OffsetNumber curOffset, curLength, leftLength = 0;
	Region	newRegions[MAX_REGIONS], *newRegion, *srcRegion, *dstRegionLeft, *dstRegionRight;
	bool leftAdjacent, rightAdjacent;
	int newRegionsCount, leftIndex, rightIndex, shift;
#ifdef DEBUG_PRINT
	int i;
#endif

	if (pageData->overflow)
		return;

	srcRegion = findRegion(pageData, srcOffset);

	/* Make new regions */
	curOffset = dstOffset;
	curLength = length;
	newRegion = newRegions;
	while (curLength > 0)
	{
		OffsetNumber shift = srcOffset - srcRegion->dstOffset;

		newRegion->dstOffset = curOffset;
		newRegion->srcOffset = srcRegion->srcOffset + shift;
		newRegion->length = Min(srcRegion->length - shift, curLength);
		srcOffset += newRegion->length;
		curLength -= newRegion->length;
		srcRegion++;
		newRegion++;
	}
	newRegionsCount = newRegion - newRegions;

	/* Check left region */
	dstRegionLeft = findRegion(pageData, dstOffset > 0 ? dstOffset - 1 : 0);
	newRegion = &newRegions[0];
	shift = dstOffset - dstRegionLeft->dstOffset;

	if (shift == 0)
	{
		leftAdjacent = true;
	}
	else if (newRegion->srcOffset == dstRegionLeft->srcOffset + shift)
	{
		leftAdjacent = true;
		newRegion->dstOffset -= shift;
		newRegion->srcOffset -= shift;
		newRegion->length += shift;
	}
	else
	{
		leftAdjacent = false;
		leftLength = shift;
	}
	leftIndex = dstRegionLeft - pageData->regions + (leftAdjacent ? 0 : 1);

	/* Check right region */
	dstRegionRight = findRegion(pageData, dstOffset + length < BLCKSZ ? dstOffset + length : BLCKSZ - 1);
	newRegion = &newRegions[newRegionsCount - 1];
	shift = (dstRegionRight->dstOffset + dstRegionRight->length) -
			(dstOffset + length);

	if (shift == 0)
	{
		rightAdjacent = true;
	}
	else if (newRegion->srcOffset + newRegion->length + shift ==
			 dstRegionRight->srcOffset + dstRegionRight->length)
	{
		rightAdjacent = true;
		newRegion->length += shift;
	}
	else
	{
		Region *tmp = dstRegionRight;

		if (!leftAdjacent)
		{
			newRegionsCount++;
			tmp = &newRegions[newRegionsCount - 1];
			*tmp = *dstRegionRight;
			rightAdjacent = true;
		}
		else
		{
			rightAdjacent = false;
		}
		tmp->srcOffset += (tmp->length - shift);
		tmp->dstOffset += (tmp->length - shift);
		tmp->length = shift;
	}
	if (!leftAdjacent)
		dstRegionLeft->length = leftLength;
	rightIndex = dstRegionRight - pageData->regions + (rightAdjacent ? 1 : 0);

	/* Move data */
	shift = newRegionsCount - (rightIndex - leftIndex);
	if (pageData->regionsCount + shift > MAX_REGIONS)
	{
		initPageData(pageData, pageData->buffer);
		pageData->overflow = true;
		return;
	}

#ifdef DEBUG_PRINT
	for (i = 0; i < pageData->regionsCount; i++)
	{
		Region *region = &pageData->regions[i];
		printf("old %d %d %d %d\n", i, region->dstOffset, region->srcOffset, region->length);
	}
	for (i = 0; i < newRegionsCount; i++)
	{
		Region *region = &newRegions[i];
		printf("new %d %d %d %d\n", i, region->dstOffset, region->srcOffset, region->length);
	}
	printf("idx %d %d\n", leftIndex, rightIndex);
#endif

	memmove(&pageData->regions[rightIndex + shift],
			&pageData->regions[rightIndex],
			sizeof(Region) * (pageData->regionsCount - rightIndex));
	pageData->regionsCount += shift;
	memcpy(&pageData->regions[leftIndex],
		   newRegions,
		   sizeof(Region) * newRegionsCount);
}

#ifdef DEBUG_PRINT
static void
printPageData(PageData *pageData)
{
	int i;
	for (i = 0; i < pageData->regionsCount; i++)
	{
		Region *region = &pageData->regions[i];
		printf("cur %d %d %d %d\n", i, region->dstOffset, region->srcOffset, region->length);
	}
}

Datum
check_move(PG_FUNCTION_ARGS)
{
	PageData pageData;
	initPageData(&pageData, false);
	printPageData(&pageData);
	memoryMove(&pageData, 0, 4096, 4096);
	printPageData(&pageData);
	memoryMove(&pageData, 0, 2048, 2048);
	printPageData(&pageData);
	memoryMove(&pageData, 0, 1024, 1024);
	printPageData(&pageData);
	PG_RETURN_VOID();
}
#endif

static void
writeCopyFlagment(PageData *pageData, OffsetNumber length, Pointer source)
{
	Pointer ptr = pageData->data + pageData->dataLen;

	memcpy(ptr, &length, sizeof(OffsetNumber));
	ptr += sizeof(OffsetNumber);
	memcpy(ptr, source, length);
	ptr += length;

	pageData->dataLen = ptr - pageData->data;
}

static void
writeMoveFlagment(PageData *pageData, OffsetNumber length, OffsetNumber source)
{
	Pointer ptr = pageData->data + pageData->dataLen;

	length |= MOVE_FLAG;
	memcpy(ptr, &length, sizeof(OffsetNumber));
	ptr += sizeof(OffsetNumber);
	memcpy(ptr, &source, sizeof(OffsetNumber));
	ptr += sizeof(OffsetNumber);

	pageData->dataLen = ptr - pageData->data;
}

#define CHECK_SET \
			if (i - match > MATCH_THRESHOLD) \
			{ \
				if (notMatch < match) \
					writeCopyFlagment(pageData, match - notMatch, page + notMatch); \
				writeMoveFlagment(pageData, i - match, region->srcOffset + regionOffset - (i - match)); \
				notMatch = i; \
			} \

static void
writeDifferentialData(PageData *pageData)
{
	Page page = BufferGetPage(pageData->buffer);
	Pointer image = pageData->image;
	Region *region = pageData->regions;
	OffsetNumber i, regionOffset = 0;
	OffsetNumber notMatch = 0, match = 0;

	for (i = 0; i < BLCKSZ; i++)
	{
		if (regionOffset >= region->length)
		{
			CHECK_SET;
			match = i;
			region++;
			regionOffset = 0;
		}

		if (page[i] != image[region->srcOffset + regionOffset])
		{
			CHECK_SET;
			match = i + 1;
		}

		regionOffset++;
	}
	CHECK_SET;
	if (notMatch < BLCKSZ)
	{
		writeCopyFlagment(pageData, BLCKSZ - notMatch, page + notMatch);
	}
}

void
GenericXLogStart(Relation index)
{
	int i;

	if (genericXlogStatus != GXLOG_NOT_STARTED)
		elog(ERROR, "GenericXLogStart: generic xlog is already started");

	genericXlogStatus = RelationNeedsWAL(index) ? GXLOG_LOGGED : GXLOG_UNLOGGED;

	for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
	{
		pages[i].buffer = InvalidBuffer;
	}
}

Page
GenericXLogRegister(Buffer buffer, bool isNew)
{
	int block_id;

	if (genericXlogStatus == GXLOG_NOT_STARTED)
		elog(ERROR, "GenericXLogRegister: generic xlog isn't started");

	for (block_id = 0; block_id < MAX_GENERIC_XLOG_PAGES; block_id++)
	{
		if (BufferIsInvalid(pages[block_id].buffer))
		{
			initPageData(&pages[block_id], buffer);
			pages[block_id].fullImage = isNew;
			return (Page)pages[block_id].image;
		}
		else if (pages[block_id].buffer == buffer)
		{
			elog(ERROR, "GenericXLogRegister: duplicate buffer %d", buffer);
		}
	}

	elog(ERROR, "GenericXLogRegister: maximum number of %d buffers is exceeded", 
		MAX_GENERIC_XLOG_PAGES);

	/* keep compiler quiet */
	return NULL;
}

void
GenericXLogMemmove(Pointer dst, Pointer src, OffsetNumber len)
{
	Page page;
	int i, block_id = -1;

	if (genericXlogStatus == GXLOG_NOT_STARTED)
		elog(ERROR, "GenericXLogMemmove: generic xlog isn't started");

	/* Find block */
	for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
	{
		page = pages[i].image;

		if (dst >= page && dst < page + BLCKSZ)
		{
			block_id = i;
			break;
		}
	}

	/* Check block is found */
	if (block_id < 0)
		elog(ERROR, "GenericXLogMemmove: page not found");

	/* Check both source and destination of memmove are inside of page */
	if (src < page || src + len > page + BLCKSZ)
		elog(ERROR, "GenericXLogMemmove: source is outside of page");
	if (dst < page || dst + len > page + BLCKSZ)
		elog(ERROR, "GenericXLogMemmove: desitnation is outside of page");

	if (pages[block_id].fullImage)
		return;

	memoryMove(&pages[block_id], dst - page, src - page, len);
	memmove(dst, src, len);
}

#ifdef DEBUG_PRINT
static void
printDataBlock(Pointer ptr, Pointer end)
{
	OffsetNumber length, offset = 0, source;

	while (ptr < end)
	{
		memcpy(&length, ptr, sizeof(length));
		ptr += sizeof(length);

		if (length & MOVE_FLAG)
		{
			length &= LENGTH_MASK;
			memcpy(&source, ptr, sizeof(source));
			ptr += sizeof(source);
			printf("\t%d: move %d from %d\n", offset, length, source);
		}
		else
		{
			OffsetNumber i;
			printf("\t%d: set %d -", offset, length);
			for (i = 0; i < length; i++)
			{
				printf(" %02X", (uint8)ptr[i]);
			}
			printf("\n");
			ptr += length;
		}
		offset += length;
	}
}
#endif

XLogRecPtr
GenericXLogFinish(void)
{
	XLogRecPtr lsn = InvalidXLogRecPtr;
	int i;

	if (genericXlogStatus == GXLOG_LOGGED)
	{
		START_CRIT_SECTION();
		XLogBeginInsert();

		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			char	tmp[BLCKSZ];

			if (BufferIsInvalid(pages[i].buffer))
				continue;

			memcpy(tmp, pages[i].image, BLCKSZ);
			memcpy(pages[i].image, BufferGetPage(pages[i].buffer), BLCKSZ);
			memcpy(BufferGetPage(pages[i].buffer), tmp, BLCKSZ);

			if (pages[i].fullImage)
			{
				XLogRegisterBuffer(i, pages[i].buffer, REGBUF_FORCE_IMAGE);
			}
			else
			{
				XLogRegisterBuffer(i, pages[i].buffer, REGBUF_STANDARD);
				writeDifferentialData(&pages[i]);
				XLogRegisterBufData(i, pages[i].data, pages[i].dataLen);
			}
		}

		lsn = XLogInsert(RM_GENERIC_ID, 0);
		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			if (BufferIsInvalid(pages[i].buffer))
				continue;
			PageSetLSN(BufferGetPage(pages[i].buffer), lsn);
			MarkBufferDirty(pages[i].buffer);
		}
		END_CRIT_SECTION();
	}
	else if (genericXlogStatus == GXLOG_UNLOGGED)
	{
		START_CRIT_SECTION();
		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			if (BufferIsInvalid(pages[i].buffer))
				continue;
			memcpy(BufferGetPage(pages[i].buffer), pages[i].image, BLCKSZ);
			MarkBufferDirty(pages[i].buffer);
		}
		END_CRIT_SECTION();
	}
	else
	{
		elog(ERROR, "GenericXLogFinish: generic xlog isn't started");
	}

#ifdef DEBUG_PRINT
	for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
	{
		if (BufferIsInvalid(pages[i].buffer))
			continue;
		printf("Block %08X\n", BufferGetBlockNumber(pages[i].buffer));
		if (pages[i].fullImage)
			printf("full image is forced");
		else
			printDataBlock(pages[i].data, pages[i].data + pages[i].dataLen);
	}
#endif

	genericXlogStatus = GXLOG_NOT_STARTED;

	return lsn;
}

void
GenericXLogAbort(void)
{
	if (genericXlogStatus == GXLOG_NOT_STARTED)
		elog(ERROR, "GenericXLogMemmove: generic xlog isn't started");

	genericXlogStatus = GXLOG_NOT_STARTED;
}

static void
applyPageRedo(Page page, Page image, Pointer data, Size dataSize)
{
	OffsetNumber length, offset = 0, source;
	Pointer ptr = data, end = data + dataSize;

	while (ptr < end)
	{
		memcpy(&length, ptr, sizeof(length));
		ptr += sizeof(length);

		if (length & MOVE_FLAG)
		{
			length &= LENGTH_MASK;
			memcpy(&source, ptr, sizeof(source));
			ptr += sizeof(source);

			memcpy(page + offset, image + source, length);
		}
		else
		{
			memcpy(page + offset, ptr, length);

			ptr += length;
		}
		offset += length;
	}
}

void
generic_redo(XLogReaderState *record)
{
	uint8		block_id;
	Buffer		buffers[MAX_GENERIC_XLOG_PAGES] = {InvalidBuffer};
	XLogRecPtr	lsn = record->EndRecPtr;
	char		image[BLCKSZ];

	Assert(record->max_block_id < MAX_GENERIC_XLOG_PAGES);

#ifdef DEBUG_PRINT
	elog(LOG, "Generic redo: %d (pid = %d)!", record->max_block_id, MyProcPid);
#endif

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		XLogRedoAction action;

		if (!XLogRecHasBlockRef(record, block_id))
			continue;
		action = XLogReadBufferForRedo(record, block_id, &buffers[block_id]);
#ifdef DEBUG_PRINT
		elog(LOG, "%d - %d", block_id, action);
#endif
		if (action == BLK_NEEDS_REDO)
		{
			Pointer	blockData;
			Size	blockDataSize;
			Page	page;

			page = BufferGetPage(buffers[block_id]);
			memcpy(image, page, BLCKSZ);

			blockData = XLogRecGetBlockData(record, block_id, &blockDataSize);

#ifdef DEBUG_PRINT
			printDataBlock(blockData, blockData + blockDataSize);
#endif

			applyPageRedo(page, image, blockData, blockDataSize);

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffers[block_id]);
		}
#ifdef DEBUG_PRINT
		if (action == BLK_RESTORED)
		{
			int i;
			RestoreBlockImage(record, block_id, image);
			printf("Restored, image: ");
			for (i = 0; i < BLCKSZ; i++)
			{
				printf("%02X ", (uint8)image[i]);
			}
			printf("\n");
		}
#endif
	}

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		if (BufferIsValid(buffers[block_id]))
			UnlockReleaseBuffer(buffers[block_id]);
	}
}
