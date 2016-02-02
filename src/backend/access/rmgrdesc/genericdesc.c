/*-------------------------------------------------------------------------
 *
 * genericdesc.c
 *	  rmgr descriptor routines for access/transam/generic_xlog.c
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/genericdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"

void
generic_desc(StringInfo buf, XLogReaderState *record)
{
	return;
}

const char *
generic_identify(uint8 info)
{
	return "Generic";
}
