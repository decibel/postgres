/*-------------------------------------------------------------------------
 *
 * generic_xlog.h
 *	  POSTGRES generic XLOG definitions.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/generic_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GENERIC_XLOG_H
#define GENERIC_XLOG_H

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

extern void GenericXLogStart(Relation index);
extern Page GenericXLogRegister(Buffer buffer, bool isNew);
extern void GenericXLogMemmove(Pointer dst, Pointer src, OffsetNumber len);
extern XLogRecPtr GenericXLogFinish(void);
extern void GenericXLogAbort(void);

extern void generic_redo(XLogReaderState *record);
extern const char *generic_identify(uint8 info);
extern void generic_desc(StringInfo buf, XLogReaderState *record);

#endif   /* GENERIC_XLOG_H */
