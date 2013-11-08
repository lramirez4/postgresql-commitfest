/*-------------------------------------------------------------------------
 *
 * ddl_rewrite.h
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/ddl_rewrite.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DDL_REWRITE_H
#define DDL_REWRITE_H

extern char *rewrite_utility_command(Oid objectId, const char *identity,
						Node *parsetree);

#endif	/* DDL_REWRITE_H */
