#ifndef CONSTRAINT_H
#define CONSTRAINT_H

#include "nodes/parsenodes.h"

extern void CreateAssertion(CreateAssertionStmt *stmt);
extern void DropAssertion(DropStmt *drop);
extern Oid RenameAssertion(List *name, const char *newname);

#endif
