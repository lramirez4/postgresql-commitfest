/*-------------------------------------------------------------------------
 *
 * constraint.c
 *	  PostgreSQL CONSTRAINT support code.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/constraint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_trigger.h"
#include "commands/constraint.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/*
 * unique_key_recheck - trigger function to do a deferred uniqueness check.
 *
 * This now also does deferred exclusion-constraint checks, so the name is
 * somewhat historical.
 *
 * This is invoked as an AFTER ROW trigger for both INSERT and UPDATE,
 * for any rows recorded as potentially violating a deferrable unique
 * or exclusion constraint.
 *
 * This may be an end-of-statement check, a commit-time check, or a
 * check triggered by a SET CONSTRAINTS command.
 */
Datum
unique_key_recheck(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	const char *funcname = "unique_key_recheck";
	HeapTuple	new_row;
	ItemPointerData tmptid;
	Relation	indexRel;
	IndexInfo  *indexInfo;
	EState	   *estate;
	ExprContext *econtext;
	TupleTableSlot *slot;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	/*
	 * Make sure this is being called as an AFTER ROW trigger.	Note:
	 * translatable error strings are shared with ri_triggers.c, so resist the
	 * temptation to fold the function name into them.
	 */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager",
						funcname)));

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired AFTER ROW",
						funcname)));

	/*
	 * Get the new data that was inserted/updated.
	 */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		new_row = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		new_row = trigdata->tg_newtuple;
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired for INSERT or UPDATE",
						funcname)));
		new_row = NULL;			/* keep compiler quiet */
	}

	/*
	 * If the new_row is now dead (ie, inserted and then deleted within our
	 * transaction), we can skip the check.  However, we have to be careful,
	 * because this trigger gets queued only in response to index insertions;
	 * which means it does not get queued for HOT updates.	The row we are
	 * called for might now be dead, but have a live HOT child, in which case
	 * we still need to make the check.  Therefore we have to use
	 * heap_hot_search, not just HeapTupleSatisfiesVisibility as is done in
	 * the comparable test in RI_FKey_check.
	 *
	 * This might look like just an optimization, because the index AM will
	 * make this identical test before throwing an error.  But it's actually
	 * needed for correctness, because the index AM will also throw an error
	 * if it doesn't find the index entry for the row.  If the row's dead then
	 * it's possible the index entry has also been marked dead, and even
	 * removed.
	 */
	tmptid = new_row->t_self;
	if (!heap_hot_search(&tmptid, trigdata->tg_relation, SnapshotSelf, NULL))
	{
		/*
		 * All rows in the HOT chain are dead, so skip the check.
		 */
		return PointerGetDatum(NULL);
	}

	/*
	 * Open the index, acquiring a RowExclusiveLock, just as if we were going
	 * to update it.  (This protects against possible changes of the index
	 * schema, not against concurrent updates.)
	 */
	indexRel = index_open(trigdata->tg_trigger->tgconstrindid,
						  RowExclusiveLock);
	indexInfo = BuildIndexInfo(indexRel);

	/*
	 * The heap tuple must be put into a slot for FormIndexDatum.
	 */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(trigdata->tg_relation));

	ExecStoreTuple(new_row, slot, InvalidBuffer, false);

	/*
	 * Typically the index won't have expressions, but if it does we need an
	 * EState to evaluate them.  We need it for exclusion constraints too,
	 * even if they are just on simple columns.
	 */
	if (indexInfo->ii_Expressions != NIL ||
		indexInfo->ii_ExclusionOps != NULL)
	{
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);
		econtext->ecxt_scantuple = slot;
	}
	else
		estate = NULL;

	/*
	 * Form the index values and isnull flags for the index entry that we need
	 * to check.
	 *
	 * Note: if the index uses functions that are not as immutable as they are
	 * supposed to be, this could produce an index tuple different from the
	 * original.  The index AM can catch such errors by verifying that it
	 * finds a matching index entry with the tuple's TID.  For exclusion
	 * constraints we check this in check_exclusion_constraint().
	 */
	FormIndexDatum(indexInfo, slot, estate, values, isnull);

	/*
	 * Now do the appropriate check.
	 */
	if (indexInfo->ii_ExclusionOps == NULL)
	{
		/*
		 * Note: this is not a real insert; it is a check that the index entry
		 * that has already been inserted is unique.
		 */
		index_insert(indexRel, values, isnull, &(new_row->t_self),
					 trigdata->tg_relation, UNIQUE_CHECK_EXISTING);
	}
	else
	{
		/*
		 * For exclusion constraints we just do the normal check, but now it's
		 * okay to throw error.
		 */
		check_exclusion_constraint(trigdata->tg_relation, indexRel, indexInfo,
								   &(new_row->t_self), values, isnull,
								   estate, false, false);
	}

	/*
	 * If that worked, then this index entry is unique or non-excluded, and we
	 * are done.
	 */
	if (estate != NULL)
		FreeExecutorState(estate);

	ExecDropSingleTupleTableSlot(slot);

	index_close(indexRel, RowExclusiveLock);

	return PointerGetDatum(NULL);
}


Datum
assertion_check(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	const char *funcname = "assertion_check";
	Oid			constraintOid;
	HeapTuple	tup;
	Datum		adatum;
	bool		isNull;
	char	   *query;
	int			ret;

	/*
	 * Make sure this is being called as an AFTER STATEMENT trigger.	Note:
	 * translatable error strings are shared with ri_triggers.c, so resist the
	 * temptation to fold the function name into them.
	 */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager",
						funcname)));

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired AFTER STATEMENT",
						funcname)));

	constraintOid = trigdata->tg_trigger->tgconstraint;
	tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for constraint %u", constraintOid);

	// XXX bogus
	adatum = SysCacheGetAttr(CONSTROID, tup,
							 Anum_pg_constraint_consrc, &isNull);
	if (isNull)
		elog(ERROR, "constraint %u has null consrc", constraintOid);

	SPI_connect();
	query = psprintf("SELECT %s", TextDatumGetCString(adatum));
	ret = SPI_exec(query, 1);

	if (ret > 0 && SPI_tuptable != NULL)
	{
		Datum val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isNull);
		if (!isNull && DatumGetBool(val) == false)
			ereport(ERROR,
					(errcode(ERRCODE_CHECK_VIOLATION),
					 errmsg("assertion \"%s\" violated",
							get_constraint_name(constraintOid))));
	}
	else
		elog(ERROR, "unexpected SPI result");

	SPI_finish();
	ReleaseSysCache(tup);
	return PointerGetDatum(NULL);
}


struct collect_used_tables_context
{
	List *rtable;
	List *rels;
};

static bool
collect_used_tables_walker(Node *node, struct collect_used_tables_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeTblRef))
	{
		RangeTblRef *ref;
		Oid relid;

		ref= (RangeTblRef *) node;
		relid = getrelid(ref->rtindex, context->rtable);
		context->rels = list_append_unique_oid(context->rels, relid);
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query;
		List *old_rtable;
		bool result;

		query = (Query *) node;
		old_rtable = context->rtable;
		context->rtable = query->rtable;
		result = query_tree_walker(query, collect_used_tables_walker, context, 0);
		context->rtable = old_rtable;
		return result;
	}

	return expression_tree_walker(node, collect_used_tables_walker, (void *) context);
}

void
CreateAssertion(CreateAssertionStmt *stmt)
{
	Oid			namespaceId;
	char	   *assertion_name;
	AclResult	aclresult;
	Node	   *expr;
	ParseState *pstate;
	char	   *ccsrc;
	char	   *ccbin;
	Oid			constrOid;
	struct collect_used_tables_context context;
	ListCell   *lc;

	namespaceId = QualifiedNameGetCreationNamespace(stmt->assertion_name,
													&assertion_name);

	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(namespaceId));

	// TODO: check constraint name

	pstate = make_parsestate(NULL);
	expr = transformExpr(pstate, stmt->constraint->raw_expr, EXPR_KIND_ASSERTION_CHECK);
	expr = coerce_to_boolean(pstate, expr, "CHECK");

	ccbin = nodeToString(expr);
	ccsrc = deparse_expression(expr, NIL, false, false);

	constrOid = CreateConstraintEntry(assertion_name,
						  namespaceId,
						  CONSTRAINT_CHECK, /* constraint type */
						  stmt->constraint->deferrable,
						  stmt->constraint->initdeferred,
						  !stmt->constraint->skip_validation,
						  InvalidOid, /* not a relation constraint */
						  NULL,		  /* no keys */
						  0,		  /* no keys */
						  InvalidOid, /* not a domain constraint */
						  InvalidOid, /* no associated index */
						  InvalidOid, /* foreign key fields ... */
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  0,
						  ' ',
						  ' ',
						  ' ',
						  NULL,	/* not an exclusion constraint */
						  expr, /* tree form of check constraint */
						  ccbin, /* binary form of check constraint */
						  ccsrc, /* source form of check constraint */
						  true, /* is local */
						  0,   /* inhcount */
						  false, /* noinherit XXX */
						  false); /* is_internal */

	context.rtable = NIL;
	context.rels = NIL;
	collect_used_tables_walker(expr, &context);

	foreach (lc, context.rels)
	{
		Oid relid = lfirst_oid(lc);
		CreateTrigStmt *trigger;
		Relation rel;
		ObjectAddress myself,
			referenced;

		rel = heap_open(relid, ShareLock); // XXX

		trigger = makeNode(CreateTrigStmt);
		trigger->trigname = "AssertionTrigger";
		trigger->relation = makeRangeVar(get_namespace_name(namespaceId),
										 pstrdup(RelationGetRelationName(rel)),
										 -1);
		trigger->funcname = SystemFuncName("assertion_check");
		trigger->args = NIL;
		trigger->row = false;
		trigger->timing = TRIGGER_TYPE_AFTER;
		trigger->events = TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_TRUNCATE;
		trigger->columns = NIL;
		trigger->whenClause = NULL;
		trigger->isconstraint = true;
		trigger->deferrable = stmt->constraint->deferrable;
		trigger->initdeferred = stmt->constraint->initdeferred;
		trigger->constrrel = NULL;

		CreateTrigger(trigger, NULL, constrOid, InvalidOid, true);

		heap_close(rel, NoLock);

		/*
		 * Record a dependency between the constraint and the table.
		 */

		myself.classId = ConstraintRelationId;
		myself.objectId = constrOid;
		myself.objectSubId = 0;

		referenced.classId = RelationRelationId;
		referenced.objectId = relid;
		referenced.objectSubId = 0;

		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
}


void
DropAssertion(DropStmt *drop)
{
	ObjectAddresses *objects;
	ListCell   *cell;

	/*
	 * First we identify all the assertions, then we delete them in a single
	 * performMultipleDeletions() call.  This is to avoid unwanted DROP
	 * RESTRICT errors if one of the assertions depends on another. (Not that
	 * that is very likely, but we may as well do this consistently.)
	 */
	objects = new_object_addresses();

	foreach(cell, drop->objects)
	{
		List	   *name = (List *) lfirst(cell);
		Oid			assertionOid;
		HeapTuple	tuple;
		Form_pg_constraint con;
		ObjectAddress object;

		assertionOid = get_assertion_oid(name, drop->missing_ok);

		if (!OidIsValid(assertionOid))
		{
			ereport(NOTICE,
					(errmsg("assertion \"%s\" does not exist, skipping",
							NameListToString(name))));
			continue;
		}

		tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(assertionOid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for constraint %u",
				 assertionOid);
		con = (Form_pg_constraint) GETSTRUCT(tuple);

		/* Permission check: must own constraint or its namespace */
		if (!pg_constraint_ownercheck(assertionOid, GetUserId()) &&
			!pg_namespace_ownercheck(con->connamespace, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CONSTRAINT,
						   NameStr(con->conname));

		object.classId = ConstraintRelationId;
		object.objectId = assertionOid;
		object.objectSubId = 0;

		add_exact_object_address(&object, objects);

		ReleaseSysCache(tuple);
	}

	performMultipleDeletions(objects, drop->behavior, 0);

	free_object_addresses(objects);
}


Oid
RenameAssertion(List *name, const char *newname)
{
	Oid			assertionOid;
	Relation	rel;
	HeapTuple	tuple;
	Form_pg_constraint con;
	AclResult	aclresult;

	rel = heap_open(ConstraintRelationId, RowExclusiveLock);

	assertionOid = get_assertion_oid(name, false);

	tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(assertionOid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for constraint %u",
			 assertionOid);
	con = (Form_pg_constraint) GETSTRUCT(tuple);

	/* must be owner */
	if (!pg_constraint_ownercheck(assertionOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CONVERSION,
					   NameListToString(name));

	/* must have CREATE privilege on namespace */
	aclresult = pg_namespace_aclcheck(con->connamespace, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(con->connamespace));

	ReleaseSysCache(tuple);

	RenameConstraintById(assertionOid, newname);

	heap_close(rel, NoLock);

	return assertionOid;
}
