/*-------------------------------------------------------------------------
 *
 * ddl_rewrite.c
 *	  Functions to convert a utility command parsetree back to a command string
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/ddl_rewrite.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/heap.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/analyze.h"
#include "utils/builtins.h"
#include "utils/ddl_rewrite.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/*
 * Given a RangeVar, return the namespace name to use as schemaname. When
 * r->schemaname is NULL, returns the first schema name of the current
 * search_path.
 *
 * It can be so that the resulting object (schema.name) does not exists, that
 * check didn't happen yet when at the "ddl_command_start" event. All we ca do
 * is play by the system's rule. The other option would be to capture and
 * expose the search_path to the event trigger functions. Then any trigger
 * function would have to duplicate the code here to extract the first schema
 * in the search_path anyway.
 */
static char *
RangeVarGetNamespace(RangeVar *r)
{
	char       *schemaname;
	List	   *search_path;

	if (r->schemaname)
		return r->schemaname;

	search_path = fetch_search_path(false);
	if (search_path == NIL) /* probably can't happen */
		schemaname = NULL;
	else
		schemaname = get_namespace_name(linitial_oid(search_path));

	list_free(search_path);

	return schemaname;
}

static char *
RangeVarToString(RangeVar *r)
{
	char       *schemaname = RangeVarGetNamespace(r);
	StringInfoData string;
	initStringInfo(&string);

	if (r->catalogname != NULL)
	{
		appendStringInfoString(&string, quote_identifier(r->catalogname));
		appendStringInfoChar(&string, '.');
	}
	if (schemaname != NULL)
	{
		appendStringInfoString(&string, quote_identifier(schemaname));
		appendStringInfoChar(&string, '.');
	}
	appendStringInfoString(&string, quote_identifier(r->relname));

	return string.data;
}

static const char *
objTypeToString(ObjectType objtype)
{
	switch (objtype)
	{
		case OBJECT_AGGREGATE:
			return "AGGREGATE";
		case OBJECT_ATTRIBUTE:
			return "ATTRIBUTE";
		case OBJECT_CAST:
			return "CAST";
		case OBJECT_COLUMN:
			return "COLUMN";
		case OBJECT_CONSTRAINT:
			return "CONSTRAINT";
		case OBJECT_COLLATION:
			return "COLLATION";
		case OBJECT_CONVERSION:
			return "CONVERSION";
		case OBJECT_DATABASE:
			return "DATABASE";
		case OBJECT_DOMAIN:
			return "DOMAIN";
		case OBJECT_EVENT_TRIGGER:
			return "EVENT TRIGGER";
		case OBJECT_EXTENSION:
			return "EXTENSION";
		case OBJECT_FDW:
			return "FDW";
		case OBJECT_FOREIGN_SERVER:
			return "FOREIGN SERVER";
		case OBJECT_FOREIGN_TABLE:
			return "FOREIGN TABLE";
		case OBJECT_FUNCTION:
			return "FUNCTION";
		case OBJECT_INDEX:
			return "INDEX";
		case OBJECT_LANGUAGE:
			return "LANGUAGE";
		case OBJECT_LARGEOBJECT:
			return "LARGE OBJECT";
		case OBJECT_MATVIEW:
			return "MATERIALIZED VIEW";
		case OBJECT_OPCLASS:
			return "OPERATOR CLASS";
		case OBJECT_OPERATOR:
			return "OPERATOR";
		case OBJECT_OPFAMILY:
			return "OPERATOR FAMILY";
		case OBJECT_ROLE:
			return "ROLE";
		case OBJECT_RULE:
			return "RULE";
		case OBJECT_SCHEMA:
			return "SCHEMA";
		case OBJECT_SEQUENCE:
			return "SEQUENCE";
		case OBJECT_TABLE:
			return "TABLE";
		case OBJECT_TABLESPACE:
			return "TABLESPACE";
		case OBJECT_TRIGGER:
			return "TRIGGER";
		case OBJECT_TSCONFIGURATION:
			return "TEXT SEARCH CONFIGURATION";
		case OBJECT_TSDICTIONARY:
			return "TEXT SEARCH DICTIONARY";
		case OBJECT_TSPARSER:
			return "TEXT SEARCH PARSER";
		case OBJECT_TSTEMPLATE:
			return "TEXT SEARCH TEMPLATE";
		case OBJECT_TYPE:
			return "TYPE";
		case OBJECT_VIEW:
			return "VIEW";
		default:
			elog(ERROR, "unrecognized object type: %d", objtype);
			return NULL;		/* silence compiler */
	}
}

static void
maybeAddSeparator(StringInfo buf, const char *sep, bool *first)
{
	if (*first)
		*first = false;
	else        appendStringInfoString(buf, sep);
}

/*
 * Given a raw expression used for a DEFAULT or a CHECK constraint, turn it
 * back into source code.
 *
 * Note we don't apply sanity checks such as the return type of the expression.
 */
static char *
uncookConstraintOrDefault(Node *raw_expr, ParseExprKind kind,
						  RangeVar *relation, Oid relOid)
{
	Node    *expr;
	char	*src;
	List	*dpcontext = NULL;
	ParseState *pstate;
	MemoryContext cxt;
	MemoryContext oldcxt;

	cxt = AllocSetContextCreate(CurrentMemoryContext,
								"uncook context",
								ALLOCSET_SMALL_MINSIZE,
								ALLOCSET_SMALL_INITSIZE,
								ALLOCSET_SMALL_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(cxt);

	pstate = make_parsestate(NULL);
	if (relation)
	{
		RangeTblEntry *rte;

		rte = addRangeTableEntry(pstate, relation, NULL, false, true);
		addRTEtoQuery(pstate, rte, true, true, true);
		dpcontext = deparse_context_for(relation->relname, relOid);
	}

	/*
	 * Transform raw parsetree to executable expression.
	 */
	expr = transformExpr(pstate, raw_expr, kind);

	/*
	 * Take care of collations.
	 */
	assign_expr_collations(pstate, expr);

	src = deparse_expression(expr, dpcontext, false, false);

	free_parsestate(pstate);

	MemoryContextSwitchTo(oldcxt);

	/* copy to destination context before destroying our own */
	/* XXX maybe we should switch just before deparse_expression? */
	src = pstrdup(src);
	MemoryContextDelete(cxt);

	return src;
}

/*
 * rewrite any_name parser production
 */
static void
_rwAnyName(StringInfo buf, List *name)
{
	bool first = true;
	ListCell *lc;

	foreach(lc, name)
	{
		Value *member = (Value *) lfirst(lc);

		maybeAddSeparator(buf, ".", &first);
		appendStringInfo(buf, "%s", member->val.str);
	}
}

static char *
_rwCreateExtensionStmt(Oid objectId, const char *identity, Node *parsetree)
{
	CreateExtensionStmt *node = (CreateExtensionStmt *) parsetree;
	StringInfoData buf;
	ListCell   *lc;

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE EXTENSION%s %s",
					 node->if_not_exists ? " IF NOT EXISTS" : "",
					 identity);

	foreach(lc, node->options)
	{
		DefElem    *defel = (DefElem *) lfirst(lc);

		if (strcmp(defel->defname, "schema") == 0)
			appendStringInfo(&buf, " SCHEMA %s", strVal(defel->arg));

		else if (strcmp(defel->defname, "new_version") == 0)
			appendStringInfo(&buf, " VERSION %s", strVal(defel->arg));

		else if (strcmp(defel->defname, "old_version") == 0)
			appendStringInfo(&buf, " FROM %s", strVal(defel->arg));
	}

	return buf.data;
}

static char *
_rwViewStmt(Oid objectId, const char *identity, Node *parsetree)
{
	ViewStmt *node = (ViewStmt *) parsetree;
	StringInfoData buf;
	Query	   *viewParse;
	char	   *viewsrc;

	initStringInfo(&buf);
	viewParse = parse_analyze(node->query,
							  "(unavailable source text)", NULL, 0);

	appendStringInfo(&buf, "CREATE %sVIEW %s AS ",
					 node->replace? "OR REPLACE": "",
					 identity);

	viewsrc = pg_get_viewstmt_definition(viewParse);
	appendStringInfoString(&buf, viewsrc);
	pfree(viewsrc);

	return buf.data;
}

/*
 * Rewrite a OptTableSpace: grammar production
 */
static void
_rwOptTableSpace(StringInfo buf, const char *name)
{
	if (name != NULL)
		appendStringInfo(buf, " TABLESPACE %s", name);
}

/*
 * Rewrite a OptConsTableSpace: grammar production
 */
static void
_rwOptConsTableSpace(StringInfo buf, const char *name)
{
	if (name != NULL)
		appendStringInfo(buf, " USING INDEX TABLESPACE %s", name);
}

/*
 * Rewrite a generic def_arg: grammar production
 */
static void
_rwDefArg(StringInfo buf, Node *arg)
{
	appendStringInfoChar(buf, '?');
}

/*
 * Rewrite a generic definition grammar production
 */
static void
_rwDefinition(StringInfo buf, List *definitions)
{
	/* FIXME: needs an option to print () when empty? */
	if (definitions != NULL)
	{
		ListCell *k;
		bool first = true;

		appendStringInfoChar(buf, '(');
		foreach(k, definitions)
		{
			DefElem *def = (DefElem *) lfirst(k);

			elog(LOG, "dumping definition element: %s",
				 nodeToString(def));

			maybeAddSeparator(buf, ", ", &first);

			/* ColLabel */
			appendStringInfo(buf, "%s", def->defname);
			/* [ '=' def_arg ] */
			if (def->arg)
			{
				appendStringInfoChar(buf, '=');
				_rwDefArg(buf, def->arg);
			}

		}
		appendStringInfoChar(buf, ')');
	}
}

/*
 * Rewrite the opt_column_list: grammar production
 */
static void
_rwOptColumnList(StringInfo buf, List *clist)
{
	if (clist != NIL)
	{
		ListCell *c;
		bool first = true;

		appendStringInfoChar(buf, '(');
		foreach(c, clist)
		{
			maybeAddSeparator(buf, ",", &first);
			appendStringInfo(buf, "%s", strVal(lfirst(c)));
		}
		appendStringInfoChar(buf, ')');
	}
}

/*
 * Rewrite the opt_column_list: grammar production
 */
static void
_rwColumnList(StringInfo buf, List *clist)
{
	if (clist == NIL)
		appendStringInfo(buf, "()");
	else
		_rwOptColumnList(buf, clist);
}

/*
 * Rewrite the key_match: grammar production
 */
static void
_rwKeyMatch(StringInfo buf, int matchtype)
{
	switch (matchtype)
	{
		case FKCONSTR_MATCH_FULL:
			appendStringInfo(buf, " MATCH FULL");
			break;

		case FKCONSTR_MATCH_PARTIAL:
			/* should not happen, not yet implemented */
			appendStringInfo(buf, " MATCH PARTIAL");
			break;

		case FKCONSTR_MATCH_SIMPLE:
		default:
			appendStringInfo(buf, " MATCH SIMPLE");
			break;
	}
}

/*
 * Rewrite the key_action: grammar production
 */
static void
_rwKeyAction(StringInfo buf, int action)
{
	switch (action)
	{
		case FKCONSTR_ACTION_NOACTION:
			appendStringInfo(buf, " NO ACTION");
			break;

		case FKCONSTR_ACTION_RESTRICT:
			appendStringInfo(buf, " RESTRICT");
			break;

		case FKCONSTR_ACTION_CASCADE:
			appendStringInfo(buf, " CASCADE");
			break;

		case FKCONSTR_ACTION_SETNULL:
			appendStringInfo(buf, " SET NULL");
			break;

		case FKCONSTR_ACTION_SETDEFAULT:
			appendStringInfo(buf, " SET DEFAULT");
			break;

		default:
			elog(ERROR, "unexpected foreign key action: %d", action);
			break;
	}
}

static void
_rwKeyActions(StringInfo buf, int upd_action, int del_action)
{
	appendStringInfo(buf, " ON UPDATE");
	_rwKeyAction(buf, upd_action);

	appendStringInfo(buf, " ON DELETE");
	_rwKeyAction(buf, del_action);
}

/*
 * Rewrite the ConstraintAttributeSpec parser production
 */
static void
_rwConstraintAttributeSpec(StringInfo buf,
						   bool deferrable, bool initdeferred)
{
	if (deferrable)
		appendStringInfo(buf, " DEFERRABLE");
	else
		appendStringInfo(buf, " NOT DEFERRABLE");

	if (initdeferred)
		appendStringInfo(buf, " INITIALLY DEFERRED");
	else
		appendStringInfo(buf, " INITIALLY IMMEDIATE");
}

static void
_rwRelPersistence(StringInfo buf, int relpersistence)
{
	switch (relpersistence)
	{
		case RELPERSISTENCE_TEMP:
			appendStringInfo(buf, " TEMPORARY");
			break;

		case RELPERSISTENCE_UNLOGGED:
			appendStringInfo(buf, " UNLOGGED");
			break;

		case RELPERSISTENCE_PERMANENT:
		default:
			break;
	}
}

/*
 * rewrite the ColConstraintElem grammar production
 *
 * Not all constraint types can be expected here, and some of them can be found
 * with other grammars as table level constraint attributes.
 */
static void
_rwColConstraintElem(StringInfo buf, Oid objectId, List *constraints,
					 RangeVar *relation)
{
	ListCell   *lc;

	foreach(lc, constraints)
	{
		Constraint *c = (Constraint *) lfirst(lc);
		Assert(IsA(c, Constraint));

		if (c->conname != NULL)
			appendStringInfo(buf, " CONSTRAINT %s", c->conname);

		switch (c->contype)
		{
			case CONSTR_NOTNULL:
				appendStringInfo(buf, " NOT NULL");
				break;

			case CONSTR_NULL:
				appendStringInfo(buf, " NULL");
				break;

			case CONSTR_UNIQUE:
				appendStringInfo(buf, " UNIQUE");
				_rwOptConsTableSpace(buf, c->indexspace);
				break;

			case CONSTR_PRIMARY:
				appendStringInfo(buf, " PRIMARY KEY");
				_rwDefinition(buf, c->options);
				_rwOptConsTableSpace(buf, c->indexspace);
				break;

			case CONSTR_CHECK:
				{
					char *src;

					src = uncookConstraintOrDefault(c->raw_expr,
											  EXPR_KIND_CHECK_CONSTRAINT,
											  relation,
											  objectId);
					appendStringInfo(buf, " CHECK (%s)", src);
					pfree(src);
					break;
				}

			case CONSTR_DEFAULT:
			{
				if (c->cooked_expr)
				{
					List		*dpcontext;
					Node		*expr = (Node *) stringToNode(c->cooked_expr);
					char		*consrc;

					dpcontext = deparse_context_for(relation->relname,
													objectId);
					consrc = deparse_expression(expr, dpcontext, false, false);

					appendStringInfo(buf, " DEFAULT %s", consrc);
				}
				else if (c->raw_expr)
				{
					char   *src;

					/* deparse the default expression */
					src = uncookConstraintOrDefault(c->raw_expr,
													EXPR_KIND_COLUMN_DEFAULT,
													relation, objectId);

					appendStringInfo(buf, " DEFAULT %s", src);
				}
				break;
			}

			case CONSTR_FOREIGN:
			{
				appendStringInfo(buf, " REFERENCES %s ",
								 RangeVarToString(c->pktable));
				_rwOptColumnList(buf, c->pk_attrs);
				_rwKeyMatch(buf, c->fk_matchtype);
				_rwKeyActions(buf, c->fk_upd_action, c->fk_del_action);
				break;
			}

			default:
				/* unexpected case, WARNING? */
				elog(WARNING, "constraint %d is not a column constraint",
					 c->contype);
				break;
		}
	}
}

/*
 * rewrite a list of TableConstraint: grammar production
 */
static void
_rwTableConstraint(StringInfo buf, Oid objectId, List *constraints,
				   RangeVar *relation)
{
	ListCell   *lc;
	List       *context = NIL;

	foreach(lc, constraints)
	{
		Constraint *c = (Constraint *) lfirst(lc);
		Assert(IsA(c, Constraint));

		if (c->conname != NULL)
			appendStringInfo(buf, " CONSTRAINT %s", c->conname);

		switch (c->contype)
		{
			case CONSTR_CHECK:
				{
					char *src;

					src = uncookConstraintOrDefault(c->raw_expr,
													EXPR_KIND_CHECK_CONSTRAINT,
													relation,
											  objectId);
					appendStringInfo(buf, " CHECK (%s)", src);
					pfree(src);
				}
				break;

			case CONSTR_UNIQUE:
				appendStringInfo(buf, " UNIQUE");

				if (c->keys)
				{
					/* unique (column, list) */
					_rwColumnList(buf, c->keys);
					_rwDefinition(buf, c->options);
					_rwConstraintAttributeSpec(buf,
											   c->deferrable, c->initdeferred);
					_rwOptConsTableSpace(buf, c->indexspace);
				}
				else
				{
					/* unique using index */
					appendStringInfo(buf, " USING INDEX %s", c->indexname);
					_rwConstraintAttributeSpec(buf,
											   c->deferrable, c->initdeferred);
				}
				break;

			case CONSTR_PRIMARY:
				appendStringInfo(buf, " PRIMARY KEY");

				if (c->keys)
				{
					/* primary key (column, list) */
					_rwColumnList(buf, c->keys);
					_rwDefinition(buf, c->options);
					_rwOptConsTableSpace(buf, c->indexspace);
					_rwConstraintAttributeSpec(buf,
											   c->deferrable, c->initdeferred);
				}
				else
				{
					/* primary key using index */
					appendStringInfo(buf, " USING INDEX %s", c->indexname);
					_rwConstraintAttributeSpec(buf,
											   c->deferrable, c->initdeferred);
				}
				break;

			case CONSTR_EXCLUSION:
				appendStringInfo(buf, " EXCLUDE %s ", c->access_method);

				if (c->exclusions == NULL)
					appendStringInfo(buf, "()");
				else
				{
					/* ExclusionConstraintList */
					ListCell *e;
					bool first = true;

					appendStringInfoChar(buf, '(');
					foreach(e, c->exclusions)
					{
						List *ec = (List *)lfirst(e);

						maybeAddSeparator(buf, ",", &first);

						/* ExclustionConstraintElem */
						appendStringInfo(buf, "%s WITH OPERATOR(%s)",
										 strVal(linitial(ec)),
										 strVal(lsecond(ec)));
					}
					appendStringInfoChar(buf, ')');
				}
				_rwDefinition(buf, c->options);
				_rwOptConsTableSpace(buf, c->indexspace);

				/* ExclusionWhereClause: */
				if (c->where_clause)
				{
					char *str;

					if (context == NIL)
						context = deparse_context_for(relation->relname,
													  objectId);

					str = deparse_expression(c->where_clause, context,
											 false, false);

					appendStringInfo(buf, " WHERE (%s)", str);
				}

				_rwConstraintAttributeSpec(buf,
										   c->deferrable, c->initdeferred);
				break;

			case CONSTR_FOREIGN:
				appendStringInfoString(buf, " FOREIGN KEY");

				_rwColumnList(buf, c->fk_attrs);

				appendStringInfo(buf, " REFERENCES %s",
								 RangeVarToString(c->pktable));

				_rwOptColumnList(buf, c->pk_attrs);

				_rwKeyMatch(buf, c->fk_matchtype);
				_rwKeyActions(buf, c->fk_upd_action, c->fk_del_action);

				_rwConstraintAttributeSpec(buf,
										   c->deferrable, c->initdeferred);

				if (c->skip_validation)
					appendStringInfoString(buf, " NOT VALID");
				break;

			default:
				/* unexpected case, WARNING? */
				elog(WARNING, "constraint %d is not a column constraint",
					 c->contype);
				break;
		}
	}
}

/*
 * rewrite TableLikeOptionList parser production
 */
static void
_rwTableLikeOptionList(StringInfo buf, bits32 options)
{
	if (options == CREATE_TABLE_LIKE_ALL)
		appendStringInfo(buf, " INCLUDING ALL");
	else
	{
		if (options & CREATE_TABLE_LIKE_DEFAULTS)
			appendStringInfo(buf, " INCLUDING DEFAULTS");

		if (options & CREATE_TABLE_LIKE_CONSTRAINTS)
			appendStringInfo(buf, " INCLUDING CONSTRAINTS");

		if (options & CREATE_TABLE_LIKE_INDEXES)
			appendStringInfo(buf, " INCLUDING INDEXES");

		if (options & CREATE_TABLE_LIKE_STORAGE)
			appendStringInfo(buf, " INCLUDING STORAGE");

		if (options & CREATE_TABLE_LIKE_COMMENTS)
			appendStringInfo(buf, " INCLUDING COMMENTS");
	}
}

/*
 * rewrite OptTableElementList parser production
 */
static void
_rwOptTableElementList(StringInfo buf, Oid objectId, List *tableElts,
					   RangeVar *relation)
{
	bool        first = true;
	ListCell   *e;

	appendStringInfoChar(buf, '(');

	foreach(e, tableElts)
	{
		Node *elt = (Node *) lfirst(e);

		maybeAddSeparator(buf, ", ", &first);

		switch (nodeTag(elt))
		{
			case T_ColumnDef:
			{
				ColumnDef  *c = (ColumnDef *) elt;

				appendStringInfo(buf, "%s %s",
								 c->colname,
								 TypeNameToString(c->typeName));
				/* XXX this might need conditional supression in some cases */
				_rwColConstraintElem(buf, objectId, c->constraints, relation);
				break;
			}
			case T_TableLikeClause:
			{
				TableLikeClause *like = (TableLikeClause *) elt;
				appendStringInfo(buf, "LIKE %s",
								 RangeVarToString(like->relation));
				_rwTableLikeOptionList(buf, like->options);
				break;
			}
			case T_Constraint:
			{
				Constraint  *c = (Constraint *) elt;
				_rwTableConstraint(buf, objectId, list_make1(c), relation);
				break;
			}
			default:
				break;
		}
	}
	appendStringInfoChar(buf, ')');
}

/*
 * rewrite OptTypedTableElementList parser production
 */
static void
_rwOptTypedTableElementList(StringInfo buf, Oid objectId, List *tableElts,
							RangeVar *relation)
{
	bool        first = true,
				parens = false;
	ListCell   *e;

	foreach(e, tableElts)
	{
		Node *elt = (Node *) lfirst(e);

		switch (nodeTag(elt))
		{
			case T_ColumnDef:
				{
					ColumnDef  *c = (ColumnDef *) elt;

					if (c->constraints)
					{
						maybeAddSeparator(buf, ",", &first);

						/* only add parens if we have columns with options */
						if (!parens)
						{
							appendStringInfoChar(buf, '(');
							parens = true;
						}
						appendStringInfo(buf, " %s WITH OPTIONS", c->colname);
						_rwColConstraintElem(buf, objectId, c->constraints,
											 relation);
					}
					break;
				}
			case T_Constraint:
				{
					Constraint  *c = (Constraint *) elt;

					_rwTableConstraint(buf, objectId, list_make1(c), relation);
					break;
				}
			default:
				break;
		}
	}

	if (parens)
		appendStringInfoChar(buf, ')');
}

/*
 * rewrite OptInherit parser production
 */
static void
_rwOptInherit(StringInfo buf, List *inhRelations)
{
	if (inhRelations)
	{
		ListCell *lc;
		bool first = true;

		appendStringInfo(buf, " INHERITS (");
		foreach(lc, inhRelations)
		{
			RangeVar   *inh = (RangeVar *) lfirst(lc);

			maybeAddSeparator(buf, ",", &first);
			appendStringInfo(buf, "%s", RangeVarToString(inh));
		}
		appendStringInfoChar(buf, ')');
	}
}

/*
 * rewrite reloptions parser production
 */
static void
_rwRelOptions(StringInfo buf, List *options, bool null_is_true)
{
	bool        first = true;
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const char *value;

		maybeAddSeparator(buf, ", ", &first);

		if (def->arg != NULL)
		{
			/* ... defname = value */
			value = defGetString(def);
			appendStringInfo(buf, "%s=%s", def->defname, value);
		}
		else
		{
			if (null_is_true)
			{
				/* ... defname = true */
				appendStringInfo(buf, "%s=true", def->defname);
			}
			else
			{
				/* ... defname */
				appendStringInfo(buf, "%s", def->defname);
			}
		}
	}
}

/*
 * rewrite OptWith parser production
 */
static void
_rwOptWith(StringInfo buf, List *options)
{
	if (options)
	{
		appendStringInfoString(buf, " WITH (");
		_rwRelOptions(buf, options, true);
		appendStringInfoChar(buf, ')');
	}
}

/*
 * rewrite OptCommitOption parser production
 */
static void
_rwOnCommitOption(StringInfo buf, int oncommit)
{
	switch (oncommit)
	{
		case ONCOMMIT_DROP:
			appendStringInfo(buf, " ON COMMIT DROP");
			break;

		case ONCOMMIT_DELETE_ROWS:
			appendStringInfo(buf, " ON COMMIT DELETE ROWS");
			break;

		case ONCOMMIT_PRESERVE_ROWS:
			appendStringInfo(buf, " ON COMMIT PRESERVE ROWS");
			break;

		case ONCOMMIT_NOOP:
			/* EMPTY */
			break;

	}
}

/*
 * rewrite CreateStmt parser production
 */
static char *
_rwCreateStmt(Oid objectId, const char *identity, Node *parsetree)
{
	CreateStmt *node = (CreateStmt *) parsetree;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "CREATE");
	_rwRelPersistence(&buf, node->relation->relpersistence);
	appendStringInfo(&buf, " TABLE %s %s", identity,
					 node->if_not_exists ? " IF NOT EXISTS" : "");

	if (node->ofTypename)
	{
		appendStringInfo(&buf, "OF %s",
						 TypeNameToString(node->ofTypename));
		_rwOptTypedTableElementList(&buf, objectId, node->tableElts,
									node->relation);
	}
	else
	{
		List *elts = list_concat(node->tableElts, node->constraints);

		_rwOptTableElementList(&buf, objectId, elts, node->relation);
		_rwOptInherit(&buf, node->inhRelations);
	}

	_rwOptWith(&buf, node->options);
	_rwOnCommitOption(&buf, node->oncommit);
	_rwOptTableSpace(&buf, node->tablespacename);

	return buf.data;
}

/*
 * rewrite OptSeqOptList parser production
 */
static void
_rwOptSeqOptList(StringInfo buf, List *options)
{
	ListCell *opt;

	foreach(opt, options)
	{
		DefElem    *defel = (DefElem *) lfirst(opt);

		if (strcmp(defel->defname, "cache") == 0)
		{
			char		num[100];

			snprintf(num, sizeof(num), INT64_FORMAT, defGetInt64(defel));
			appendStringInfo(buf, " CACHE %s", num);
		}
		else if (strcmp(defel->defname, "cycle") == 0)
		{
			if (intVal(defel))
				appendStringInfo(buf, " CYCLE");
			else
				appendStringInfo(buf, " NO CYCLE");
		}
		else if (strcmp(defel->defname, "increment") == 0)
		{
			char		num[100];

			snprintf(num, sizeof(num), INT64_FORMAT, defGetInt64(defel));
			appendStringInfo(buf, " INCREMENT BY %s", num);
		}
		else if (strcmp(defel->defname, "maxvalue") == 0)
		{
			if (defel->arg)
			{
				char		num[100];

				snprintf(num, sizeof(num), INT64_FORMAT, defGetInt64(defel));
				appendStringInfo(buf, " MAXVALUE %s", num);
			}
			else
				appendStringInfo(buf, " NO MAXVALUE");
		}
		else if (strcmp(defel->defname, "minvalue") == 0)
		{
			if (defel->arg)
			{
				char		num[100];

				snprintf(num, sizeof(num), INT64_FORMAT, defGetInt64(defel));
				appendStringInfo(buf, " MINVALUE %s", num);
			}
			else
				appendStringInfo(buf, " NO MINVALUE");
		}
		else if (strcmp(defel->defname, "owned_by") == 0)
		{
			List       *owned_by = defGetQualifiedName(defel);
			int         nnames = list_length(owned_by);
			List	   *relname;
			char	   *attrname;
			RangeVar   *rel;

			relname = list_truncate(list_copy(owned_by), nnames - 1);
			attrname = strVal(lfirst(list_tail(owned_by)));
			rel = makeRangeVarFromNameList(relname);

			appendStringInfo(buf, " OWNED BY %s.%s",
							 RangeVarToString(rel), attrname);
		}
		else if (strcmp(defel->defname, "start") == 0)
		{
			char		num[100];

			snprintf(num, sizeof(num), INT64_FORMAT, defGetInt64(defel));
			appendStringInfo(buf, " START WITH %s", num);
		}
		else if (strcmp(defel->defname, "restart") == 0)
		{
			if (defel->arg)
			{
				char		num[100];

				snprintf(num, sizeof(num), INT64_FORMAT, defGetInt64(defel));
				appendStringInfo(buf, " RESTART WITH %s", num);
			}
			else
				appendStringInfo(buf, " RESTART");
		}
	}
}

/*
 * rewrite CreateSeqStmt parser production
 */
static char *
_rwCreateSeqStmt(Oid objectId, const char *identity, Node *parsetree)
{
	CreateSeqStmt *node = (CreateSeqStmt *) parsetree;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE");
	_rwRelPersistence(&buf, node->sequence->relpersistence);
	appendStringInfo(&buf, " SEQUENCE %s ", identity);
	_rwOptSeqOptList(&buf, node->options);

	return buf.data;
}

/*
 * rewrite index_elem parser production
 */
static void
_rwIndexElem(StringInfo buf, IndexElem *e, List *context)
{
	if (e->name)
		appendStringInfo(buf, "%s", e->name);
	else
	{
		char *str;

		str = deparse_expression(e->expr, context, false, false);

		/* Need parens if it's not a bare function call */
		if (IsA(e->expr, FuncExpr) &&
			((FuncExpr *) e->expr)->funcformat == COERCE_EXPLICIT_CALL)
			appendStringInfo(buf, "%s", str);
		else
			appendStringInfo(buf, "(%s)", str);
	}

	if (e->collation)
	{
		appendStringInfo(buf, " COLLATE");
		_rwAnyName(buf, e->collation);
	}

	if (e->opclass)
	{
		appendStringInfo(buf, " USING ");
		_rwAnyName(buf, e->opclass);
	}

	/* defensive coding, so that the compiler hints us into updating those bits
	 * if needs be */
	switch (e->ordering)
	{
		/* using unexpected in create index */
		case SORTBY_DEFAULT:
		case SORTBY_USING:
			break;

		case SORTBY_ASC:
			appendStringInfo(buf, " ASC");
			break;

		case SORTBY_DESC:
			appendStringInfo(buf, " DESC");
			break;
	}
	switch (e->nulls_ordering)
	{
		case SORTBY_NULLS_DEFAULT:
			break;

		case SORTBY_NULLS_FIRST:
			appendStringInfo(buf, " NULLS FIRST");
			break;

		case SORTBY_NULLS_LAST:
			appendStringInfo(buf, " NULLS LAST");
			break;
	}
}

/*
 * rewrite IndexStmt parser production
 */
static char *
_rwCreateIndexStmt(Oid objectId, const char *identity, Node *parsetree)
{
	IndexStmt			*node  = (IndexStmt *) parsetree;
	StringInfoData		 buf;
	Oid					 relId;
	bool				 first = true;
	ListCell			*lc;
	List				*context;

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE%s INDEX", node->unique ? " UNIQUE" : "");

	if (node->concurrent)
		appendStringInfo(&buf, " CONCURRENTLY");

	appendStringInfo(&buf, " %s ON %s USING %s (",
					 identity,
					 RangeVarToString(node->relation),
					 node->accessMethod);

	relId = IndexGetRelation(objectId, false);
	context = deparse_context_for("", relId);	/* alias is not important */

	foreach(lc, node->indexParams)
	{
		IndexElem *e = (IndexElem *) lfirst(lc);

		maybeAddSeparator(&buf, ", ", &first);
		_rwIndexElem(&buf, e, context);
	}
	appendStringInfoChar(&buf, ')');

	_rwOptWith(&buf, node->options);
	_rwOptConsTableSpace(&buf, node->tableSpace);

	if (node->whereClause)
	{
		Node *transformed;
		ParseState *pstate;
		char *str;
		RangeTblEntry *rte;

		pstate = make_parsestate(NULL);
		rte = addRangeTableEntry(pstate, node->relation,
								 NULL, false, true);
		addRTEtoQuery(pstate, rte, true, true, true);

		transformed = transformExpr(pstate, node->whereClause,
									EXPR_KIND_INDEX_PREDICATE);

		str = deparse_expression(transformed, context, false, false);
		/* deparse_expression adds outer parens */
		appendStringInfo(&buf, " WHERE %s", str);
	}


	return buf.data;
}

/*
 * rewrite func_arg parser production
 */
static void
_rwFuncArg(StringInfo buf, FunctionParameter *fp)
{
	/* Parameter's mode */
	switch (fp->mode)
	{
		case FUNC_PARAM_INOUT:
			appendStringInfoString(buf, "IN OUT");
			break;

		case FUNC_PARAM_IN:
			appendStringInfoString(buf, "IN");
			break;

		case FUNC_PARAM_OUT:
			appendStringInfoString(buf, "OUT");
			break;

		case FUNC_PARAM_VARIADIC:
			appendStringInfoString(buf, "VARIADIC");
			break;

		case FUNC_PARAM_TABLE:
			elog(ERROR, "FUNC_PARAM_TABLE not expected in argument list");
			break;
	}

	/* Parameter's name is optional */
	if (fp->name)
		appendStringInfo(buf, " %s", fp->name);

	/* Parameter's type name is not */
	appendStringInfo(buf, " %s", TypeNameToString(fp->argType));

	if (fp->defexpr)
	{
		char *src;

		src = uncookConstraintOrDefault(fp->defexpr,
										EXPR_KIND_FUNCTION_DEFAULT,
										NULL, InvalidOid);

		appendStringInfo(buf, " DEFAULT %s", src);
		pfree(src);
	}
}

/*
 * rewrite CreateFunctionStmt parser production
 */
static char *
_rwCreateFunctionStmt(Oid objectId, const char *identity, Node *parsetree)
{
	CreateFunctionStmt *node  = (CreateFunctionStmt *) parsetree;
	StringInfoData	buf;
	HeapTuple		funcTup;
	Form_pg_proc	func;
	char		   *schema;
	char		   *qualifiedName;

#if 0
	char				*fname, *nspname;
	Oid					 namespaceId, languageOid;
	ListCell			*x;
	Oid					 prorettype;
	bool				 returnsSet;
	List				*as_clause;
	char				*language;
	bool				 isWindowFunc, isStrict, security, isLeakProof;
	char				 volatility = PROVOLATILE_VOLATILE;
	ArrayType			*proconfig;
	float4				 procost = -1;
	float4				 prorows = -1;
	char                *procost_str, *prorows_str;
	char				*probin_str;
	char				*prosrc_str;
	HeapTuple			 languageTuple;
#endif

	initStringInfo(&buf);

	/*
	 * We cannot use the passed identity here: it includes only IN parameters,
	 * but that's not sufficient to write a CREATE FUNCTION command.  So
	 * fetch the name on our own.
	 */
	funcTup = SearchSysCache1(PROCOID, objectId);
	func = (Form_pg_proc) GETSTRUCT(funcTup);

	schema = get_namespace_name(func->pronamespace);
	qualifiedName = quote_qualified_identifier(schema,
											   NameStr(func->proname));
	appendStringInfo(&buf, "CREATE FUNCTION %s ",
					 qualifiedName);
	pfree(schema);
	pfree(qualifiedName);

	/* Emit complete parameter list */
	if (node->parameters)
	{
		ListCell   *lc;
		bool		first = true;

		appendStringInfoChar(&buf, '(');

		foreach(lc, node->parameters)
		{
			FunctionParameter *fp = (FunctionParameter *) lfirst(lc);

			maybeAddSeparator(&buf, ", ", &first);
			/* direction, name, type, default */
			_rwFuncArg(&buf, fp);
		}
		appendStringInfoChar(&buf, ')');
	}

#if 0

	/* return type */
	compute_return_type(node->returnType, languageOid, &prorettype, &returnsSet);
	appendStringInfo(&buf, " RETURNS %s", TypeNameToString(node->returnType));

	/* get options and attributes */
	compute_attributes_sql_style(node->options,
								 &as_clause, &language,
								 &isWindowFunc, &volatility,
								 &isStrict, &security, &isLeakProof,
								 &proconfig, &procost, &prorows);

	languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));
	languageOid =  HeapTupleGetOid(languageTuple);
	ReleaseSysCache(languageTuple);

	/* also get the function's body */
	interpret_AS_clause(languageOid, language, fname, as_clause,
						&prosrc_str, &probin_str,
						returnsSet, &procost, &prorows);

	/* language */
	appendStringInfo(&buf, " LANGUAGE %s", language);

	/* options */
	if (isWindowFunc)
		appendStringInfo(&buf, " WINDOW");

	switch (volatility)
	{
		case PROVOLATILE_IMMUTABLE:
			appendStringInfo(&buf, " IMMUTABLE");
			break;
		case PROVOLATILE_STABLE:
			appendStringInfo(&buf, " STABLE");
			break;
		case PROVOLATILE_VOLATILE:
			appendStringInfo(&buf, " VOLATILE");
			break;
	}
	appendStringInfo(&buf, " %sLEAKPROOF", isLeakProof ? "" : "NOT ");

	if (isStrict)
		appendStringInfo(&buf, " RETURNS NULL ON NULL INPUT");
	else
		appendStringInfo(&buf, " CALLED ON NULL INPUT");

	/* friendly output for cost and rows */
	procost_str = DatumGetCString(
		DirectFunctionCall1(float4out, Float4GetDatum(procost)));
	prorows_str = DatumGetCString(
		DirectFunctionCall1(float4out, Float4GetDatum(prorows)));

	appendStringInfo(&buf, " COST %s", procost_str);
	appendStringInfo(&buf, " ROWS %s", prorows_str);

	/* body */
	appendStringInfo(&buf, " AS $%s$ %s $%s$;", fname, prosrc_str, fname);
#endif

	ReleaseSysCache(funcTup);

	return buf.data;
}

/*
 * rewrite CreateSchemaStmt parser production
 *
 * We don't bother with OptSchemaEltList, those will get back each separately
 * as new ProcessUtility queries with a SUBCOMMAND context.
 */
static char *
_rwCreateSchemaStmt(Oid objectId, const char *identity, Node *parsetree)
{
	CreateSchemaStmt	*node  = (CreateSchemaStmt *) parsetree;
	StringInfoData		 buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE SCHEMA%s %s",
					 node->if_not_exists ? " IF NOT EXISTS" : "",
					 identity);

	if (node->authid)
		appendStringInfo(&buf, " AUTHORIZATION %s", node->authid);

	return buf.data;
}

/*
 * rewrite CreateConversionStmt parser production
 */
static char *
_rwCreateConversionStmt(Oid objectId, const char *identity, Node *parsetree)
{
	CreateConversionStmt *node  = (CreateConversionStmt *) parsetree;
	StringInfoData		  buf;
	StringInfoData		fnamebuf;

	initStringInfo(&buf);
	initStringInfo(&fnamebuf);

	_rwAnyName(&fnamebuf, node->func_name);

	appendStringInfo(&buf, "CREATE%s CONVERSION %s FOR %s TO %s FROM %s;",
					 node->def ? " DEFAULT" : "",
					 identity,
					 node->for_encoding_name,
					 node->to_encoding_name,
					 fnamebuf.data);
	pfree(fnamebuf.data);

	return buf.data;
}

/*
 * rewrite DefineStmt parser productions
 */
static char *
_rwDefineStmt(Oid objectId, const char *identity, Node *parsetree)
{
	DefineStmt			*node = (DefineStmt *) parsetree;
	StringInfoData		 buf;
	ListCell			*opt;
	bool                first = true;

	initStringInfo(&buf);

	appendStringInfo(&buf, "CREATE %s %s ", objTypeToString(node->kind),
					 identity);

	if (node->definition)
	{
		appendStringInfoChar(&buf, '(');

		/* definition: grammar production */
		foreach(opt, node->definition)
		{
			DefElem    *defel = (DefElem *) lfirst(opt);

			maybeAddSeparator(&buf, ", ", &first);

			if (defel->arg != NULL)
			{
				Node *arg = (Node *)defel->arg;

				appendStringInfo(&buf, "%s=", defel->defname);

				/* def_arg: grammar production */
				switch (nodeTag(arg))
				{
					/* func_type */
					case T_TypeName:
						appendStringInfoString(&buf,
											   TypeNameToString((TypeName *)arg));
						break;

					/* reserved_keyword or Sconst */
					case T_String:
						appendStringInfo(&buf, "'%s'", defGetString(defel));
						break;

					/* qual_all_Op */
					case T_List:
						appendStringInfo(&buf, "OPERATOR (%s)",
										 strVal((Value *)linitial((List *)arg)));
						break;

					/* NumericOnly */
					case T_Float:
						appendStringInfo(&buf, "%g", defGetNumeric(defel));
						break;

					/* NumericOnly */
					case T_Integer:
					{
						char		num[100];

						snprintf(num, sizeof(num), INT64_FORMAT, defGetInt64(defel));
						appendStringInfo(&buf, "%s", num);
						break;
					}

					default:
						elog(DEBUG1, "unrecognized node type: %d",
							 (int) nodeTag(arg));
				}
			}
			else
			{
				appendStringInfo(&buf, "%s", defel->defname);
			}
		}
		appendStringInfoChar(&buf, ')');
	}

	return buf.data;
}

/*
 * rewrite ColQualList parser production
 *
 * FIXME this code assumes the ColQualList comes from a domain, but is this
 * always the case?
 */
static void
_rwColQualList(StringInfo buf, List *constraints,
			   const char *domainName, TypeName *typeName)
{
	ListCell			*lc;
	HeapTuple			 typeTup = NULL;
	Oid					 basetypeoid = InvalidOid;
	int32				 basetypeMod;

	foreach(lc, constraints)
	{
		Constraint *c = (Constraint *) lfirst(lc);
		Assert(IsA(c, Constraint));

		if (c->conname != NULL)
			appendStringInfo(buf, " CONSTRAINT %s", c->conname);

		switch (c->contype)
		{
			case CONSTR_NOTNULL:
				appendStringInfo(buf, " NOT NULL");
				break;

			case CONSTR_NULL:
				appendStringInfo(buf, " NULL");
				break;

			case CONSTR_DEFAULT:
			{
				char			*src;

				src = uncookConstraintOrDefault(c->raw_expr,
									   EXPR_KIND_COLUMN_DEFAULT,
									   NULL, InvalidOid);

				appendStringInfo(buf, " DEFAULT %s", src);
				break;
			}

			case CONSTR_CHECK:
			{
				Node					*expr;
				char					*ccsrc;
				ParseState				*pstate;
				CoerceToDomainValue		*domVal;

				if (typeTup == NULL)
				{
					typeTup = typenameType(NULL, typeName, &basetypeMod);
					basetypeoid = HeapTupleGetOid(typeTup);
				}

				domVal = makeNode(CoerceToDomainValue);
				domVal->typeId = basetypeoid;
				domVal->typeMod = basetypeMod;
				domVal->collation = get_typcollation(basetypeoid);
				domVal->location = -1;

				pstate = make_parsestate(NULL);
				pstate->p_value_substitute = (Node *) domVal;

				expr = transformExpr(pstate,
									 c->raw_expr,
									 EXPR_KIND_DOMAIN_CHECK);

				assign_expr_collations(pstate, expr);

				ccsrc = deparse_expression(expr, NIL, false, false);

				appendStringInfo(buf, " CHECK (%s)", ccsrc);
				break;
			}

			default:
				/* not a domain constraint */
				break;
		}
	}

	if (typeTup)
		ReleaseSysCache(typeTup);
}

/*
 * rewrite CreateDomainStmt parser production
 */
static char *
_rwCreateDomainStmt(Oid objectId, const char *identity, Node *parsetree)
{
	CreateDomainStmt *node  = (CreateDomainStmt *) parsetree;
	StringInfoData	buf;

	initStringInfo(&buf);

	appendStringInfo(&buf, "CREATE DOMAIN %s AS %s ",
					 identity,
					 TypeNameToString(node->typeName));

	if (node->collClause)
	{
		Oid		 collNspId;
		char	*collname;

		collNspId =
			QualifiedNameGetCreationNamespace(node->collClause->collname,
											  &collname);

		appendStringInfo(&buf, "COLLATE %s.%s ",
						 get_namespace_name(collNspId), collname);
	}
	_rwColQualList(&buf, node->constraints, identity, node->typeName);

	return buf.data;
}

/*
 * Given a utility command parsetree and the OID of the corresponding object,
 * return a textual representation of the command.
 *
 * The command is expanded fully, so that there are no ambiguities even in the
 * face of search_path changes.
 *
 * Note we currently only support commands for which ProcessUtilitySlow saves
 * objects to create; currently this excludes all forms of ALTER and DROP.
 */
char *
rewrite_utility_command(Oid objectId, const char *identity, Node *parsetree)
{

	switch (nodeTag(parsetree))
	{
		case T_CreateSchemaStmt:
			return _rwCreateSchemaStmt(objectId, identity, parsetree);

		case T_CreateStmt:
		case T_CreateForeignTableStmt:
			return _rwCreateStmt(objectId, identity, parsetree);

		case T_DefineStmt:
			return _rwDefineStmt(objectId, identity, parsetree);

		case T_IndexStmt:
			return _rwCreateIndexStmt(objectId, identity, parsetree);

		case T_CreateExtensionStmt:
			return _rwCreateExtensionStmt(objectId, identity, parsetree);

		case T_CreateFdwStmt:
			/* XXX these "unsupported" cases need to be filled in for
			 * final submission */
			return "unsupported CREATE FDW";
		case T_CreateForeignServerStmt:
			return "unsupported CREATE FOREIGN SERVER";

		case T_CreateUserMappingStmt:
			return "unsupported CREATE USER MAPPING";
		case T_CompositeTypeStmt:	/* CREATE TYPE (composite) */
		case T_CreateEnumStmt:		/* CREATE TYPE AS ENUM */
		case T_CreateRangeStmt:		/* CREATE TYPE AS RANGE */
			return "unsupported CREATE TYPE";

		case T_ViewStmt:
			return _rwViewStmt(objectId, identity, parsetree);

		case T_CreateFunctionStmt:
			return _rwCreateFunctionStmt(objectId, identity, parsetree);

		case T_RuleStmt:
			return "unsupported CREATE RULE";

		case T_CreateSeqStmt:
			return _rwCreateSeqStmt(objectId, identity, parsetree);

		case T_CreateTableAsStmt:
			return "unsupported CREATE TABLE AS";
		case T_RefreshMatViewStmt:
			return "unsupported REFRESH MATERIALIZED VIEW";
		case T_CreateTrigStmt:
			return "unsupported CREATE TRIGGER";
		case T_CreatePLangStmt:
			return "unsupported CREATE PROCEDURAL LANGUAGE";

		case T_CreateDomainStmt:
			return _rwCreateDomainStmt(objectId, identity, parsetree);

		case T_CreateConversionStmt:
			return _rwCreateConversionStmt(objectId, identity, parsetree);

		case T_CreateCastStmt:
			return "unsupported CREATE CAST";
		case T_CreateOpClassStmt:
			return "unsupported CREATE OPERATOR CLASS";
		case T_CreateOpFamilyStmt:
			return "unsupported CREATE OPERATOR FAMILY";

		default:
			elog(LOG, "unrecognized node type: %d",
				 (int) nodeTag(parsetree));
	}

	return NULL;
}
