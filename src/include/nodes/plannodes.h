/*-------------------------------------------------------------------------
 *
 * plannodes.h
 *	  definitions for query plan nodes
 *
 *
 * Portions Copyright (c) 2005-2008, Cloudberry inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/plannodes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNODES_H
#define PLANNODES_H

#include "access/sdir.h"
#include "access/stratnum.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/lockoptions.h"
#include "nodes/primnodes.h"
#include "parsenodes.h"

typedef struct DirectDispatchInfo
{
	/*
	 * if true then this Slice requires an n-gang but the gang can be
	 * targeted to fewer segments than the entire cluster.
	 *
	 * When true, 'contentIds' list the segments that this slice needs to be
	 * dispatched to.
	 */
	bool		isDirectDispatch;
	List	   *contentIds;

	/* only used while planning, in createplan.c */
	bool		haveProcessedAnyCalculations;
} DirectDispatchInfo;

typedef enum PlanGenerator
{
	PLANGEN_PLANNER,			/* plan produced by the planner*/
	PLANGEN_OPTIMIZER,			/* plan produced by the optimizer*/
} PlanGenerator;

/* DML Actions */
typedef enum DMLAction
{
	DML_DELETE,
	DML_INSERT
} DMLAction;

/* ----------------------------------------------------------------
 *						node definitions
 * ----------------------------------------------------------------
 */

/* ----------------
 *		PlannedStmt node
 *
 * The output of the planner is a Plan tree headed by a PlannedStmt node.
 * PlannedStmt holds the "one time" information needed by the executor.
 *
 * For simplicity in APIs, we also wrap utility statements in PlannedStmt
 * nodes; in such cases, commandType == CMD_UTILITY, the statement itself
 * is in the utilityStmt field, and the rest of the struct is mostly dummy.
 * (We do use canSetTag, stmt_location, stmt_len, and possibly queryId.)
 * ----------------
 */
typedef struct PlannedStmt
{
	NodeTag		type;

	CmdType		commandType;	/* select|insert|update|delete|utility */

	PlanGenerator	planGen;		/* optimizer generation */

	uint64		queryId;		/* query identifier (copied from Query) */

	bool		hasReturning;	/* is it insert|update|delete RETURNING? */

	bool		hasModifyingCTE;	/* has insert|update|delete in WITH? */

	bool		canSetTag;		/* do I set the command result tag? */

	bool		transientPlan;	/* redo plan when TransactionXmin changes? */
	bool		oneoffPlan;		/* redo plan on every execution? */

	Oid			simplyUpdatableRel; /* if valid, query can be used with CURRENT OF for this rel */

	bool		dependsOnRole;	/* is plan specific to current role? */

	bool		parallelModeNeeded; /* parallel mode required to execute? */

	int			jitFlags;		/* which forms of JIT should be performed */

	struct Plan *planTree;		/* tree of Plan nodes */

	/* Slice table */
	int			numSlices;
	struct PlanSlice *slices;

	List	   *rtable;			/* list of RangeTblEntry nodes */

	/* rtable indexes of target relations for INSERT/UPDATE/DELETE */
	List	   *resultRelations;	/* integer list of RT indexes, or NIL */

	List	   *appendRelations;	/* list of AppendRelInfo nodes */

	List	   *subplans;		/* Plan trees for SubPlan expressions; note
								 * that some could be NULL */
	int		   *subplan_sliceIds;	/* slice IDs containing SubPlans; size equals 'subplans' */

	Bitmapset  *rewindPlanIDs;	/* indices of subplans that require REWIND */

	List	   *rowMarks;		/* a list of PlanRowMark's */

	List	   *relationOids;	/* OIDs of relations the plan depends on */

	List	   *invalItems;		/* other dependencies, as PlanInvalItems */

	List	   *paramExecTypes; /* type OIDs for PARAM_EXEC Params */

	Node	   *utilityStmt;	/* non-null if this is utility stmt */

	/* statement location in source string (copied from Query) */
	int			stmt_location;	/* start location, or -1 if unknown */
	int			stmt_len;		/* length in bytes; 0 means "rest of string" */

	/* 
	 * Cloned from top Query node at the end of planning.
	 * Holds the result distribution policy
	 * for SELECT ... INTO and set operations.
	 */
	struct GpPolicy  *intoPolicy;

	/* What is the memory reserved for this query's execution? */
	uint64		query_mem;

	/*
	 * GPDB: Used to keep target information for CTAS and it is needed
	 * to be dispatched to QEs.
	 */
	IntoClause *intoClause;
	CopyIntoClause *copyIntoClause;
	RefreshClause   *refreshClause;		/* relation to insert into */

	/* 
 	 * GPDB: whether a query is a SPI inner query for extension usage 
 	 */
	int8		metricsQueryType;

	/*
	 * Facilitate various extensions to pass context
	 */
	List 		*extensionContext;
} PlannedStmt;

/*
 * Fetch the Plan associated with a SubPlan node in a completed PlannedStmt.
 */
static inline struct Plan *exec_subplan_get_plan(struct PlannedStmt *plannedstmt, SubPlan *subplan)
{
	return (struct Plan *) list_nth(plannedstmt->subplans, subplan->plan_id - 1);
}

/*
 * Rewrite the Plan associated with a SubPlan node in a completed PlannedStmt.
 */
static inline void exec_subplan_put_plan(struct PlannedStmt *plannedstmt, SubPlan *subplan, struct Plan *plan)
{
	ListCell *cell = list_nth_cell(plannedstmt->subplans, subplan->plan_id-1);
	cell->ptr_value = plan;
}

/*
 * FlowType - kinds of tuple flows in parallelized plans.
 *
 * This enum is a MPP extension.
 */
typedef enum FlowType
{
	FLOW_UNDEFINED,		/* used prior to calculation of type of derived flow */
	FLOW_SINGLETON,		/* flow has single stream */
	FLOW_REPLICATED,	/* flow is replicated across IOPs */
	FLOW_PARTITIONED,	/* flow is partitioned across IOPs */
} FlowType;

/*----------
 * Flow - describes a tuple flow in a parallelized plan
 *
 * This node type is a MPP extension.
 *
 * Plan nodes contain a reference to a Flow that characterizes the output
 * tuple flow of the node.
 *----------
 */
typedef struct Flow
{
	NodeTag		type;			/* T_Flow */
	FlowType	flotype;		/* Type of flow produced by the plan. */

	/* Locus type (optimizer flow characterization).
	 */
	CdbLocusType	locustype;

	/* If flotype is FLOW_SINGLETON, then this is the segment (-1 for entry)
	 * on which tuples occur.
	 */
	int			segindex;		/* Segment index of singleton flow. */
	int         numsegments;

} Flow;

/* GangType enumeration is used in several structures related to CDB
 * slice plan support.
 */
typedef enum GangType
{
	GANGTYPE_UNALLOCATED,       /* a root slice executed by the qDisp */
	GANGTYPE_ENTRYDB_READER,    /* a 1-gang with read access to the entry db */
	GANGTYPE_SINGLETON_READER,	/* a 1-gang to read the segment dbs */
	GANGTYPE_PRIMARY_READER,    /* a 1-gang or N-gang to read the segment dbs */
	GANGTYPE_PRIMARY_WRITER		/* the N-gang that can update the segment dbs */
} GangType;

/*
 * PlanSlice represents one query slice, to be executed by a separate gang
 * of executor processes.
 */
typedef struct PlanSlice
{
	int			sliceIndex;
	int			parentIndex;

	GangType	gangType;

	/* # of segments in the gang, for PRIMARY_READER/WRITER slices */
	int			numsegments;
	int			parallel_workers;
	/* segment to execute on, for SINGLETON_READER slices */
	int			segindex;

	/* direct dispatch information, for PRIMARY_READER/WRITER slices */
	DirectDispatchInfo directDispatch;
} PlanSlice;

/* ----------------
 *		Plan node
 *
 * All plan nodes "derive" from the Plan structure by having the
 * Plan structure as the first field.  This ensures that everything works
 * when nodes are cast to Plan's.  (node pointers are frequently cast to Plan*
 * when passed around generically in the executor)
 *
 * We never actually instantiate any Plan nodes; this is just the common
 * abstract superclass for all Plan-type nodes.
 * ----------------
 */
typedef struct Plan
{
	NodeTag		type;

	/*
	 * estimated execution costs for plan (see costsize.c for more info)
	 */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/*
	 * planner's estimate of result size of this plan step
	 */
	double		plan_rows;		/* number of rows plan is expected to emit */
	int			plan_width;		/* average row width in bytes */

	/*
	 * information needed for parallel query
	 */
	bool		parallel_aware; /* engage parallel-aware logic? */
	bool		parallel_safe;	/* OK to use as part of parallel plan? */

	/*
	 * information needed for asynchronous execution
	 */
	bool		async_capable;	/* engage asynchronous-capable logic? */

	/*
	 * Common structural data for all Plan types.
	 */
	int			plan_node_id;	/* unique across entire final plan tree */
	List	   *targetlist;		/* target list to be computed at this node */
	List	   *qual;			/* implicitly-ANDed qual conditions */
	struct Plan *lefttree;		/* input plan tree(s) */
	struct Plan *righttree;
	List	   *initPlan;		/* Init Plan nodes (un-correlated expr
								 * subselects) */

	/*
	 * Information for management of parameter-change-driven rescanning
	 *
	 * extParam includes the paramIDs of all external PARAM_EXEC params
	 * affecting this plan node or its children.  setParam params from the
	 * node's initPlans are not included, but their extParams are.
	 *
	 * allParam includes all the extParam paramIDs, plus the IDs of local
	 * params that affect the node (i.e., the setParams of its initplans).
	 * These are _all_ the PARAM_EXEC params that affect this node.
	 */
	Bitmapset  *extParam;
	Bitmapset  *allParam;

	/*
	 * MPP needs to keep track of the characteristics of flow of output
	 * tuple of Plan nodes.
	 */
	Flow		*flow;			/* Flow description.  Initially NULL.
	 * Set during parallelization.
	 */

	/*
	 * GPDB parallel
	 * Additional fields here are used to show locus type and
	 * parallel workers of plan node.
	 * Field flow has the locus info only in the top Plan nodes,
	 * other nodes couldn't be set that.
	 */
	CdbLocusType	locustype;
	int 			parallel; /* parallel workers of this plan if there was */

	/**
	 * How much memory (in KB) should be used to execute this plan node?
	 */
	uint64 operatorMemKB;
} Plan;

/* ----------------
 *	these are defined to avoid confusion problems with "left"
 *	and "right" and "inner" and "outer".  The convention is that
 *	the "left" plan is the "outer" plan and the "right" plan is
 *	the inner plan, but these make the code more readable.
 * ----------------
 */
#define innerPlan(node)			(((Plan *)(node))->righttree)
#define outerPlan(node)			(((Plan *)(node))->lefttree)


/* ----------------
 *	 Result node -
 *		If no outer plan, evaluate a variable-free targetlist.
 *		If outer plan, return tuples from outer plan (after a level of
 *		projection as shown by targetlist).
 *
 * If resconstantqual isn't NULL, it represents a one-time qualification
 * test (i.e., one that doesn't depend on any variables from the outer plan,
 * so needs to be evaluated only once).
 *
 * If numHashFilterCols is non-zero, we compute a cdbhash value based
 * on the columns listed in hashFilterColIdx for each input row. If the
 * target segment based on the hash doesn't match the current execution
 * segment, the row is discarded.
 * ----------------
 */
typedef struct Result
{
	Plan		plan;
	Node	   *resconstantqual;

	int			numHashFilterCols;
	AttrNumber *hashFilterColIdx;
	Oid		   *hashFilterFuncs;
} Result;

/* ----------------
 *	 ProjectSet node -
 *		Apply a projection that includes set-returning functions to the
 *		output tuples of the outer plan.
 * ----------------
 */
typedef struct ProjectSet
{
	Plan		plan;
} ProjectSet;

/* ----------------
 *	 ModifyTable node -
 *		Apply rows produced by outer plan to result table(s),
 *		by inserting, updating, or deleting.
 *
 * If the originally named target table is a partitioned table, both
 * nominalRelation and rootRelation contain the RT index of the partition
 * root, which is not otherwise mentioned in the plan.  Otherwise rootRelation
 * is zero.  However, nominalRelation will always be set, as it's the rel that
 * EXPLAIN should claim is the INSERT/UPDATE/DELETE target.
 *
 * Note that rowMarks and epqParam are presumed to be valid for all the
 * table(s); they can't contain any info that varies across tables.
 * ----------------
 */
typedef struct ModifyTable
{
	Plan		plan;
	CmdType		operation;		/* INSERT, UPDATE, or DELETE */
	bool		canSetTag;		/* do we set the command tag/es_processed? */
	Index		nominalRelation;	/* Parent RT index for use of EXPLAIN */
	Index		rootRelation;	/* Root RT index, if target is partitioned */
	bool		partColsUpdated;	/* some part key in hierarchy updated? */
	bool		splitUpdate;		/* if it's split update */
	List	   *resultRelations;	/* integer list of RT indexes */
	List	   *updateColnosLists;	/* per-target-table update_colnos lists */
	List	   *withCheckOptionLists;	/* per-target-table WCO lists */
	List	   *returningLists; /* per-target-table RETURNING tlists */
	List	   *fdwPrivLists;	/* per-target-table FDW private data lists */
	Bitmapset  *fdwDirectModifyPlans;	/* indices of FDW DM plans */
	List	   *rowMarks;		/* PlanRowMarks (non-locking only) */
	int			epqParam;		/* ID of Param for EvalPlanQual re-eval */
	OnConflictAction onConflictAction;	/* ON CONFLICT action */
	List	   *arbiterIndexes; /* List of ON CONFLICT arbiter index OIDs  */
	List	   *onConflictSet;	/* INSERT ON CONFLICT DO UPDATE targetlist */
	List	   *onConflictCols; /* target column numbers for onConflictSet */
	Node	   *onConflictWhere;	/* WHERE for ON CONFLICT UPDATE */
	Index		exclRelRTI;		/* RTI of the EXCLUDED pseudo relation */
	List	   *exclRelTlist;	/* tlist of the EXCLUDED pseudo relation */
} ModifyTable;

struct PartitionPruneInfo;		/* forward reference to struct below */

/* ----------------
 *	 Append node -
 *		Generate the concatenation of the results of sub-plans.
 * ----------------
 */
typedef struct Append
{
	Plan		plan;
	Bitmapset  *apprelids;		/* RTIs of appendrel(s) formed by this node */
	List	   *appendplans;
	int			nasyncplans;	/* # of asynchronous plans */

	/*
	 * All 'appendplans' preceding this index are non-partial plans. All
	 * 'appendplans' from this index onwards are partial plans.
	 */
	int			first_partial_plan;

	/* Info for run-time subplan pruning; NULL if we're not doing that */
	struct PartitionPruneInfo *part_prune_info;

	/*
	 * Info for run-time join pruning, using Partition Selector nodes.
	 * These param IDs contain additional Bitmapsets containing selected
	 * partitions.
	 */
	List	   *join_prune_paramids;
} Append;

/* ----------------
 *	 MergeAppend node -
 *		Merge the results of pre-sorted sub-plans to preserve the ordering.
 * ----------------
 */
typedef struct MergeAppend
{
	Plan		plan;
	Bitmapset  *apprelids;		/* RTIs of appendrel(s) formed by this node */
	List	   *mergeplans;
	/* these fields are just like the sort-key info in struct Sort: */
	int			numCols;		/* number of sort-key columns */
	AttrNumber *sortColIdx;		/* their indexes in the target list */
	Oid		   *sortOperators;	/* OIDs of operators to sort them by */
	Oid		   *collations;		/* OIDs of collations */
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */
	/* Info for run-time subplan pruning; NULL if we're not doing that */
	struct PartitionPruneInfo *part_prune_info;

	/*
	 * Info for run-time join pruning, using Partition Selector nodes.
	 * These param IDs contain additional Bitmapsets containing selected
	 * partitions.
	 */
	List	   *join_prune_paramids;
} MergeAppend;

/*
 * Sequence node
 *   Execute a list of subplans in the order of left-to-right, and return
 * the results of the last subplan.
 */
typedef struct Sequence
{
	Plan plan;
	List *subplans;
} Sequence;

/* ----------------
 *	RecursiveUnion node -
 *		Generate a recursive union of two subplans.
 *
 * The "outer" subplan is always the non-recursive term, and the "inner"
 * subplan is the recursive term.
 * ----------------
 */
typedef struct RecursiveUnion
{
	Plan		plan;
	int			wtParam;		/* ID of Param representing work table */
	/* Remaining fields are zero/null in UNION ALL case */
	int			numCols;		/* number of columns to check for
								 * duplicate-ness */
	AttrNumber *dupColIdx;		/* their indexes in the target list */
	Oid		   *dupOperators;	/* equality operators to compare with */
	Oid		   *dupCollations;
	long		numGroups;		/* estimated number of groups in input */
} RecursiveUnion;

/* ----------------
 *	 BitmapAnd node -
 *		Generate the intersection of the results of sub-plans.
 *
 * The subplans must be of types that yield tuple bitmaps.  The targetlist
 * and qual fields of the plan are unused and are always NIL.
 * ----------------
 */
typedef struct BitmapAnd
{
	Plan		plan;
	List	   *bitmapplans;
} BitmapAnd;

/* ----------------
 *	 BitmapOr node -
 *		Generate the union of the results of sub-plans.
 *
 * The subplans must be of types that yield tuple bitmaps.  The targetlist
 * and qual fields of the plan are unused and are always NIL.
 * ----------------
 */
typedef struct BitmapOr
{
	Plan		plan;
	bool		isshared;
	List	   *bitmapplans;
} BitmapOr;

/*
 * ==========
 * Scan nodes
 * ==========
 */
typedef struct Scan
{
	Plan		plan;
	Index		scanrelid;		/* relid is index into the range table */
} Scan;

/* ----------------
 *		sequential scan node
 * ----------------
 */
typedef Scan SeqScan;

/* ----------------
 *		table sample scan node
 * ----------------
 */
typedef struct SampleScan
{
	Scan		scan;
	/* use struct pointer to avoid including parsenodes.h here */
	struct TableSampleClause *tablesample;
} SampleScan;

/* ----------------
 *		index scan node
 *
 * indexqualorig is an implicitly-ANDed list of index qual expressions, each
 * in the same form it appeared in the query WHERE condition.  Each should
 * be of the form (indexkey OP comparisonval) or (comparisonval OP indexkey).
 * The indexkey is a Var or expression referencing column(s) of the index's
 * base table.  The comparisonval might be any expression, but it won't use
 * any columns of the base table.  The expressions are ordered by index
 * column position (but items referencing the same index column can appear
 * in any order).  indexqualorig is used at runtime only if we have to recheck
 * a lossy indexqual.
 *
 * indexqual has the same form, but the expressions have been commuted if
 * necessary to put the indexkeys on the left, and the indexkeys are replaced
 * by Var nodes identifying the index columns (their varno is INDEX_VAR and
 * their varattno is the index column number).
 *
 * indexorderbyorig is similarly the original form of any ORDER BY expressions
 * that are being implemented by the index, while indexorderby is modified to
 * have index column Vars on the left-hand side.  Here, multiple expressions
 * must appear in exactly the ORDER BY order, and this is not necessarily the
 * index column order.  Only the expressions are provided, not the auxiliary
 * sort-order information from the ORDER BY SortGroupClauses; it's assumed
 * that the sort ordering is fully determinable from the top-level operators.
 * indexorderbyorig is used at runtime to recheck the ordering, if the index
 * cannot calculate an accurate ordering.  It is also needed for EXPLAIN.
 *
 * indexorderbyops is a list of the OIDs of the operators used to sort the
 * ORDER BY expressions.  This is used together with indexorderbyorig to
 * recheck ordering at run time.  (Note that indexorderby, indexorderbyorig,
 * and indexorderbyops are used for amcanorderbyop cases, not amcanorder.)
 *
 * indexorderdir specifies the scan ordering, for indexscans on amcanorder
 * indexes (for other indexes it should be "don't care").
 * ----------------
 */
typedef struct IndexScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	List	   *indexqual;		/* list of index quals (usually OpExprs) */
	List	   *indexqualorig;	/* the same in original form */
	List	   *indexorderby;	/* list of index ORDER BY exprs */
	List	   *indexorderbyorig;	/* the same in original form */
	List	   *indexorderbyops;	/* OIDs of sort ops for ORDER BY exprs */
	ScanDirection indexorderdir;	/* forward or backward or don't care */
} IndexScan;

/* ----------------
 *		index-only scan node
 *
 * IndexOnlyScan is very similar to IndexScan, but it specifies an
 * index-only scan, in which the data comes from the index not the heap.
 * Because of this, *all* Vars in the plan node's targetlist, qual, and
 * index expressions reference index columns and have varno = INDEX_VAR.
 *
 * We could almost use indexqual directly against the index's output tuple
 * when rechecking lossy index operators, but that won't work for quals on
 * index columns that are not retrievable.  Hence, recheckqual is needed
 * for rechecks: it expresses the same condition as indexqual, but using
 * only index columns that are retrievable.  (We will not generate an
 * index-only scan if this is not possible.  An example is that if an
 * index has table column "x" in a retrievable index column "ind1", plus
 * an expression f(x) in a non-retrievable column "ind2", an indexable
 * query on f(x) will use "ind2" in indexqual and f(ind1) in recheckqual.
 * Without the "ind1" column, an index-only scan would be disallowed.)
 *
 * We don't currently need a recheckable equivalent of indexorderby,
 * because we don't support lossy operators in index ORDER BY.
 *
 * To help EXPLAIN interpret the index Vars for display, we provide
 * indextlist, which represents the contents of the index as a targetlist
 * with one TLE per index column.  Vars appearing in this list reference
 * the base table, and this is the only field in the plan node that may
 * contain such Vars.  Also, for the convenience of setrefs.c, TLEs in
 * indextlist are marked as resjunk if they correspond to columns that
 * the index AM cannot reconstruct.
 * ----------------
 * GPDB: We need indexqualorig to determine direct dispatch, however there
 * is no need to dispatch it.
 */
typedef struct IndexOnlyScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	List	   *indexqual;		/* list of index quals (usually OpExprs) */
	List	   *indexqualorig;	/* the same in original form (GPDB keeps it) */
	List	   *indexorderby;	/* list of index ORDER BY exprs */
	List	   *indextlist;		/* TargetEntry list describing index's cols */
	ScanDirection indexorderdir;	/* forward or backward or don't care */
	List	   *recheckqual;	/* index quals in recheckable form */
} IndexOnlyScan;

/* ----------------
 *		bitmap index scan node
 *
 * BitmapIndexScan delivers a bitmap of potential tuple locations;
 * it does not access the heap itself.  The bitmap is used by an
 * ancestor BitmapHeapScan node, possibly after passing through
 * intermediate BitmapAnd and/or BitmapOr nodes to combine it with
 * the results of other BitmapIndexScans.
 *
 * The fields have the same meanings as for IndexScan, except we don't
 * store a direction flag because direction is uninteresting.
 *
 * In a BitmapIndexScan plan node, the targetlist and qual fields are
 * not used and are always NIL.  The indexqualorig field is unused at
 * run time too, but is saved for the benefit of EXPLAIN, as well
 * as for the use of the planner when doing clause examination on plans
 * (such as for targeted dispatch)
 * ----------------
 */
typedef struct BitmapIndexScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	bool		isshared;		/* Create shared bitmap if set */
	List	   *indexqual;		/* list of index quals (OpExprs) */
	List	   *indexqualorig;	/* the same in original form */
} BitmapIndexScan;

/* ----------------
 *		bitmap sequential scan node
 *
 * This needs a copy of the qual conditions being used by the input index
 * scans because there are various cases where we need to recheck the quals;
 * for example, when the bitmap is lossy about the specific rows on a page
 * that meet the index condition.
 * ----------------
 */
typedef struct BitmapHeapScan
{
	Scan		scan;
	List	   *bitmapqualorig; /* index quals, in standard expr form */
} BitmapHeapScan;

/* ----------------
 *		tid scan node
 *
 * tidquals is an implicitly OR'ed list of qual expressions of the form
 * "CTID = pseudoconstant", or "CTID = ANY(pseudoconstant_array)",
 * or a CurrentOfExpr for the relation.
 * ----------------
 */
typedef struct TidScan
{
	Scan		scan;
	List	   *tidquals;		/* qual(s) involving CTID = something */
} TidScan;

/* ----------------
 *		tid range scan node
 *
 * tidrangequals is an implicitly AND'ed list of qual expressions of the form
 * "CTID relop pseudoconstant", where relop is one of >,>=,<,<=.
 * ----------------
 */
typedef struct TidRangeScan
{
	Scan		scan;
	List	   *tidrangequals;	/* qual(s) involving CTID op something */
} TidRangeScan;

/* ----------------
 *		subquery scan node
 *
 * SubqueryScan is for scanning the output of a sub-query in the range table.
 * We often need an extra plan node above the sub-query's plan to perform
 * expression evaluations (which we can't push into the sub-query without
 * risking changing its semantics).  Although we are not scanning a physical
 * relation, we make this a descendant of Scan anyway for code-sharing
 * purposes.
 *
 * Note: we store the sub-plan in the type-specific subplan field, not in
 * the generic lefttree field as you might expect.  This is because we do
 * not want plan-tree-traversal routines to recurse into the subplan without
 * knowing that they are changing Query contexts.
 * ----------------
 */
typedef struct SubqueryScan
{
	Scan		scan;
	Plan	   *subplan;
} SubqueryScan;

/* ----------------
 *		FunctionScan node
 * ----------------
 */
typedef struct FunctionScan
{
	Scan		scan;
	List	   *functions;		/* list of RangeTblFunction nodes */
	bool		funcordinality; /* WITH ORDINALITY */
	Param      *param;			/* used when funtionscan run as initplan */
	bool		resultInTupleStore; /* function result stored in tuplestore */
	int			initplanId;			/* initplan id for function execute on initplan */
} FunctionScan;

/* ----------------
 *      TableFunctionScan node
 *
 * This is similar to a FunctionScan, but we only support one function,
 * and WITH ORDINALITY is not supported.
 *
 * ----------------
 */
typedef struct TableFunctionScan
{
	Scan		scan;
	struct RangeTblFunction *function;
} TableFunctionScan;

/* ----------------
 *		ValuesScan node
 * ----------------
 */
typedef struct ValuesScan
{
	Scan		scan;
	List	   *values_lists;	/* list of expression lists */
} ValuesScan;

/* ----------------
 *		TableFunc scan node
 * ----------------
 */
typedef struct TableFuncScan
{
	Scan		scan;
	TableFunc  *tablefunc;		/* table function node */
} TableFuncScan;

/* ----------------
 *		CteScan node
 * ----------------
 */
typedef struct CteScan
{
	Scan		scan;
	int			ctePlanId;		/* ID of init SubPlan for CTE */
	int			cteParam;		/* ID of Param representing CTE output */
} CteScan;

/* ----------------
 *		NamedTuplestoreScan node
 * ----------------
 */
typedef struct NamedTuplestoreScan
{
	Scan		scan;
	char	   *enrname;		/* Name given to Ephemeral Named Relation */
} NamedTuplestoreScan;

/* ----------------
 *		WorkTableScan node
 * ----------------
 */
typedef struct WorkTableScan
{
	Scan		scan;
	int			wtParam;		/* ID of Param representing work table */
} WorkTableScan;

/* ----------------
 * External Scan parameters
 *
 * Field filenames is a list of N string node pointers (or NULL)
 * where N is number of segments in the array. The pointer in
 * position I is NULL or points to the string node containing the
 * file name for segment I.
 *
 * This used to be a separate node type. Now it's just used to carry
 * the parameters for a Foreign Scan on an external table, for the
 * shim layer.
 * ----------------
 */
typedef struct ExternalScanInfo
{
	NodeTag		type;
	List		*uriList;       /* data uri or null for each segment  */
	char		fmtType;        /* data format type                   */
	bool		isMasterOnly;   /* true for EXECUTE on master seg only */
	int			rejLimit;       /* reject limit (-1 for no sreh)      */
	bool		rejLimitInRows; /* true if ROWS false if PERCENT      */
	char		logErrors;      /* 't', 'p' to log errors into file. 'p' makes persistent error log */
	int			encoding;		/* encoding of external table data    */
	uint32      scancounter;	/* counter incr per scan node created */
	List	   *extOptions;		/* external options */
} ExternalScanInfo;

/* ----------------
 *		ForeignScan node
 *
 * fdw_exprs and fdw_private are both under the control of the foreign-data
 * wrapper, but fdw_exprs is presumed to contain expression trees and will
 * be post-processed accordingly by the planner; fdw_private won't be.
 * Note that everything in both lists must be copiable by copyObject().
 * One way to store an arbitrary blob of bytes is to represent it as a bytea
 * Const.  Usually, though, you'll be better off choosing a representation
 * that can be dumped usefully by nodeToString().
 *
 * fdw_scan_tlist is a targetlist describing the contents of the scan tuple
 * returned by the FDW; it can be NIL if the scan tuple matches the declared
 * rowtype of the foreign table, which is the normal case for a simple foreign
 * table scan.  (If the plan node represents a foreign join, fdw_scan_tlist
 * is required since there is no rowtype available from the system catalogs.)
 * When fdw_scan_tlist is provided, Vars in the node's tlist and quals must
 * have varno INDEX_VAR, and their varattnos correspond to resnos in the
 * fdw_scan_tlist (which are also column numbers in the actual scan tuple).
 * fdw_scan_tlist is never actually executed; it just holds expression trees
 * describing what is in the scan tuple's columns.
 *
 * fdw_recheck_quals should contain any quals which the core system passed to
 * the FDW but which were not added to scan.plan.qual; that is, it should
 * contain the quals being checked remotely.  This is needed for correct
 * behavior during EvalPlanQual rechecks.
 *
 * When the plan node represents a foreign join, scan.scanrelid is zero and
 * fs_relids must be consulted to identify the join relation.  (fs_relids
 * is valid for simple scans as well, but will always match scan.scanrelid.)
 *
 * If the FDW's PlanDirectModify() callback decides to repurpose a ForeignScan
 * node to perform the UPDATE or DELETE operation directly in the remote
 * server, it sets 'operation' and 'resultRelation' to identify the operation
 * type and target relation.  Note that these fields are only set if the
 * modification is performed *fully* remotely; otherwise, the modification is
 * driven by a local ModifyTable node and 'operation' is left to CMD_SELECT.
 * ----------------
 */
typedef struct ForeignScan
{
	Scan		scan;
	CmdType		operation;		/* SELECT/INSERT/UPDATE/DELETE */
	Index		resultRelation; /* direct modification target's RT index */
	Oid			fs_server;		/* OID of foreign server */
	List	   *fdw_exprs;		/* expressions that FDW may evaluate */
	List	   *fdw_private;	/* private data for FDW */
	List	   *fdw_scan_tlist; /* optional tlist describing scan tuple */
	List	   *fdw_recheck_quals;	/* original quals not in scan.plan.qual */
	Bitmapset  *fs_relids;		/* RTIs generated by this scan */
	bool		fsSystemCol;	/* true if any "system column" is needed */
} ForeignScan;

/* ----------------
 *	   CustomScan node
 *
 * The comments for ForeignScan's fdw_exprs, fdw_private, fdw_scan_tlist,
 * and fs_relids fields apply equally to CustomScan's custom_exprs,
 * custom_private, custom_scan_tlist, and custom_relids fields.  The
 * convention of setting scan.scanrelid to zero for joins applies as well.
 *
 * Note that since Plan trees can be copied, custom scan providers *must*
 * fit all plan data they need into those fields; embedding CustomScan in
 * a larger struct will not work.
 * ----------------
 */
struct CustomScanMethods;

typedef struct CustomScan
{
	Scan		scan;
	uint32		flags;			/* mask of CUSTOMPATH_* flags, see
								 * nodes/extensible.h */
	List	   *custom_plans;	/* list of Plan nodes, if any */
	List	   *custom_exprs;	/* expressions that custom code may evaluate */
	List	   *custom_private; /* private data for custom code */
	List	   *custom_scan_tlist;	/* optional tlist describing scan tuple */
	Bitmapset  *custom_relids;	/* RTIs generated by this scan */
	const struct CustomScanMethods *methods;
} CustomScan;

/*
 * ==========
 * Join nodes
 * ==========
 */

/* ----------------
 *		Join node
 *
 * jointype:	rule for joining tuples from left and right subtrees
 * inner_unique each outer tuple can match to no more than one inner tuple
 * joinqual:	qual conditions that came from JOIN/ON or JOIN/USING
 *				(plan.qual contains conditions that came from WHERE)
 *
 * When jointype is INNER, joinqual and plan.qual are semantically
 * interchangeable.  For OUTER jointypes, the two are *not* interchangeable;
 * only joinqual is used to determine whether a match has been found for
 * the purpose of deciding whether to generate null-extended tuples.
 * (But plan.qual is still applied before actually returning a tuple.)
 * For an outer join, only joinquals are allowed to be used as the merge
 * or hash condition of a merge or hash join.
 *
 * inner_unique is set if the joinquals are such that no more than one inner
 * tuple could match any given outer tuple.  This allows the executor to
 * skip searching for additional matches.  (This must be provable from just
 * the joinquals, ignoring plan.qual, due to where the executor tests it.)
 * ----------------
 */
typedef struct Join
{
	Plan		plan;
	JoinType	jointype;
	bool		inner_unique;
	List	   *joinqual;		/* JOIN quals (in addition to plan.qual) */

	bool		prefetch_inner; /* to avoid deadlock in MPP */
	bool		prefetch_joinqual; /* to avoid deadlock in MPP */
	bool		prefetch_qual; /* to avoid deadlock in MPP */
} Join;

/* ----------------
 *		nest loop join node
 *
 * The nestParams list identifies any executor Params that must be passed
 * into execution of the inner subplan carrying values from the current row
 * of the outer subplan.  Currently we restrict these values to be simple
 * Vars, but perhaps someday that'd be worth relaxing.  (Note: during plan
 * creation, the paramval can actually be a PlaceHolderVar expression; but it
 * must be a Var with varno OUTER_VAR by the time it gets to the executor.)
 * ----------------
 */
typedef struct NestLoop
{
	Join		join;
	List	   *nestParams;		/* list of NestLoopParam nodes */

	bool		shared_outer;
	bool		singleton_outer; /*CDB-OLAP true => outer is plain Agg */
} NestLoop;

typedef struct NestLoopParam
{
	NodeTag		type;
	int			paramno;		/* number of the PARAM_EXEC Param to set */
	Var		   *paramval;		/* outer-relation Var to assign to Param */
} NestLoopParam;

/* ----------------
 *		merge join node
 *
 * The expected ordering of each mergeable column is described by a btree
 * opfamily OID, a collation OID, a direction (BTLessStrategyNumber or
 * BTGreaterStrategyNumber) and a nulls-first flag.  Note that the two sides
 * of each mergeclause may be of different datatypes, but they are ordered the
 * same way according to the common opfamily and collation.  The operator in
 * each mergeclause must be an equality operator of the indicated opfamily.
 * ----------------
 */
typedef struct MergeJoin
{
	Join		join;
	bool		skip_mark_restore;	/* Can we skip mark/restore calls? */
	List	   *mergeclauses;	/* mergeclauses as expression trees */
	/* these are arrays, but have the same length as the mergeclauses list: */
	Oid		   *mergeFamilies;	/* per-clause OIDs of btree opfamilies */
	Oid		   *mergeCollations;	/* per-clause OIDs of collations */
	int		   *mergeStrategies;	/* per-clause ordering (ASC or DESC) */
	bool	   *mergeNullsFirst;	/* per-clause nulls ordering */
	bool		unique_outer; /*CDB-OLAP true => outer is unique in merge key */
} MergeJoin;

/* ----------------
 *		hash join (probe) node
 *
 * CDB:	In order to support hash join on IS NOT DISTINCT FROM (as well as =),
 *		field hashqualclauses is added to hold the expression that tests for
 *		a match.  This is normally identical to hashclauses (which holds the
 *		equality test), but differs in case of non-equijoin comparisons.
 *		Field hashclauses is retained for use in hash table operations.
 * ----------------
 */
typedef struct HashJoin
{
	Join		join;
	List	   *hashclauses;
	List	   *hashoperators;
	List	   *hashcollations;

	/*
	 * List of expressions to be hashed for tuples from the outer plan, to
	 * perform lookups in the hashtable over the inner plan.
	 */
	List	   *hashkeys;
	List	   *hashqualclauses;
	bool	    batch0_barrier;
} HashJoin;

#define SHARE_ID_NOT_SHARED (-1)
#define SHARE_ID_NOT_ASSIGNED (-2)

/* ----------------
 *		shareinputscan node
 * ----------------
 */
typedef struct ShareInputScan
{
	Scan 		scan;

	bool		cross_slice;
	int 		share_id;

	/*
	 * Slice that produces the tuplestore for this shared scan.
	 *
	 * As a special case, in a plan that has only one slice, this may be left
	 * to -1. The executor node ignores this when there is only one slice.
	 */
	int			producer_slice_id;

	/*
	 * Slice id that this ShareInputScan node runs in. If it's
	 * different from current slice ID, this ShareInputScan is "alien"
	 * to the current slice and doesn't need to be executed at all (in
	 * this slice). It is used to skip IPC in alien nodes.
	 *
	 * Like producer_slice_id, this can be left to -1 if there is only one
	 * slice in the plan tree.
	 */
	int			this_slice_id;

	/* Number of consumer slices participating, not including the producer. */
	int			nconsumers;
} ShareInputScan;

/* ----------------
 *		materialization node
 * ----------------
 */
typedef struct Material
{
	Plan		plan;
	bool		cdb_strict;
	bool		cdb_shield_child_from_rescans;
} Material;

/* ----------------
 *		memoize node
 * ----------------
 */
typedef struct Memoize
{
	Plan		plan;

	int			numKeys;		/* size of the two arrays below */

	Oid		   *hashOperators;	/* hash operators for each key */
	Oid		   *collations;		/* collations for each key */
	List	   *param_exprs;	/* cache keys in the form of exprs containing
								 * parameters */
	bool		singlerow;		/* true if the cache entry should be marked as
								 * complete after we store the first tuple in
								 * it. */
	bool		binary_mode;	/* true when cache key should be compared bit
								 * by bit, false when using hash equality ops */
	uint32		est_entries;	/* The maximum number of entries that the
								 * planner expects will fit in the cache, or 0
								 * if unknown */
	Bitmapset   *keyparamids;	/* paramids from param_exprs */
} Memoize;

/* ----------------
 *		sort node
 * ----------------
 */
typedef struct Sort
{
	Plan		plan;
	int			numCols;		/* number of sort-key columns */
	AttrNumber *sortColIdx;		/* their indexes in the target list */
	Oid		   *sortOperators;	/* OIDs of operators to sort them by */
	Oid		   *collations;		/* OIDs of collations */
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */
    /* CDB */
	bool		noduplicates;   /* TRUE if sort should discard duplicates */
} Sort;

/* ----------------
 *		incremental sort node
 * ----------------
 */
typedef struct IncrementalSort
{
	Sort		sort;
	int			nPresortedCols; /* number of presorted columns */
} IncrementalSort;

/* ---------------
 *	 group node -
 *		Used for queries with GROUP BY (but no aggregates) specified.
 *		The input must be presorted according to the grouping columns.
 * ---------------
 */
typedef struct Group
{
	Plan		plan;
	int			numCols;		/* number of grouping columns */
	AttrNumber *grpColIdx;		/* their indexes in the target list */
	Oid		   *grpOperators;	/* equality operators to compare with */
	Oid		   *grpCollations;
} Group;

/* ---------------
 *		aggregate node
 *
 * An Agg node implements plain or grouped aggregation.  For grouped
 * aggregation, we can work with presorted input or unsorted input;
 * the latter strategy uses an internal hashtable.
 *
 * Notice the lack of any direct info about the aggregate functions to be
 * computed.  They are found by scanning the node's tlist and quals during
 * executor startup.  (It is possible that there are no aggregate functions;
 * this could happen if they get optimized away by constant-folding, or if
 * we are using the Agg node to implement hash-based grouping.)
 * ---------------
 */
typedef struct Agg
{
	Plan		plan;
	AggStrategy aggstrategy;	/* basic strategy, see nodes.h */
	AggSplit	aggsplit;		/* agg-splitting mode, see nodes.h */
	int			numCols;		/* number of grouping columns */
	AttrNumber *grpColIdx;		/* their indexes in the target list */
	bool		combineStates;	/* input tuples contain transition states */
	bool		finalizeAggs;	/* should we call the finalfn on agg states? */
	Oid		   *grpOperators;	/* equality operators to compare with */
	Oid		   *grpCollations;
	long		numGroups;		/* estimated number of groups in input */
	uint64		transitionSpace;	/* for pass-by-ref transition data */
	Bitmapset  *aggParams;		/* IDs of Params used in Aggref inputs */
	/* Note: planner provides numGroups & aggParams only in HASHED/MIXED case */
	List	   *groupingSets;	/* grouping sets to use */
	List	   *chain;			/* chained Agg/Sort nodes */

	/* Stream entries when out of memory instead of spilling to disk */
	bool		streaming;

	/* if input tuple has an AggExprId, save the tlist index */
	Index       agg_expr_id;
} Agg;

/* ---------------
 *		tuple split node
 *
 * A TupleSplit node implements tuple split in multiple DQAs MPP query.
 *
 * ---------------
 */
typedef struct TupleSplit
{
	Plan		plan;

	int			numCols;		    /* number of grouping columns */
	AttrNumber *grpColIdx;		    /* their indexes in the target list */

	List       *dqa_expr_lst;
} TupleSplit;

/* ----------------
 *		window aggregate node
 *
 * A WindowAgg node implements window functions over zero or more
 * ordering/framing specifications within a partition specification on
 * appropriately ordered input.
 *
 * For example, if there are window functions
 *
 *   over (partition by a,b orderby c) and
 *   over (partition by a,b order by c,d,e)
 *
 * then the input (outer plan) of the window node will be sorted by
 * (a,b,c,d,e) -- the common partition key (a,b) and the partial
 * ordering keys (c) and (d,e).
 *
 * A Window node contains no direct information about the window
 * functions it computes.  Those functions are found by scanning
 * the node's targetlist for WindowFunc nodes during executor startup.
 * There need not be any, but there's no good reason for the planner
 * to construct a WindowAgg node without at least one WindowFunc.
 *
 * A WindowFunc is related to its WindowAgg node by the fact that it is
 * contained by it.
 *
 * ----------------
 */
typedef struct WindowAgg
{
	Plan		plan;
	Index		winref;			/* ID referenced by window functions */
	int			partNumCols;	/* number of columns in partition clause */
	AttrNumber *partColIdx;		/* their indexes in the target list */
	Oid		   *partOperators;	/* equality operators for partition columns */
	Oid		   *partCollations; /* collations for partition columns */
	int			ordNumCols;		/* number of columns in ordering clause */
	AttrNumber *ordColIdx;		/* their indexes in the target list */
	Oid		   *ordOperators;	/* equality operators for ordering columns */
	Oid		   *ordCollations;	/* collations for ordering columns */

	/*
	 * GPDB: Information on the first ORDER BY column. This is different from
	 * simply taking the first element of the ordColIdx/ordOperators fields,
	 * because those arrays don't include any columns that are also present
	 * in the PARTITION BY. For example, in "OVER (PARTITION BY foo ORDER BY
	 * foo, bar)", ordColIdx/ordOperators would not include column 'foo'. But
	 * for computing with RANGE BETWEEN values correctly, we need the first
	 * actual ORDER BY column, even if it's redundant with the PARTITION BY.
	 * firstOrder* has that information. Also, we need a sort operator, not
	 * equality operator, here.
	 */
	AttrNumber	firstOrderCol;
	Oid			firstOrderCmpOperator; /* ordering op */
	bool		firstOrderNullsFirst;

	int			frameOptions;	/* frame_clause options, see WindowDef */
	Node	   *startOffset;	/* expression for starting bound, if any */
	Node	   *endOffset;		/* expression for ending bound, if any */
	/* these fields are used with RANGE offset PRECEDING/FOLLOWING: */
	Oid			startInRangeFunc;	/* in_range function for startOffset */
	Oid			endInRangeFunc; /* in_range function for endOffset */
	Oid			inRangeColl;	/* collation for in_range tests */
	bool		inRangeAsc;		/* use ASC sort order for in_range tests? */
	bool		inRangeNullsFirst;	/* nulls sort first for in_range tests? */
} WindowAgg;

/* ----------------
 *		unique node
 * ----------------
 */
typedef struct Unique
{
	Plan		plan;
	int			numCols;		/* number of columns to check for uniqueness */
	AttrNumber *uniqColIdx;		/* their indexes in the target list */
	Oid		   *uniqOperators;	/* equality operators to compare with */
	Oid		   *uniqCollations; /* collations for equality comparisons */
} Unique;

/* ------------
 *		gather node
 *
 * Note: rescan_param is the ID of a PARAM_EXEC parameter slot.  That slot
 * will never actually contain a value, but the Gather node must flag it as
 * having changed whenever it is rescanned.  The child parallel-aware scan
 * nodes are marked as depending on that parameter, so that the rescan
 * machinery is aware that their output is likely to change across rescans.
 * In some cases we don't need a rescan Param, so rescan_param is set to -1.
 * ------------
 */
typedef struct Gather
{
	Plan		plan;
	int			num_workers;	/* planned number of worker processes */
	int			rescan_param;	/* ID of Param that signals a rescan, or -1 */
	bool		single_copy;	/* don't execute plan more than once */
	bool		invisible;		/* suppress EXPLAIN display (for testing)? */
	Bitmapset  *initParam;		/* param id's of initplans which are referred
								 * at gather or one of it's child node */
} Gather;

/* ------------
 *		gather merge node
 * ------------
 */
typedef struct GatherMerge
{
	Plan		plan;
	int			num_workers;	/* planned number of worker processes */
	int			rescan_param;	/* ID of Param that signals a rescan, or -1 */
	/* remaining fields are just like the sort-key info in struct Sort */
	int			numCols;		/* number of sort-key columns */
	AttrNumber *sortColIdx;		/* their indexes in the target list */
	Oid		   *sortOperators;	/* OIDs of operators to sort them by */
	Oid		   *collations;		/* OIDs of collations */
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */
	Bitmapset  *initParam;		/* param id's of initplans which are referred
								 * at gather merge or one of it's child node */
} GatherMerge;

/* ----------------
 *		hash build node
 *
 * If the executor is supposed to try to apply skew join optimization, then
 * skewTable/skewColumn/skewInherit identify the outer relation's join key
 * column, from which the relevant MCV statistics can be fetched.
 * ----------------
 */
typedef struct Hash
{
	Plan		plan;

	/*
	 * List of expressions to be hashed for tuples from Hash's outer plan,
	 * needed to put them into the hashtable.
	 */
	List	   *hashkeys;		/* hash keys for the hashjoin condition */
	Oid			skewTable;		/* outer join key's table OID, or InvalidOid */
	AttrNumber	skewColumn;		/* outer join key's column #, or zero */
	bool		skewInherit;	/* is outer join rel an inheritance tree? */
	/* all other info is in the parent HashJoin node */
	double		rows_total;		/* estimate total rows if parallel_aware */
	bool		rescannable;            /* CDB: true => save rows for rescan */
	bool		sync_barrier;
} Hash;

/* ----------------
 *		runtime filter node
 * ----------------
 */
typedef struct RuntimeFilter
{
	Plan		plan;
} RuntimeFilter;

/* ----------------
 *		setop node
 * ----------------
 */
typedef struct SetOp
{
	Plan		plan;
	SetOpCmd	cmd;			/* what to do, see nodes.h */
	SetOpStrategy strategy;		/* how to do it, see nodes.h */
	int			numCols;		/* number of columns to check for
								 * duplicate-ness */
	AttrNumber *dupColIdx;		/* their indexes in the target list */
	Oid		   *dupOperators;	/* equality operators to compare with */
	Oid		   *dupCollations;
	AttrNumber	flagColIdx;		/* where is the flag column, if any */
	int			firstFlag;		/* flag value for first input relation */
	long		numGroups;		/* estimated number of groups in input */
} SetOp;

/* ----------------
 *		lock-rows node
 *
 * rowMarks identifies the rels to be locked by this node; it should be
 * a subset of the rowMarks listed in the top-level PlannedStmt.
 * epqParam is a Param that all scan nodes below this one must depend on.
 * It is used to force re-evaluation of the plan during EvalPlanQual.
 * ----------------
 */
typedef struct LockRows
{
	Plan		plan;
	List	   *rowMarks;		/* a list of PlanRowMark's */
	int			epqParam;		/* ID of Param for EvalPlanQual re-eval */
} LockRows;

/* ----------------
 *		limit node
 *
 * Note: as of Postgres 8.2, the offset and count expressions are expected
 * to yield int8, rather than int4 as before.
 * ----------------
 */
typedef struct Limit
{
	Plan		plan;
	Node	   *limitOffset;	/* OFFSET parameter, or NULL if none */
	Node	   *limitCount;		/* COUNT parameter, or NULL if none */
	LimitOption limitOption;	/* limit type */
	int			uniqNumCols;	/* number of columns to check for similarity  */
	AttrNumber *uniqColIdx;		/* their indexes in the target list */
	Oid		   *uniqOperators;	/* equality operators to compare with */
	Oid		   *uniqCollations; /* collations for equality comparisons */
} Limit;

/* -------------------------
 *		motion node structs
 * -------------------------
 */
typedef enum MotionType
{
	MOTIONTYPE_GATHER,		/* Send tuples from N senders to one receiver */
	MOTIONTYPE_GATHER_SINGLE, /* Execute subplan on N nodes, but only send the tuples from one */
	MOTIONTYPE_HASH,		/* Use hashing to select a segindex destination */
	MOTIONTYPE_BROADCAST,	/* Send tuples from one sender to a fixed set of segindexes */
	MOTIONTYPE_PARALLEL_BROADCAST, /*  */
	MOTIONTYPE_EXPLICIT,	/* Send tuples to the segment explicitly specified in their segid column */
	MOTIONTYPE_OUTER_QUERY	/* Gather or Broadcast to outer query's slice, don't know which one yet */
} MotionType;

/*
 * Motion Node
 *
 */
typedef struct Motion
{
	Plan		plan;

	MotionType  motionType;
	bool		sendSorted;			/* if true, output should be sorted */
	int			motionID;			/* required by AMS  */

	/* For Hash */
	List		*hashExprs;			/* list of hash expressions */
	Oid			*hashFuncs;			/* corresponding hash functions */
	int         numHashSegments;	/* the module number of the hash function */

	/* For Explicit */
	AttrNumber segidColIdx;			/* index of the segid column in the target list */

	/* The following field is only used when sendSorted == true */
	int			numSortCols;	/* number of sort-key columns */
	AttrNumber *sortColIdx;		/* their indexes in the target list */
	Oid		   *sortOperators;	/* OIDs of operators to sort them by */
	Oid		   *collations;		/* OIDs of collations */
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */

	/* sender slice info */
	PlanSlice  *senderSliceInfo;
} Motion;

/*
 * SplitUpdate Node
 *
 */
typedef struct SplitUpdate
{
	Plan		plan;
	AttrNumber	actionColIdx;		/* index of action column into the target list */
	AttrNumber	tupleoidColIdx;		/* index of tuple oid column into the target list */
	List		*insertColIdx;		/* list of columns to INSERT into the target list */
	List		*deleteColIdx;		/* list of columns to DELETE into the target list */

	/*
	 * Fields for calculating the target segment id.
	 *
	 * If the targetlist contains a 'gp_segment_id' field, these fields are
	 * used to compute the target segment id, for INSERT-action rows.
	 */
	int			numHashAttrs;
	AttrNumber *hashAttnos;
	Oid		   *hashFuncs;			/* corresponding hash functions */
	int			numHashSegments;	/* # of segs to use in hash computation */
} SplitUpdate;

/*
 * AssertOp Node
 *
 */
typedef struct AssertOp
{
	Plan 			plan;
	int				errcode;		/* SQL error code */
	List 			*errmessage;	/* error message */

} AssertOp;

/*
 * RowMarkType -
 *	  enums for types of row-marking operations
 *
 * The first four of these values represent different lock strengths that
 * we can take on tuples according to SELECT FOR [KEY] UPDATE/SHARE requests.
 * We support these on regular tables, as well as on foreign tables whose FDWs
 * report support for late locking.  For other foreign tables, any locking
 * that might be done for such requests must happen during the initial row
 * fetch; their FDWs provide no mechanism for going back to lock a row later.
 * This means that the semantics will be a bit different than for a local
 * table; in particular we are likely to lock more rows than would be locked
 * locally, since remote rows will be locked even if they then fail
 * locally-checked restriction or join quals.  However, the prospect of
 * doing a separate remote query to lock each selected row is usually pretty
 * unappealing, so early locking remains a credible design choice for FDWs.
 *
 * When doing UPDATE, DELETE, or SELECT FOR UPDATE/SHARE, we have to uniquely
 * identify all the source rows, not only those from the target relations, so
 * that we can perform EvalPlanQual rechecking at need.  For plain tables we
 * can just fetch the TID, much as for a target relation; this case is
 * represented by ROW_MARK_REFERENCE.  Otherwise (for example for VALUES or
 * FUNCTION scans) we have to copy the whole row value.  ROW_MARK_COPY is
 * pretty inefficient, since most of the time we'll never need the data; but
 * fortunately the overhead is usually not performance-critical in practice.
 * By default we use ROW_MARK_COPY for foreign tables, but if the FDW has
 * a concept of rowid it can request to use ROW_MARK_REFERENCE instead.
 * (Again, this probably doesn't make sense if a physical remote fetch is
 * needed, but for FDWs that map to local storage it might be credible.)
 */
typedef enum RowMarkType
{
	ROW_MARK_EXCLUSIVE,			/* obtain exclusive tuple lock */
	ROW_MARK_NOKEYEXCLUSIVE,	/* obtain no-key exclusive tuple lock */
	ROW_MARK_SHARE,				/* obtain shared tuple lock */
	ROW_MARK_KEYSHARE,			/* obtain keyshare tuple lock */
	ROW_MARK_REFERENCE,			/* just fetch the TID */
	ROW_MARK_COPY				/* physically copy the row value */
} RowMarkType;

#define RowMarkRequiresRowShareLock(marktype)  ((marktype) <= ROW_MARK_KEYSHARE)

/*
 * PlanRowMark -
 *	   plan-time representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * When doing UPDATE, DELETE, or SELECT FOR UPDATE/SHARE, we create a separate
 * PlanRowMark node for each non-target relation in the query.  Relations that
 * are not specified as FOR UPDATE/SHARE are marked ROW_MARK_REFERENCE (if
 * regular tables or supported foreign tables) or ROW_MARK_COPY (if not).
 *
 * Initially all PlanRowMarks have rti == prti and isParent == false.
 * When the planner discovers that a relation is the root of an inheritance
 * tree, it sets isParent true, and adds an additional PlanRowMark to the
 * list for each child relation (including the target rel itself in its role
 * as a child, if it is not a partitioned table).  Any non-leaf partitioned
 * child relations will also have entries with isParent = true.  The child
 * entries have rti == child rel's RT index and prti == top parent's RT index,
 * and can therefore be recognized as children by the fact that prti != rti.
 * The parent's allMarkTypes field gets the OR of (1<<markType) across all
 * its children (this definition allows children to use different markTypes).
 *
 * The planner also adds resjunk output columns to the plan that carry
 * information sufficient to identify the locked or fetched rows.  When
 * markType != ROW_MARK_COPY, these columns are named
 *		tableoid%u			OID of table
 *		ctid%u				TID of row
 * The tableoid column is only present for an inheritance hierarchy.
 * When markType == ROW_MARK_COPY, there is instead a single column named
 *		wholerow%u			whole-row value of relation
 * (An inheritance hierarchy could have all three resjunk output columns,
 * if some children use a different markType than others.)
 * In all three cases, %u represents the rowmark ID number (rowmarkId).
 * This number is unique within a plan tree, except that child relation
 * entries copy their parent's rowmarkId.  (Assigning unique numbers
 * means we needn't renumber rowmarkIds when flattening subqueries, which
 * would require finding and renaming the resjunk columns as well.)
 * Note this means that all tables in an inheritance hierarchy share the
 * same resjunk column names.
 */
typedef struct PlanRowMark
{
	NodeTag		type;
	Index		rti;			/* range table index of markable relation */
	Index		prti;			/* range table index of parent relation */
	Index		rowmarkId;		/* unique identifier for resjunk columns */
	RowMarkType markType;		/* see enum above */
	int			allMarkTypes;	/* OR of (1<<markType) for all children */
	LockClauseStrength strength;	/* LockingClause's strength, or LCS_NONE */
	LockWaitPolicy waitPolicy;	/* NOWAIT and SKIP LOCKED options */
	bool		isParent;		/* true if this is a "dummy" parent entry */
	bool        canOptSelectLockingClause; /* Whether can do some optimization on select with locking clause */
} PlanRowMark;


/*
 * Node types to represent partition pruning information.
 */

/*
 * PartitionPruneInfo - Details required to allow the executor to prune
 * partitions.
 *
 * Here we store mapping details to allow translation of a partitioned table's
 * index as returned by the partition pruning code into subplan indexes for
 * plan types which support arbitrary numbers of subplans, such as Append.
 * We also store various details to tell the executor when it should be
 * performing partition pruning.
 *
 * Each PartitionedRelPruneInfo describes the partitioning rules for a single
 * partitioned table (a/k/a level of partitioning).  Since a partitioning
 * hierarchy could contain multiple levels, we represent it by a List of
 * PartitionedRelPruneInfos, where the first entry represents the topmost
 * partitioned table and additional entries represent non-leaf child
 * partitions, ordered such that parents appear before their children.
 * Then, since an Append-type node could have multiple partitioning
 * hierarchies among its children, we have an unordered List of those Lists.
 *
 * prune_infos			List of Lists containing PartitionedRelPruneInfo nodes,
 *						one sublist per run-time-prunable partition hierarchy
 *						appearing in the parent plan node's subplans.
 * other_subplans		Indexes of any subplans that are not accounted for
 *						by any of the PartitionedRelPruneInfo nodes in
 *						"prune_infos".  These subplans must not be pruned.
 */
typedef struct PartitionPruneInfo
{
	NodeTag		type;
	List	   *prune_infos;
	Bitmapset  *other_subplans;
} PartitionPruneInfo;

/*
 * PartitionedRelPruneInfo - Details required to allow the executor to prune
 * partitions for a single partitioned table.
 *
 * subplan_map[] and subpart_map[] are indexed by partition index of the
 * partitioned table referenced by 'rtindex', the partition index being the
 * order that the partitions are defined in the table's PartitionDesc.  For a
 * leaf partition p, subplan_map[p] contains the zero-based index of the
 * partition's subplan in the parent plan's subplan list; it is -1 if the
 * partition is non-leaf or has been pruned.  For a non-leaf partition p,
 * subpart_map[p] contains the zero-based index of that sub-partition's
 * PartitionedRelPruneInfo in the hierarchy's PartitionedRelPruneInfo list;
 * it is -1 if the partition is a leaf or has been pruned.  Note that subplan
 * indexes, as stored in 'subplan_map', are global across the parent plan
 * node, but partition indexes are valid only within a particular hierarchy.
 * relid_map[p] contains the partition's OID, or 0 if the partition was pruned.
 */
typedef struct PartitionedRelPruneInfo
{
	NodeTag		type;
	Index		rtindex;		/* RT index of partition rel for this level */
	Bitmapset  *present_parts;	/* Indexes of all partitions which subplans or
								 * subparts are present for */
	int			nparts;			/* Length of the following arrays: */
	int		   *subplan_map;	/* subplan index by partition index, or -1 */
	int		   *subpart_map;	/* subpart index by partition index, or -1 */
	Oid		   *relid_map;		/* relation OID by partition index, or 0 */

	/*
	 * initial_pruning_steps shows how to prune during executor startup (i.e.,
	 * without use of any PARAM_EXEC Params); it is NIL if no startup pruning
	 * is required.  exec_pruning_steps shows how to prune with PARAM_EXEC
	 * Params; it is NIL if no per-scan pruning is required.
	 */
	List	   *initial_pruning_steps;	/* List of PartitionPruneStep */
	List	   *exec_pruning_steps; /* List of PartitionPruneStep */
	Bitmapset  *execparamids;	/* All PARAM_EXEC Param IDs in
								 * exec_pruning_steps */
} PartitionedRelPruneInfo;

/*
 * Abstract Node type for partition pruning steps (there are no concrete
 * Nodes of this type).
 *
 * step_id is the global identifier of the step within its pruning context.
 */
typedef struct PartitionPruneStep
{
	NodeTag		type;
	int			step_id;
} PartitionPruneStep;

/*
 * PartitionPruneStepOp - Information to prune using a set of mutually ANDed
 *							OpExpr clauses
 *
 * This contains information extracted from up to partnatts OpExpr clauses,
 * where partnatts is the number of partition key columns.  'opstrategy' is the
 * strategy of the operator in the clause matched to the last partition key.
 * 'exprs' contains expressions which comprise the lookup key to be passed to
 * the partition bound search function.  'cmpfns' contains the OIDs of
 * comparison functions used to compare aforementioned expressions with
 * partition bounds.  Both 'exprs' and 'cmpfns' contain the same number of
 * items, up to partnatts items.
 *
 * Once we find the offset of a partition bound using the lookup key, we
 * determine which partitions to include in the result based on the value of
 * 'opstrategy'.  For example, if it were equality, we'd return just the
 * partition that would contain that key or a set of partitions if the key
 * didn't consist of all partitioning columns.  For non-equality strategies,
 * we'd need to include other partitions as appropriate.
 *
 * 'nullkeys' is the set containing the offset of the partition keys (0 to
 * partnatts - 1) that were matched to an IS NULL clause.  This is only
 * considered for hash partitioning as we need to pass which keys are null
 * to the hash partition bound search function.  It is never possible to
 * have an expression be present in 'exprs' for a given partition key and
 * the corresponding bit set in 'nullkeys'.
 */
typedef struct PartitionPruneStepOp
{
	PartitionPruneStep step;

	StrategyNumber opstrategy;
	List	   *exprs;
	List	   *cmpfns;
	Bitmapset  *nullkeys;
} PartitionPruneStepOp;

/*
 * PartitionPruneStepCombine - Information to prune using a BoolExpr clause
 *
 * For BoolExpr clauses, we combine the set of partitions determined for each
 * of the argument clauses.
 */
typedef enum PartitionPruneCombineOp
{
	PARTPRUNE_COMBINE_UNION,
	PARTPRUNE_COMBINE_INTERSECT
} PartitionPruneCombineOp;

typedef struct PartitionPruneStepCombine
{
	PartitionPruneStep step;

	PartitionPruneCombineOp combineOp;
	List	   *source_stepids;
} PartitionPruneStepCombine;


/*
 * Plan invalidation info
 *
 * We track the objects on which a PlannedStmt depends in two ways:
 * relations are recorded as a simple list of OIDs, and everything else
 * is represented as a list of PlanInvalItems.  A PlanInvalItem is designed
 * to be used with the syscache invalidation mechanism, so it identifies a
 * system catalog entry by cache ID and hash value.
 */
typedef struct PlanInvalItem
{
	NodeTag		type;
	int			cacheId;		/* a syscache ID, see utils/syscache.h */
	uint32		hashValue;		/* hash value of object's cache lookup key */
} PlanInvalItem;

/* ----------------
 * PartitionSelector node
 *
 * PartitionSelector performs partition pruning based on rows seen on
 * the "other" side of a join. It performs partition pruning similar to
 * run-time partition pruning in an Append node, but it is performed based
 * on the rows seen, instead of executor params. The set of surviving
 * partitions is made available to the Append node, by storing it in a
 * special executor param, identified by 'paramid' field.
 * ----------------
 */
typedef struct PartitionSelector
{
	Plan		plan;

	struct PartitionPruneInfo *part_prune_info;
	int32		paramid;	/* result is stored here */

} PartitionSelector;

#endif							/* PLANNODES_H */
