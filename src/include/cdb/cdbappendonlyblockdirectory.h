/*------------------------------------------------------------------------------
 *
 * cdbappendonlyblockdirectory.h
 *
 * Portions Copyright (c) 2009, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbappendonlyblockdirectory.h
 *
 *------------------------------------------------------------------------------
 */
#ifndef CDBAPPENDONLYBLOCKDIRECTORY_H
#define CDBAPPENDONLYBLOCKDIRECTORY_H

#include "access/aosegfiles.h"
#include "access/aocssegfiles.h"
#include "access/appendonlytid.h"
#include "access/skey.h"
#include "catalog/indexing.h"

extern int gp_blockdirectory_entry_min_range;
extern int gp_blockdirectory_minipage_size;

/*
 * In-memory equivalent of on-disk data structure MinipageEntry, used to
 * represent a block directory entry.
 */
typedef struct AppendOnlyBlockDirectoryEntry
{
	/*
	 * The range of blocks covered by the Block Directory entry, which is the
	 * continuous range [firstRowNum, lastRowNum]. There are no gaps (or holes)
	 * within this range. However, there may be gaps between successive block
	 * directory entries. For e.g. entry0 could have range [1,50] and entry1
	 * could have: [100,150]. The reason gaps arise between successive entries
	 * is that we allocate row numbers using the gp_fastsequence mechanism,
	 * which allocates blocks of row numbers of a pre-determined size (that may
	 * be larger than the number of blocks being inserted)
	 */
	struct range
	{
		int64		fileOffset;
		int64		firstRowNum;

		int64		afterFileOffset;
		int64		lastRowNum;
	} range;

} AppendOnlyBlockDirectoryEntry;

/*
 * The entry in the minipage.
 */
typedef struct MinipageEntry
{
	int64 firstRowNum;
	int64 fileOffset;
	int64 rowCount;
} MinipageEntry;

/*
 * Define a varlena type for a minipage.
 */
typedef struct Minipage
{
	/* Total length. Must be the first. */
	int32 _len;
	int32 version;
	uint32 nEntry;
	
	/* Varlena array */
	MinipageEntry entry[1];
} Minipage;

/*
 * Define the relevant info for a minipage for each
 * column group.
 */
typedef struct MinipagePerColumnGroup
{
	Minipage *minipage;
	uint32 numMinipageEntries;
	ItemPointerData tupleTid;
} MinipagePerColumnGroup;

/*
 * I don't know the ideal value here. But let us put approximate
 * 8 minipages per heap page.
 */
#define NUM_MINIPAGE_ENTRIES (((MaxHeapTupleSize)/8 - sizeof(HeapTupleHeaderData) - 64 * 3)\
							  / sizeof(MinipageEntry))

#define IsMinipageFull(minipagePerColumnGroup) \
	((minipagePerColumnGroup)->numMinipageEntries == (uint32) gp_blockdirectory_minipage_size)

/*
 * Define a structure for the append-only relation block directory.
 */
typedef struct AppendOnlyBlockDirectory
{
	Relation aoRel;
	Snapshot appendOnlyMetaDataSnapshot;
	Relation blkdirRel;
	Relation blkdirIdx;
	CatalogIndexState indinfo;
	int numColumnGroups;
	bool isAOCol;
	bool *proj; /* projected columns, used only if isAOCol = TRUE */

	MemoryContext memoryContext;
	
	int				totalSegfiles;
	FileSegInfo 	**segmentFileInfo;

	/*
	 * Current segment file number.
	 */
	int currentSegmentFileNum;
	FileSegInfo *currentSegmentFileInfo;

	/*
	 * Last minipage that contains an array of MinipageEntries.
	 */
	MinipagePerColumnGroup *minipages;

	/*
	 * Some temporary space to help form tuples to be inserted into
	 * the block directory, and to help the index scan.
	 */
	Datum *values;
	bool *nulls;
	int numScanKeys;
	ScanKey scanKeys;
	StrategyNumber *strategyNumbers;

}	AppendOnlyBlockDirectory;


typedef struct CurrentBlock
{
	/*
	 * Current cached block directory entry.
	 * FIXME: At times, we rely upon the values in this struct to be valid even
	 * when AOFetchBlockMetadata->valid = false. This indicates that this should
	 * live elsewhere.
	 */
	AppendOnlyBlockDirectoryEntry blockDirectoryEntry;

	bool have;

	int64 fileOffset;
	
	int32 overallBlockLen;
	
	int64 firstRowNum;
	int64 lastRowNum;
	
	bool isCompressed;
	bool isLargeContent;
	
	bool		gotContents;
} CurrentBlock;

typedef struct CurrentSegmentFile
{
	bool isOpen;
	
	int num;
	
	int64 logicalEof;
} CurrentSegmentFile;

typedef struct AppendOnlyBlockDirectorySeqScan {
	AppendOnlyBlockDirectory blkdir;
	SysScanDesc sysScan;
} AppendOnlyBlockDirectorySeqScan;

extern void AppendOnlyBlockDirectoryEntry_GetBeginRange(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							*fileOffset,
	int64							*firstRowNum);
extern void AppendOnlyBlockDirectoryEntry_GetEndRange(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							*afterFileOffset,
	int64							*lastRowNum);
extern bool AppendOnlyBlockDirectoryEntry_RangeHasRow(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							checkRowNum);
extern bool AppendOnlyBlockDirectory_GetEntry(
	AppendOnlyBlockDirectory		*blockDirectory,
	AOTupleId 						*aoTupleId,
	int                             columnGroupNo,
	AppendOnlyBlockDirectoryEntry	*directoryEntry);
extern bool AppendOnlyBlockDirectory_CoversTuple(
	AppendOnlyBlockDirectory		*blockDirectory,
	AOTupleId 						*aoTupleId);
extern void AppendOnlyBlockDirectory_Init_forInsert(
	AppendOnlyBlockDirectory *blockDirectory,
	Snapshot appendOnlyMetaDataSnapshot,
	FileSegInfo *segmentFileInfo,
	int64 lastSequence,
	Relation aoRel,
	int segno,
	int numColumnGroups,
	bool isAOCol);
extern void AppendOnlyBlockDirectory_Init_forSearch(
	AppendOnlyBlockDirectory *blockDirectory,
	Snapshot appendOnlyMetaDataSnapshot,
	FileSegInfo **segmentFileInfo,
	int totalSegfiles,
	Relation aoRel,
	int numColumnGroups,
	bool isAOCol,
	bool *proj);
extern void AppendOnlyBlockDirectory_Init_addCol(
	AppendOnlyBlockDirectory *blockDirectory,
	Snapshot appendOnlyMetaDataSnapshot,
	FileSegInfo *segmentFileInfo,
	Relation aoRel,
	int segno,
	int numColumnGroups,
	bool isAOCol);
extern bool AppendOnlyBlockDirectory_InsertEntry(
	AppendOnlyBlockDirectory *blockDirectory,
	int columnGroupNo,
	int64 firstRowNum,
	int64 fileOffset,
	int64 rowCount,
	bool addColAction);
extern bool AppendOnlyBlockDirectory_addCol_InsertEntry(
	AppendOnlyBlockDirectory *blockDirectory,
	int columnGroupNo,
	int64 firstRowNum,
	int64 fileOffset,
	int64 rowCount);
extern bool AppendOnlyBlockDirectory_DeleteEntry(
	AppendOnlyBlockDirectory *blockDirectory,
	AOTupleId *aoTupleId);
extern bool AppendOnlyBlockDirectory_DeleteEntryForUpdate(
	AppendOnlyBlockDirectory *visibilityBlockDirectory,
	AppendOnlyBlockDirectory *insertBlockDirectory,
	AOTupleId* aoTupleId);
extern void AppendOnlyBlockDirectory_End_forInsert(
	AppendOnlyBlockDirectory *blockDirectory);
extern void AppendOnlyBlockDirectory_End_forSearch(
	AppendOnlyBlockDirectory *blockDirectory);
extern void AppendOnlyBlockDirectory_End_addCol(
	AppendOnlyBlockDirectory *blockDirectory);
extern void AppendOnlyBlockDirectory_DeleteSegmentFile(
	Relation aoRel,
		Snapshot snapshot,
		int segno,
		int columnGroupNo);

extern void AppendOnlyBlockDirectory_InsertPlaceholder(AppendOnlyBlockDirectory *blockDirectory,
												  int64 firstRowNum,
												  int64 fileOffset,
												  int columnGroupNo);

static inline uint32
minipage_size(uint32 nEntry)
{
	return offsetof(Minipage, entry) + sizeof(MinipageEntry) * nEntry;
}

/*
 * copy_out_minipage
 *
 * Copy out the minipage content from a deformed tuple.
 */
static inline void
copy_out_minipage(MinipagePerColumnGroup *minipageInfo,
				  Datum minipage_value,
				  bool minipage_isnull)
{
	struct varlena *value;
	struct varlena *detoast_value;

	Assert(!minipage_isnull);

	value = (struct varlena *)
		DatumGetPointer(minipage_value);
	detoast_value = pg_detoast_datum(value);
	Assert(VARSIZE(detoast_value) <= minipage_size(NUM_MINIPAGE_ENTRIES));

	memcpy(minipageInfo->minipage, detoast_value, VARSIZE(detoast_value));
	if (detoast_value != value)
		pfree(detoast_value);

	Assert(minipageInfo->minipage->nEntry <= NUM_MINIPAGE_ENTRIES);

	minipageInfo->numMinipageEntries = minipageInfo->minipage->nEntry;
}

#endif
