/*-------------------------------------------------------------------------
 *
 * main_manifest.c
 *	  save all storage manifest info.
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 *
 * IDENTIFICATION
 *		src/backend/catalog/main_manifest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/main_manifest.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * RemoveMainManifestByRelnode
 *      Remove the main manifest record for the relnode.
 */
void
RemoveMainManifestByRelid(Oid relid)
{
    Relation    main_manifest;
    HeapTuple   tuple;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];
    HeapTuple	tp;
    RelFileNodeId relnode;
    
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

    if(HeapTupleIsValid(tp))
    {
        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

		relnode = reltup->relfilenode;
		ReleaseSysCache(tp);
    }
    else
        elog(ERROR, "cache lookup failed for relid %u", relid);

    /*
     * FIXME: relnode is uint64, but we treat it as int64. Although relnode 
     * will not exceed the range of int64.
     */
    main_manifest = table_open(ManifestRelationId, RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_main_manifest_relnode, BTEqualStrategyNumber,
                F_INT8EQ, Int64GetDatum(relnode));

    scanDescriptor = systable_beginscan(main_manifest, MainManifestRelnodeIndexId,
                                        true, NULL, 1, scanKey);

    if(HeapTupleIsValid(tuple = systable_getnext(scanDescriptor)))
    {
        CatalogTupleDelete(main_manifest, &tuple->t_self);
    }

    systable_endscan(scanDescriptor);
    table_close(main_manifest, RowExclusiveLock);
}
