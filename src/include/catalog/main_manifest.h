/*-------------------------------------------------------------------------
 *
 * main_manifest.h
 *	  save all storage manifest info
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/main_manifest.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAIN_MANIFEST_H
#define MAIN_MANIFEST_H

#include "catalog/genbki.h"
#include "catalog/main_manifest_d.h"

/* ----------------
 *		main_manifest definition.  cpp turns this into
 *		typedef struct FormData_main_manifest
 * ----------------
 */
CATALOG(main_manifest,9004,ManifestRelationId)
{
	RelFileNodeId	relnode;
	text			path;
} FormData_main_manifest;

typedef FormData_main_manifest *Form_main_manifest;

DECLARE_INDEX(main_manifest_relnode_index, 7606, on main_manifest using btree(relnode int8_ops));
#define MainManifestRelnodeIndexId  7606

extern void RemoveMainManifestByRelid(Oid relid);

#endif /* MAIN_MANIFEST.h */
