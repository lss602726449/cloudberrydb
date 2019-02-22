--
-- Validate GPDB can create unique index on a table created in utility mode
--
-- NOTICE: we must connect to master in utility mode because the oid of table is
-- preassigned in QD, if we create a table in utility mode in QE, the oid might
-- conflict with preassigned oid.
-1U: create table utilitymode_primary_key_tab (c1 int);
-1U: create unique index idx_utilitymode_c1 on utilitymode_primary_key_tab (c1);
-1U: drop table utilitymode_primary_key_tab;
