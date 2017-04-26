--
-- MULTI_METADATA_ACCESS
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1360000;

CREATE USER no_access;
SET ROLE no_access;

-- list relations in the citus extension without sufficient privileges
SELECT pg_class.oid::regclass
FROM pg_class
    JOIN pg_namespace nsp ON (pg_class.relnamespace = nsp.oid)
    JOIN pg_depend dep ON(objid = pg_class.oid)
    JOIN pg_extension ext ON (ext.oid = dep.refobjid)
WHERE
    refclassid = 'pg_extension'::regclass
    AND classid ='pg_class'::regclass
    AND ext.extname = 'citus'
    AND nsp.nspname = 'pg_catalog'
    AND NOT has_table_privilege(pg_class.oid, 'select');


RESET role;
DROP USER no_access;
