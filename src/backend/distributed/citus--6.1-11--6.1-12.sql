/* citus--6.1-11--6.1-12.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION upgrade_reference_table(table_name regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$upgrade_reference_table$$;
COMMENT ON FUNCTION upgrade_reference_table(table_name regclass)
    IS 'upgrades an existing implicit reference table to an explicit one';
    
RESET search_path;
