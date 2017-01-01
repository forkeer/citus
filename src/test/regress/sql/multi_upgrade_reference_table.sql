--
-- MULTI_UPGRADE_REFERENCE_TABLE
--
-- Tests around upgrade_reference_table UDF
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1360000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1360000;

-- test with not distributed table
CREATE TABLE upgrade_reference_table_append(column1 int);
SELECT upgrade_reference_table('upgrade_reference_table_append');

-- test with append distributed table
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('upgrade_reference_table_append', 'column1', 'append');
SELECT upgrade_reference_table('upgrade_reference_table_append');

-- test with table which has more than one shard
CREATE TABLE upgrade_reference_table_multiple_shard(column1 int);
SELECT create_distributed_table('upgrade_reference_table_multiple_shard', 'column1');
SELECT upgrade_reference_table('upgrade_reference_table_multiple_shard');

-- test with table with foreign keys
SET citus.shard_count TO 1;
CREATE TABLE upgrade_reference_table_referenced(column1 int PRIMARY KEY);
SELECT create_distributed_table('upgrade_reference_table_referenced', 'column1');

CREATE TABLE upgrade_reference_table_referencing(column1 int REFERENCES upgrade_reference_table_referenced(column1));
SELECT create_distributed_table('upgrade_reference_table_referencing', 'column1');

SELECT upgrade_reference_table('upgrade_reference_table_referenced');
SELECT upgrade_reference_table('upgrade_reference_table_referencing');

-- test with no healthy placements
CREATE TABLE upgrade_reference_table_unhealthy(column1 int);
SELECT create_distributed_table('upgrade_reference_table_unhealthy', 'column1');
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1360006;
SELECT upgrade_reference_table('upgrade_reference_table_unhealthy');

-- test valid cases, shard exists at one worker
CREATE TABLE upgrade_reference_table_one_worker(column1 int);
SELECT create_distributed_table('upgrade_reference_table_one_worker', 'column1');

-- situation before upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass);

SELECT upgrade_reference_table('upgrade_reference_table_one_worker');

-- situation after upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass);

-- test valid cases, shard exists at both workers but one is unhealthy
SET citus.shard_replication_factor TO 2;
CREATE TABLE upgrade_reference_table_one_unhealthy(column1 int);
SELECT create_distributed_table('upgrade_reference_table_one_unhealthy', 'column1');
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1360008 AND nodeport = :worker_1_port;

-- situation before upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass);

SELECT upgrade_reference_table('upgrade_reference_table_one_unhealthy');

-- situation after upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass);

-- test valid cases, shard exists at both workers and both are healthy
CREATE TABLE upgrade_reference_table_both_healthy(column1 int);
SELECT create_distributed_table('upgrade_reference_table_both_healthy', 'column1');

-- situation before upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass);

SELECT upgrade_reference_table('upgrade_reference_table_both_healthy');

-- situation after upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass);

-- test valid cases, do it in transaction and ROLLBACK
SET citus.shard_replication_factor TO 1;
CREATE TABLE upgrade_reference_table_transaction_rollback(column1 int);
SELECT create_distributed_table('upgrade_reference_table_transaction_rollback', 'column1');

-- situation before upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass);

BEGIN;
SELECT upgrade_reference_table('upgrade_reference_table_transaction_rollback');
ROLLBACK;

-- situation after upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass);

-- verify that shard is not replicated to other worker
\c - - - :worker_2_port
\d upgrade_reference_table_transaction_rollback_*
\c - - - :master_port

-- test valid cases, do it in transaction and COMMIT
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;
CREATE TABLE upgrade_reference_table_transaction_commit(column1 int);
SELECT create_distributed_table('upgrade_reference_table_transaction_commit', 'column1');

-- situation before upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass);

BEGIN;
SELECT upgrade_reference_table('upgrade_reference_table_transaction_commit');
COMMIT;

-- situation after upgrade_reference_table
SELECT * FROM pg_dist_partition WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass;
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass;
SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass);
SELECT *
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass);

-- verify that shard is replicated to other worker
\c - - - :worker_2_port
\d upgrade_reference_table_transaction_commit_*
\c - - - :master_port


-- drop used tables to clean the workspace
DROP TABLE upgrade_reference_table_append;
DROP TABLE upgrade_reference_table_multiple_shard;
DROP TABLE upgrade_reference_table_referencing;
DROP TABLE upgrade_reference_table_referenced;
DROP TABLE upgrade_reference_table_unhealthy;
DROP TABLE upgrade_reference_table_one_worker;
DROP TABLE upgrade_reference_table_one_unhealthy;
DROP TABLE upgrade_reference_table_both_healthy;
DROP TABLE upgrade_reference_table_transaction_rollback;
DROP TABLE upgrade_reference_table_transaction_commit;
