--
-- This file is a TEMPLATE of the schema of the balboa-http Cassandra database.
-- A couple of items need to be replaced with legal values in order for this
-- template to be usable.
--
-- In the very first statement, replace:
--     {{datacenter-name}}
--     {{replication-factor}}
-- Reasonable defaults are:
--     datacenter1
--     1
--
-- A schema definition of the keyspace and it's contents used by balboa for
-- recording metric data.
--
-- This file only works with the `cassandra-cli` command line utility. That
-- utility is deprecated and due to be removed from Cassandra in 2.2. This
-- schema will have to be ported to cqlsh to upgrade to that release.
--
create keyspace Metrics2012
  with placement_strategy = 'NetworkTopologyStrategy'
  and strategy_options = { {{datacenter-name}} : {{replication-factor}} }
  and durable_writes = true;

use Metrics2012;

create column family daily_absolute
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'LongType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family daily_aggregate
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'CounterColumnType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family hourly_absolute
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'LongType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family hourly_aggregate
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'CounterColumnType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family fifteen_minute_absolute
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'LongType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family fifteen_minute_aggregate
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'CounterColumnType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family monthly_absolute
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'LongType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family monthly_aggregate
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'CounterColumnType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family yearly_absolute
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'LongType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family yearly_aggregate
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'CounterColumnType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

