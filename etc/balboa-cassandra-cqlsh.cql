--
-- This file is a TEMPLATE of the schema of the balboa-http Cassandra database.
-- A couple of items need to be replaced with legal values in order for this
-- template to be usable.
--
-- In the very first statement, replace:
--     {{datacenter-name}}
--     {{replication-factor}}
-- Reasonable defaults are:
--     'datacenter1'
--     1
--
-- A schema definition of the keyspace and it's contents used by balboa for
-- recording metric data.
--
-- This file only works with the `cqlsh` utility. Refer to `balboa-cassandra.cql` if you need to run these commands
-- while being compatible with `cassandra-cli`.
--

CREATE KEYSPACE Metrics2012
WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy',
    {{datacenter-name}} : {{replication-factor}}
  }
AND DURABLE_WRITES = true;

use Metrics2012;

-- caching = 'KEYS_ONLY' is equivalent to caching = { 'keys' : 'ALL'}
-- https://docs.huihoo.com/apache/cassandra/datastax/CQL-3.1-for-Cassandra-2.0-and-2.1.pdf
CREATE TABLE daily_absolute(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE daily_aggregate(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE hourly_absolute(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE hourly_aggregate(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE fifteen_minute_absolute(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE fifteen_minute_aggregate(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE monthly_absolute(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE monthly_aggregate(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE yearly_absolute(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };

CREATE TABLE yearly_aggregate(
  id uuid primary key
) WITH read_repair_chance = 0.1
    AND dclocal_read_repair_chance = 0.0
    AND gc_grace_seconds = 864000
    AND COMPACTION = {
    'class' : 'SizeTieredCompactionStrategy',
    'min_threshold' : 4,
    'max_threshold' : 32
    }
    AND COMPRESSION = {
    'sstable_compression' : 'SnappyCompressor'
    }
    AND caching = {
    'keys' : 'ALL'
    };
