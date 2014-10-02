# standard requires
require 'sql_partitioner/loader'
require 'sql_partitioner/lock_wait_timeout_handler'
require 'sql_partitioner/partition'
require 'sql_partitioner/time_unit_converter'
require 'sql_partitioner/sql_helper'
require 'sql_partitioner/base_partitions_manager'
require 'sql_partitioner/partitions_manager'

SqlPartitioner::Loader.require_or_skip('sql_partitioner/adapters/ar_adapter', 'ActiveRecord')
SqlPartitioner::Loader.require_or_skip('sql_partitioner/adapters/dm_adapter', 'DataMapper')



