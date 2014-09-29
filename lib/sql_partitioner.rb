# standard requires
require 'sql_partitioner/lock_wait_timeout_handler'
require 'sql_partitioner/partition'
require 'sql_partitioner/time_unit_manager'
require 'sql_partitioner/sql_helper'
require 'sql_partitioner/base_partitions_manager'
require 'sql_partitioner/partitions_manager'

def self.require_or_skip(path, required_constant)
  if Object.const_defined?(required_constant)
    require path
  else
    puts "{sql_partitioner} SKIPPING `require '#{path}'` because #{required_constant} is not defined."
  end
end

require_or_skip('sql_partitioner/adapters/ar_adapter', 'ActiveRecord')
require_or_skip('sql_partitioner/adapters/dm_adapter', 'DataMapper')



