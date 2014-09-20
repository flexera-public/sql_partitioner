# standard requires
require 'sql_partitioner/partition'
require 'sql_partitioner/partitions_fetcher'
require 'sql_partitioner/time_unit_manager'
require 'sql_partitioner/sql_helper'
require 'sql_partitioner/base_partitions_manager'
require 'sql_partitioner/partitions_manager'
require 'sql_partitioner/adv_partitions_manager'

# only require AR adapter if AR available
begin
  require 'sql_partitioner/adapters/ar_adapter'
rescue
  # had to wrap it this way as Gem.available? lies
end


