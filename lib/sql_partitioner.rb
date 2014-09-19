# method for testing gem availability across Ruby 1.87 and 2+
def gem_available?(name)
   Gem::Specification.find_by_name(name)
rescue Gem::LoadError
   false
rescue
   Gem.available?(name)
end

# standard requires
require 'sql_partitioner/sql_helper'
require 'sql_partitioner/partitions_manager'
require 'sql_partitioner/adv_partitions_manager'

# only require AR adapter if AR available
require 'sql_partitioner/adapters/ar_adapter' if gem_available?("activerecord")



