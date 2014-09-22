
# standard requires
require 'sql_partitioner/sql_helper'
require 'sql_partitioner/partitions_manager'
require 'sql_partitioner/adv_partitions_manager'

def self.require_or_skip(path, category)  
  begin
    require path
  rescue LoadError => e
    puts "SKIPPING #{category} functionality due to #{e.message}"  
  end
end

require_or_skip('sql_partitioner/adapters/ar_adapter', 'ActiveRecord')
require_or_skip('sql_partitioner/partitions_manager_as', 'ActiveSupport')


