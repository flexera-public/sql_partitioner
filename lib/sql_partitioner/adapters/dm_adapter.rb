require File.expand_path("./base_adapter", File.dirname(__FILE__))
require "data_mapper"

module SqlPartitioner

  # Adapter wrapping an Active Record Connection
  class DMAdapter < BaseAdapter
    def initialize(dm_adapter)
      @dm_adapter = dm_adapter
    end

    def select(*args)
      @dm_adapter.select(*args)
    end

    def execute(*args)
      @dm_adapter.execute(*args)
    end

    def schema_name
      @dm_adapter.schema_name
    end
  end

end