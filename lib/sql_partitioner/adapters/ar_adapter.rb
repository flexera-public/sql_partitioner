require_relative "base_adapter"

module SqlPartitioner

  # Adapter wrapping an Active Record Connection
  class ARAdapter < BaseAdapter
    def initialize(connection)
      @connection = connection
    end

    def select(*args)
      result = []
      strukt = nil
      
      sanitized_sql = ActiveRecord::Base.send(:sanitize_sql_array, args)
      conn_result = @connection.send(:select, sanitized_sql)
      conn_result.each do |h|
        strukt ||= Struct.new(*h.keys.map{ |k| k.downcase.to_sym})
        result << strukt.new(*h.values)
      end
      result.size == 1 ? result[0] : result
    end

    def execute(*args)
      sanitized_sql = ActiveRecord::Base.send(:sanitize_sql_array, args)
      @connection.execute(sanitized_sql)
    end

    def schema_name
      @connection.current_database 
    end
  end

end