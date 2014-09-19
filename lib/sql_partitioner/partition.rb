module SqlPartitioner
  class Partition
    FUTURE_PARTITION_NAME = 'future'

    FUTURE_PARTITION_VALUE = 'MAXVALUE'

    PARTITION_INFO_ATTRS = [:ordinal_position,
                            :partition_name,
                            :partition_timestamp,
                            :table_rows,
                            :data_length,
                            :index_length]

    def initialize(partition_data)
      @partition_data = partition_data
    end

    def ordinal_position
      @partition_data.partition_ordinal_position
    end

    def name
      @partition_data.partition_name
    end

    def timestamp
      if @partition_data.partition_description == FUTURE_PARTITION_VALUE
        FUTURE_PARTITION_VALUE
      else
        @partition_data.partition_description.to_i
      end
    end

    def table_rows
      @partition_data.table_rows
    end

    def data_length
      @partition_data.data_length
    end

    def index_length
      @partition_data.index_length
    end

    def future_partition?
      self.timestamp == FUTURE_PARTITION_VALUE
    end

  end
end
