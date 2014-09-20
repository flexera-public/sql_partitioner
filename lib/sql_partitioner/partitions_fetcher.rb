module SqlPartitioner
  class PartitionsFetcher

    attr_accessor :adapter, :table_name, :current_timestamp

    FUTURE_PARTITION_NAME = 'future'

    FUTURE_PARTITION_VALUE = 'MAXVALUE'


    PARTITION_INFO_ATTRS = [:ordinal_position,
                            :partition_name,
                            :partition_timestamp,
                            :table_rows,
                            :data_length,
                            :index_length]

    def initialize(adapter, current_timestamp, table_name)
      @adapter           = adapter
      @current_timestamp = current_timestamp
    end

    def fetch_partition_info_from_db
      SqlPartitioner::Partition.all(adapter, "events")
    end

    # get all partitions that does not have timestamp as 'FUTURE_PARTITION_VALUE'
    def non_future_partitions(partitions = nil)
      partitions ||= Partition.all(adapter, table_name)
      partitions.reject { |p| future_partition?(p) }
    end

    # fetch the latest partition that is not a future partition i.e.(value
    #  is not FUTURE_PARTITION_VALUE)
    # @param [Array] partition_info Array of partition info structs. if nil
    #                partition info is fetched from db
    # @return [Struct or NilClass] partition with maximum timestamp value
    def fetch_latest_partition(partitions = nil)
      non_future_partitions(partitions).max_by{ |p| p.timestamp }
    end

    #fetch the partition which is currently active. i.e  holds the records
    # generated now
    def fetch_current_partition(partitions = nil)
      non_future_partitions(partitions).select do |p|
        p.timestamp > self.current_timestamp
      end.min_by { |p| p.timestamp }
    end

    #fetch the partition with oldest timestamp
    def fetch_oldest_partition(partitions = nil)
      non_future_partitions(partitions).min_by { |p| p.partition_timestamp }
    end

    def future_partition?(partition)
      partition.timestamp == FUTURE_PARTITION_VALUE
    end

  end
end
