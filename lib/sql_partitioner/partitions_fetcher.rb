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

    # helper to format the partition_info into tabular form
    # @param [Array] array of partition info Structs
    # @return [String] formatted partitions in tabular form
    def self.format_partition_info(partitions)
      padding = PARTITION_INFO_ATTRS.map do |attribute|
                  max_length = partitions.map do |partition|
                    partition.send(attribute).to_s.length
                  end.max
                  [attribute.to_s.length, max_length].max + 3
                end
      header = PARTITION_INFO_ATTRS.map.with_index do |attribute, index|
                 attribute.to_s.ljust(padding[index])
               end.join
      body = partitions.map do |partition|
               PARTITION_INFO_ATTRS.map.with_index do |attribute, index|
                 partition.send(attribute).to_s.ljust(padding[index])
               end.join
             end.join("\n")
      seperator = ''.ljust(padding.inject(&:+),'-')
      [seperator, header, seperator, body, seperator].join("\n")
    end

    def with_lock_wait_timeout(timeout, &block)
      lock_wait_timeout_before = adapter.select("SELECT @@local.lock_wait_timeout").first
      adapter.execute("SET @@local.lock_wait_timeout = #{timeout}")
      begin
        return block.call
      ensure
        adapter.execute("SET @@local.lock_wait_timeout = #{lock_wait_timeout_before}")
      end
    end

    # logs the formatted partition info from information schema
    # @return [Boolean] true
    def display_partition_info
      partition_info = Partition.all(adapter, table_name)
      log "\n#{self.class.format_partition_info(partition_info)}", false
      true
    end

    # get all partitions that does not have timestamp as 'FUTURE_PARTITION_VALUE'
    def non_future_partitions(partition_info = nil)
      partition_info ||= Partition.all(adapter, table_name)
      partition_info.reject { |p| future_partition?(p) }
    end

    # fetch the latest partition that is not a future partition i.e.(value
    #  is not FUTURE_PARTITION_VALUE)
    # @param [Array] partition_info Array of partition info structs. if nil
    #                partition info is fetched from db
    # @return [Struct or NilClass] partition with maximum timestamp value
    def fetch_latest_partition(partition_info = nil)
      non_future_partitions(partition_info).max_by{ |p| p.partition_timestamp }
    end

    #fetch the partition which is currently active. i.e  holds the records
    # generated now
    def fetch_current_partition(partition_info = nil)
      non_future_partitions(partition_info).select do |p|
        p.partition_timestamp > self.current_timestamp
      end.min_by { |p| p.partition_timestamp }
    end

    #fetch the partition with oldest timestamp
    def fetch_oldest_partition(partition_info = nil)
      non_future_partitions(partition_info).min_by { |p| p.partition_timestamp }
    end

    def future_partition?(partition)
      partition.partition_timestamp == FUTURE_PARTITION_VALUE
    end

  end
end
