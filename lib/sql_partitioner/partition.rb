module SqlPartitioner
  class PartitionCollection < Array

    # selects all partitions that hold records older than the timestamp provided
    # @param [Fixnum] timestamp
    # @return [Array<Partition>] partitions that hold data older than given timestamp
    def older_than_timestamp(timestamp)
      non_future_partitions.select do |p|
        timestamp > p.timestamp
      end
    end

    # selects all partitions that hold records newer than the timestamp provided
    # @param [Fixnum] timestamp
    # @return [Array<Partition>] partitions that hold data newer than given timestamp
    def newer_than_timestamp(timestamp)
      non_future_partitions.select do |p|
        timestamp <= p.timestamp
      end
    end

    # fetch the partition which is currently active. i.e. holds the records generated now
    # @param [Fixnum] current_timestamp
    # @return [Partition,NilClass]
    def current_partition(current_timestamp)
      non_future_partitions.select do |p|
        p.timestamp > current_timestamp
      end.min_by { |p| p.timestamp }
    end

    # @return [Array<Partition>] all partitions that do not have timestamp as `FUTURE_PARTITION_VALUE`
    def non_future_partitions
      self.reject { |p| p.future_partition? }
    end

    # fetch the latest partition that is not a future partition i.e. (value
    #  is not `FUTURE_PARTITION_VALUE`)
    # @return [Partition,NilClass] partition with maximum timestamp value
    def latest_partition
      non_future_partitions.max_by{ |p| p.timestamp }
    end

    # @return [Partition,NilClass] the partition with oldest timestamp
    def oldest_partition
      non_future_partitions.min_by { |p| p.timestamp }
    end
  end

  class Partition
    FUTURE_PARTITION_NAME  = 'future'
    FUTURE_PARTITION_VALUE = 'MAXVALUE'
    TO_LOG_ATTRIBUTES_SORT_ORDER = [
      :ordinal_position, :name, :timestamp, :table_rows, :data_length, :index_length
    ]

    def initialize(partition_data)
      @partition_data = partition_data
    end

    # @return [PartitionCollection]
    def self.all(adapter, table_name)
      select_sql = SqlPartitioner::SQL.partition_info
      result = adapter.select(select_sql, adapter.schema_name, table_name).reject{|r| r.partition_description.nil? }

      partition_collection = PartitionCollection.new
      result.each{ |r| partition_collection << self.new(r) }

      partition_collection
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

    def attributes
      {
        :ordinal_position  => ordinal_position,
        :name              => name,
        :timestamp         => timestamp,
        :table_rows        => table_rows,
        :data_length       => data_length,
        :index_length      => index_length
      }
    end

    # logs the formatted partition info from information schema
    # @param [Array] array of partition objects
    # @return [String] formatted partitions in tabular form
    def self.to_log(partitions)
      return "none" if partitions.empty?

      padding = TO_LOG_ATTRIBUTES_SORT_ORDER.map do |attribute|
                  max_length = partitions.map do |partition|
                    partition.send(attribute).to_s.length
                  end.max
                  [attribute.to_s.length, max_length].max + 3
                end

      header = TO_LOG_ATTRIBUTES_SORT_ORDER.each_with_index.map do |attribute, index|
                  attribute.to_s.ljust(padding[index])
                end.join

      body = partitions.map do |partition|
               TO_LOG_ATTRIBUTES_SORT_ORDER.each_with_index.map do |attribute, index|
                 partition.send(attribute).to_s.ljust(padding[index])
               end.join
             end.join("\n")

      separator = ''.ljust(padding.inject(&:+),'-')

      [separator, header, separator, body, separator].join("\n")
    end

  end
end
