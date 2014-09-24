module SqlPartitioner
  class PartitionCollection < Array

    # fetch all partitions from information schema or input that hold records
    # older than the timestamp provided
    #
    # @param [Fixnum] timestamp
    # @param [Array] partition_info Array of partition info structs. if nil
    #                partition info is fetched from db
    # @return [Array] Array of partition name(String) that hold data older
    #                 than given timestamp
    def older_than_timestamp(timestamp)
      non_future_partitions.select do |p|
        timestamp > p.timestamp
      end
    end

    #fetch the partition which is currently active. i.e  holds the records
    # generated now
    def current_partition(current_timestamp)
      non_future_partitions.select do |p|
        p.timestamp > current_timestamp
      end.min_by { |p| p.timestamp }
    end

    # get all partitions that does not have timestamp as 'FUTURE_PARTITION_VALUE'
    def non_future_partitions
      self.reject { |p| p.future_partition? }
    end

    # fetch the latest partition that is not a future partition i.e.(value
    #  is not FUTURE_PARTITION_VALUE)
    # @param [Array] partition_info Array of partition info structs. if nil
    #                partition info is fetched from db
    # @return [Struct or NilClass] partition with maximum timestamp value
    def latest_partition
      non_future_partitions.max_by{ |p| p.timestamp }
    end

    #fetch the partition with oldest timestamp
    def oldest_partition
      non_future_partitions.min_by { |p| p.timestamp }
    end
  end

  class Partition
    FUTURE_PARTITION_NAME  = 'future'
    FUTURE_PARTITION_VALUE = 'MAXVALUE'

    def initialize(partition_data)
      @partition_data = partition_data
    end

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

      partition_info_attrs = partitions.first.keys

      padding = partition_info_attrs.map do |attribute|
                  max_length = partitions.map do |partition|
                    partition.send(attribute).to_s.length
                  end.max
                  [attribute.to_s.length, max_length].max + 3
                end
      header = partition_info_attrs.map.each_with_index do |attribute, index|
                 attribute.to_s.ljust(padding[index])
               end.join
      body = partitions.map do |partition|
               partition_info_attrs.map.each_with_index do |attribute, index|
                 partition.send(attribute).to_s.ljust(padding[index])
               end.join
             end.join("\n")
      seperator = ''.ljust(padding.inject(&:+),'-')

      [seperator, header, seperator, body, seperator].join("\n")
    end

  end
end
