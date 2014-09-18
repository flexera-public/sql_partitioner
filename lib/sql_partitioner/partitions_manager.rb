module SqlPartitioner
  class PartitionsManager

    attr_accessor :table_name, :adapter, :logger, :current_timestamp

    FUTURE_PARTITION_NAME = 'future'

    FUTURE_PARTITION_VALUE = 'MAXVALUE'

    PARTITION_INFO_ATTRS = [:ordinal_position,
                            :partition_name,
                            :partition_timestamp,
                            :table_rows,
                            :data_length,
                            :index_length]

    def initialize(options = {})
      @adapter           = options[:adapter]
      @tum               = TimeUnitManager.new(options[:time_unit] || :seconds)

      @current_timestamp = options[:current_timestamp] || @tum.to_time_unit(Time.now.to_i)
      @table_name        = options[:table_name]
      @logger            = options[:logger]
      @lock_wait_timeout = options[:lock_wait_timeout]
    end

    def log(message, prefix = true)
      message = "[#{self.class.name}]#{message}" if prefix
      @logger.info "#{message}"
    end

    # generates name of for "until_yyyy_mm_dd" from the given timestamp.
    # returns future partition name if value is FUTURE_PARTITION_VALUE
    #
    # @param [Fixnum] timestamp  timestamp for which the name has to be
    #                            generated.
    #
    # @return [String] partition_name
    def name_from_timestamp(timestamp)
      if timestamp == FUTURE_PARTITION_VALUE
         FUTURE_PARTITION_NAME
      else
        seconds = @tum.from_time_unit(timestamp)
        "until_#{Time.at(seconds).strftime("%Y_%m_%d")}"
      end
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

    # executes the sql and then displays the partition info
    # @param [String] sql to be executed
    # @return [Boolean] true
    def _execute_and_display_partition_info(sql)
      if @lock_wait_timeout
        with_lock_wait_timeout(@lock_wait_timeout) do
          adapter.execute(sql)
        end
      else
        adapter.execute(sql)
      end

      display_partition_info
    end
    private :_execute_and_display_partition_info


    def _raise_arg_err(err_message)
      raise ArgumentError.new err_message
    end
    private :_raise_arg_err

    #----------- Validation Helpers ---------------

    def _validate_timestamp(timestamp)
      if !timestamp.kind_of?(Integer) || timestamp < 0
        _raise_arg_err  "timestamp should be a positive integer"
      end
      true
    end
    private :_validate_timestamp


    def _validate_drop_partitions_params(partition_names)
      unless  partition_names.kind_of?(Array)
        msg = "partition_names should be array but #{partition_names.class}"\
               " found"
        _raise_arg_err(msg)
      end
      partition_names.each do |name|
        unless name.kind_of?(String)
          _raise_arg_err "Invalid value #{name}. String expected but"\
                         " #{name.class} found"
        end
      end
      black_listed_partitions = [FUTURE_PARTITION_NAME]
      if active_partition = fetch_current_partition
        black_listed_partitions << active_partition.partition_name
      end
      if (partition_names & black_listed_partitions).any?
       _raise_arg_err "current and future partition can never be dropped"
      end
      true
    end
    private :_validate_drop_partitions_params


    # Reorganizes the future partition into a new partition with the
    # timestamp provided. Partition name will be generated from timestamp
    #
    # @param [Fixnum] until_timestamp, timestamp of the partition
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean] true if dry_run is false
    # @return [String] reorg sql if dry run is true
    # @raise [ArgumentError] if timestamp provided is not Integer
    def _append_partition(until_timestamp, dry_run = false)
      _validate_timestamp(until_timestamp)
      new_partition_name = name_from_timestamp(until_timestamp)
      new_partition_data = {new_partition_name => until_timestamp}
      _reorg_future_partition(new_partition_data, dry_run)
    end
    private :_append_partition


    #------------------------------------------

    # Reorgs future partition into partitions provided as input.
    #
    # @param [Hash] partition_data of form { partition_name1 => timestamp1..}
    # @param [Boolean] dry_run Query wont be executed if dry run is true
    # @return [Boolean] true if not dry run and query is executed else false
    # @return [String] sql if dry_run is true
    def _reorg_future_partition(partition_data, dry_run = false)
      unless partition_data.empty?
        partition_data[FUTURE_PARTITION_NAME] = FUTURE_PARTITION_VALUE
      end
      reorg_sql = SqlPartitioner::SQL.reorg_partitions(table_name,
                                                            partition_data,
                                                            FUTURE_PARTITION_NAME)
      if dry_run
        reorg_sql
      else
        reorg_sql ? _execute_and_display_partition_info(reorg_sql) : false
      end
    end
    private :_reorg_future_partition


    # fetches the partition info from information schema
    # @return [Array] Array of partition info Struct
    def fetch_partition_info_from_db
      select_sql = SqlPartitioner::SQL.partition_info
      result = adapter.select(select_sql, adapter.schema_name, table_name)

      result.map do |partition|
        wrapper = OpenStruct.new(Hash[partition.each_pair.to_a])
        if partition.partition_description == FUTURE_PARTITION_VALUE
          wrapper.partition_timestamp = FUTURE_PARTITION_VALUE
        else
          wrapper.partition_timestamp = partition.partition_description.to_i
        end
        wrapper.ordinal_position = partition.partition_ordinal_position
        wrapper
      end
    end

    # logs the formatted partition info from information schema
    # @return [Boolean] true
    def display_partition_info
      partition_info = fetch_partition_info_from_db
      log "\n#{self.class.format_partition_info(partition_info)}", false
      true
    end

    # Wrapper around append partition to add a partition to end with the
    # given window size
    # @param [Fixnum] partition_size, days covered by the new partition
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean] true if dry_run is false
    # @return [String] reorg sql if dry run is true
    # @raise [ArgumentError] if  window size is nil or not greater than 0
    def append_partition(partition_size, dry_run = false)
      if partition_size.nil? || partition_size <= 0
        _raise_arg_err "Partition size should be > 0"
      end

      latest_partition = fetch_latest_partition
      raise "Latest partition not found" unless latest_partition

      until_timestamp = latest_partition.timestamp +
                        @tum.days_to_time_unit(partition_size)

      _append_partition(until_timestamp, dry_run)
    end


    # Drop partitions by name
    # @param [Array] array of partition_names in String
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [String] drop sql if dry run is true
    # @raise [ArgumentError] if input is not an Array or if partition name is
    #                        not a string
    def drop_partitions(partition_names, dry_run = false)
      _validate_drop_partitions_params(partition_names)
      drop_sql = SqlPartitioner::SQL.drop_partitions(table_name,
                                                          partition_names)
      if dry_run
        drop_sql
      else
        drop_sql ? _execute_and_display_partition_info(drop_sql) : false
      end
    end

    # drop partitions that are older than days(input) from now
    # @param [Fixnum] days_from_now
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    def drop_partitions_older_than_in_days(days_from_now, dry_run = false)
      timestamp = self.current_timestamp + @tum.days_to_time_unit(days_from_now)
      drop_partitions_older_than(timestamp, dry_run)
    end


    # get all partitions that does not have timestamp as 'FUTURE_PARTITION_VALUE'
    def non_future_partitions(partition_info = nil)
      partition_info ||= fetch_partition_info_from_db
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
