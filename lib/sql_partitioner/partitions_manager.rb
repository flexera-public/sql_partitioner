module SqlPartitioner
  class PartitionsManager < BasePartitionsManager

    # Initialize the partitions based on intervals relative to current timestamp.
    # Partition size specified by (partition_size, partition_size_unit), i.e. 1 month.
    # Partitions will be created as needed to cover days_into_future.
    #
    # @param [Fixnum] days_into_future Number of days into the future from current_timestamp
    # @param [Symbol] partition_size_unit one of: [:months, :days]
    # @param [Fixnum] partition_size size of partition (in terms of partition_size_unit), i.e. 3 month
    # @param [Boolean] dry_run Defaults to false. If true, query wont be executed.
    # @return [Boolean] true if not dry run
    # @return [String] sql to initialize partitions if dry run is true
    # @raise [ArgumentError] if days is not array or if one of the
    #                    days is not integer
    def initialize_partitioning_in_intervals(days_into_future, partition_size_unit = :months, partition_size = 1, dry_run = false)
      _validate_initialize_partitioning_in_intervals_params(days_into_future)

      start_ts = @tum.advance(current_timestamp, :days, -days_in_past)
      end_ts   = @tum.advance(current_timestamp, :days, days_into_future)

      partition_data = partitions_to_append_by_date_range(start_ts, end_ts, partition_size_unit, partition_size)
      initialize_partitioning(partition_data, dry_run)
    end

    def _validate_initialize_partitioning_in_intervals_params(days_into_future)
      msg = "days_into_future should be Fixnum but #{days_into_future.class} found"
      _raise_arg_err(msg) unless days_into_future.kind_of?(Fixnum)
      true
    end
    private :_validate_initialize_partitioning_in_intervals_params


    VALID_PARTITION_SIZE_UNITS = [:months, :days]
    # Get partition to add a partition to end with the given window size
    #
    # @param [Fixnum] partition_start_timestamp
    # @param [Symbol] partition_size_unit: [:days, :months]
    # @param [Fixnum] partition_size, size of partition (in terms of partition_size_unit)
    # @param [Fixnum] partitions_into_future, how many partitions into the future should be covered
    #
    # @return [Hash] partition_data hash
    def partitions_to_append(partition_start_timestamp, partition_size_unit, partition_size, days_into_future)
      if partition_size_unit.nil? || !VALID_PARTITION_SIZE_UNITS.include?(partition_size_unit)
        _raise_arg_err "partition_size_unit must be one of: #{VALID_PARTITION_SIZE_UNITS.inspect}"
      end
      if days_into_future.nil? || days_into_future <= 0
        _raise_arg_err "partitions_into_future should be > 0"
      end

      end_timestamp  = @tum.advance(current_timestamp, :days, days_into_future)
      partitions_to_append_by_date_range(partition_start_timestamp, end_timestamp, partition_size_unit, partition_size)
    end

    # Get partition_data hash based on the last partition's timestamp and covering end_timestamp
    #
    # @param [Fixnum] partition_start_timestamp, timestamp of last partition
    # @param [Fixnum] end_timestamp, timestamp which the newest partition needs to include
    # @param [Symbol] partition_size_unit: [:days, :months]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    #
    # @return [Hash] partition_data hash
    def partitions_to_append_by_date_range(partition_start_timestamp, end_timestamp, partition_size_unit, partition_size)
      num_partitions = num_partitions_for(partition_start_timestamp, end_timestamp, partition_size_unit, partition_size)
      partitions_to_append_since(partition_start_timestamp, partition_size_unit, partition_size, num_partitions)
    end

    # Get number of partitions to create starting from partition_start_timestamp and covering end_timestamp
    #
    # @param [Fixnum] partition_start_timestamp, timestamp of last partition
    # @param [Fixnum] end_timestamp, timestamp which the newest partition needs to include
    # @param [Symbol] partition_size_unit: [:days, :months]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    #
    # @return [Hash] partition_data hash
    def num_partitions_for(partition_start_timestamp, end_timestamp, partition_size_unit, partition_size)
      new_start_timestamp = partition_start_timestamp

      num_partitions = 0
      while new_start_timestamp < end_timestamp
        new_start_timestamp = @tum.advance(new_start_timestamp, partition_size_unit, partition_size)

        num_partitions += 1
      end

      num_partitions
    end

    VALID_PARTITION_INTERVALS = [:months, :days]

    # Get partition_data hash based on the last partition's timestamp
    #
    # @param [Fixnum] partition_start_timestamp, timestamp of last partition
    # @param [Symbol] partition_size_unit: [:days, :months]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    # @param [Fixnum] num_partitions, how many partitions should be created
    #
    # @return [Hash] partition_data hash
    def partitions_to_append_since(partition_start_timestamp, partition_size_unit, partition_size, num_partitions)
      if partition_size_unit.nil? || !VALID_PARTITION_INTERVALS.include?(partition_size_unit)
        _raise_arg_err "partition_size_unit must be one of: #{VALID_PARTITION_INTERVALS.inspect}"
      end
      if partition_size.nil? || partition_size <= 0
        _raise_arg_err "partition_size should be > 0"
      end

      new_start_timestamp = partition_start_timestamp

      new_partition_data    = {}
      num_partitions.times do |i|
        new_start_timestamp = @tum.advance(new_start_timestamp, partition_size_unit, partition_size)

        new_partition_name = name_from_timestamp(new_start_timestamp)
        new_partition_data[new_partition_name] = new_start_timestamp
      end

      new_partition_data
    end

    # Wrapper around append partition to add a partition to end with the
    # given window size
    #
    # @param [Symbol] partition_size_unit: [:days, :months]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    # @param [Fixnum] days_into_future, how many days into the future need to be covered by partitions
    # @param [Boolean] dry_run, Defaults to false. If true, query wont be executed.
    # @raise [ArgumentError] if  window size is nil or not greater than 0
    def append_partition_intervals(partition_size_unit, partition_size, days_into_future = 30, dry_run = false)
      partitions = Partition.all(adapter, table_name)
      if partitions.blank?
        raise "partitions must be properly initialized before appending"
      end
      latest_partition = partitions.latest_partition

      new_partition_data = partitions_to_append(latest_partition.timestamp, partition_size_unit, partition_size, days_into_future)

      if new_partition_data.empty?
        msg = <<-MSG
          Append: No-Op - Latest Partition Time of #{latest_partition.timestamp}, i.e. #{Time.at(@tum.from_time_unit(latest_partition.timestamp))} covers >= #{days_into_future} days_into_future
        MSG
      else
        msg = <<-MSG
          Append: Appending the following new partitions: #{new_partition_data.inspect}
        MSG
        reorg_future_partition(new_partition_data, dry_run)
      end

      log(msg)
    end

    # drop partitions that are older than days(input) from now
    # @param [Fixnum] days_from_now
    # @param [Boolean] dry_run, Defaults to false. If true, query wont be executed.
    def drop_partitions_older_than_in_days(days_from_now, dry_run = false)
      timestamp = self.current_timestamp - @tum.days_to_time_unit(days_from_now)
      drop_partitions_older_than(timestamp, dry_run)
    end

    # drop partitions that are older than the given timestamp
    # @param [Fixnum] timestamp partitions older than this timestamp will be
    #                           dropped
    # @param [Boolean] dry_run, Defaults to false. If true, query wont be executed.
    def drop_partitions_older_than(timestamp, dry_run = false)
      partitions = Partition.all(adapter, table_name).older_than_timestamp(timestamp)

      if partitions.blank?
        msg = <<-MSG
          Drop: No-Op - No partitions older than #{timestamp}, i.e. #{Time.at(@tum.from_time_unit(timestamp))} to drop
        MSG
      else
        partition_names = partitions.map(&:name)

        msg = <<-MSG
          Drop: Dropped partitions: #{partition_names.inspect}
        MSG
        drop_partitions(partition_names, dry_run)
      end

      log(msg)
    end
  end
end
