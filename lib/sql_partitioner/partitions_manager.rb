module SqlPartitioner
  class PartitionsManager < BasePartitionsManager
    VALID_PARTITION_SIZE_UNITS = [:months, :days]

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
      partition_data = partitions_to_append(@current_timestamp, partition_size_unit, partition_size, days_into_future)
      initialize_partitioning(partition_data, dry_run)
    end

    # Get partition to add a partition to end with the given window size
    #
    # @param [Fixnum] partition_start_timestamp
    # @param [Symbol] partition_size_unit: [:days, :months]
    # @param [Fixnum] partition_size, size of partition (in terms of partition_size_unit)
    # @param [Fixnum] partitions_into_future, how many partitions into the future should be covered
    #
    # @return [Hash] partition_data hash
    def partitions_to_append(partition_start_timestamp, partition_size_unit, partition_size, days_into_future)
      _validate_positive_fixnum(:days_into_future, days_into_future)

      end_timestamp = @tum.advance(current_timestamp, :days, days_into_future)
      partitions_to_append_by_ts_range(partition_start_timestamp, end_timestamp, partition_size_unit, partition_size)
    end

    # Get partition_data hash based on the last partition's timestamp and covering end_timestamp
    #
    # @param [Fixnum] partition_start_timestamp, timestamp of last partition
    # @param [Fixnum] end_timestamp, timestamp which the newest partition needs to include
    # @param [Symbol] partition_size_unit: [:days, :months]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    #
    # @return [Hash] partition_data hash
    def partitions_to_append_by_ts_range(partition_start_timestamp, end_timestamp, partition_size_unit, partition_size)
      if partition_size_unit.nil? || !VALID_PARTITION_SIZE_UNITS.include?(partition_size_unit)
        _raise_arg_err "partition_size_unit must be one of: #{VALID_PARTITION_SIZE_UNITS.inspect}"
      end
      _validate_positive_fixnum(:partition_size, partition_size)
      _validate_positive_fixnum(:partition_start_timestamp, partition_start_timestamp)
      _validate_positive_fixnum(:end_timestamp, end_timestamp)

      timestamp = partition_start_timestamp

      partitions_to_append = {}
      while timestamp < end_timestamp
        timestamp = @tum.advance(timestamp, partition_size_unit, partition_size)

        partition_name = name_from_timestamp(timestamp)
        partitions_to_append[partition_name] = timestamp
      end

      partitions_to_append
    end

    # Wrapper around append partition to add a partition to end with the
    # given window size
    #
    # @param [Symbol] partition_size_unit: [:days, :months]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    # @param [Fixnum] days_into_future, how many days into the future need to be covered by partitions
    # @param [Boolean] dry_run, Defaults to false. If true, query wont be executed.
    # @return [Hash] partition_data hash of the partitions appended
    # @raise [ArgumentError] if  window size is nil or not greater than 0
    def append_partition_intervals(partition_size_unit, partition_size, days_into_future = 30, dry_run = false)
      partitions = Partition.all(adapter, table_name)
      if partitions.blank? || partitions.non_future_partitions.blank?
        raise "partitions must be properly initialized before appending"
      end
      latest_partition = partitions.latest_partition

      new_partition_data = partitions_to_append(latest_partition.timestamp, partition_size_unit, partition_size, days_into_future)

      if new_partition_data.empty?
        msg = "Append: No-Op - Latest Partition Time of #{latest_partition.timestamp}, " +
              "i.e. #{Time.at(@tum.from_time_unit(latest_partition.timestamp))} covers >= #{days_into_future} days_into_future"
      else
        msg = "Append: Appending the following new partitions: #{new_partition_data.inspect}"
        reorg_future_partition(new_partition_data, dry_run)
      end

      log(msg)

      new_partition_data
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
    # @return [Array] an array of partition names that were dropped
    def drop_partitions_older_than(timestamp, dry_run = false)
      partitions = Partition.all(adapter, table_name).older_than_timestamp(timestamp)
      partition_names = partitions.map(&:name)

      if partition_names.empty?
        msg = "Drop: No-Op - No partitions older than #{timestamp}, i.e. #{Time.at(@tum.from_time_unit(timestamp))} to drop"
      else
        msg = "Drop: Dropped partitions: #{partition_names.inspect}"
        drop_partitions(partition_names, dry_run)
      end

      log(msg)

      partition_names
    end
  end
end
