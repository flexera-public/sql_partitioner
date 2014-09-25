module SqlPartitioner
  class PartitionsManager < BasePartitionsManager

    # Initialize the partitions based on intervals relative to current timestamp.
    # Number of partition will be equal to the number of (:months or :days) provided.
    #
    # @param [Fixnum] Number of days into the past from current_timestamp
    # @param [Fixnum] Number of days into the future from current_timestamp
    # @param [Symbol] partition interval type [:months, :days]
    # @param [Fixnum] number of intervals per partition, i.e. 3 month partitions
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean] true if not dry run
    # @return [String] sql to initialize partitions if dry run is true
    # @raise [ArgumentError] if days is not array or if one of the
    #                    days is not integer
    def initialize_partitioning_in_intervals(days_in_past, days_into_future = 27, partition_interval = :months, partition_size = 1, dry_run = false)
      _validate_initialize_partitioning_in_intervals_params(days_in_past, days_into_future)

      current_date_time = @tum.from_time_unit_to_date_time(current_timestamp)
      starting_date_time = TimeUnitManager.advance_date_time(current_date_time, :days, -days_in_past)
      @current_timestamp = @tum.to_time_unit(starting_date_time.to_time.to_i)

      partition_data = partitions_to_append(@current_timestamp, partition_interval, partition_size, days_in_past + days_into_future)
      initialize_partitioning(partition_data, dry_run)
    end

    def _validate_initialize_partitioning_in_intervals_params(parts_before, parts_from_now)
      [parts_before, parts_from_now].each do |parts|
        msg = "parts should be Fixnum but #{parts.class} found"
        _raise_arg_err(msg) unless parts.kind_of?(Fixnum)
      end
      true
    end
    private :_validate_initialize_partitioning_in_intervals_params


    # Get partition to add a partition to end with the given window size
    #
    # @param [Symbol] partition_interval: [:days, :months]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    # @param [Fixnum] partitions_into_future, how many partitions into the future should be covered
    #
    # @return [Hash] partition_data hash
    VALID_PARTITION_INTERVALS = [:months, :days]
    def partitions_to_append(partition_start_timestamp, partition_interval, partition_size, days_into_future)
      if partition_interval.nil? || !VALID_PARTITION_INTERVALS.include?(partition_interval)
        _raise_arg_err "partition_interval must be one of: #{VALID_PARTITION_INTERVALS.inspect}"
      end
      if partition_size.nil? || partition_size <= 0
        _raise_arg_err "partition_size should be > 0"
      end
      if days_into_future.nil? || days_into_future <= 0
        _raise_arg_err "partitions_into_future should be > 0"
      end

      # ensure partitions created at interval from latest thru target
      current_timestamp_date_time = @tum.from_time_unit_to_date_time(current_timestamp)
      date_time_to_be_covered     = TimeUnitManager.advance_date_time(current_timestamp_date_time, :days, days_into_future)

      latest_part_date_time = @tum.from_time_unit_to_date_time(partition_start_timestamp)
      new_partition_data    = {}

      while latest_part_date_time < date_time_to_be_covered
        latest_part_date_time = TimeUnitManager.advance_date_time(latest_part_date_time, partition_interval, partition_size * 1)

        new_partition_ts   = @tum.to_time_unit(latest_part_date_time.strftime('%s').to_i)
        new_partition_name = name_from_timestamp(new_partition_ts)
        new_partition_data[new_partition_name] = new_partition_ts
      end

      new_partition_data
    end

    # Wrapper around append partition to add a partition to end with the
    # given window size
    #
    # @param [Symbol] partition_interval: [:days, :months]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    # @param [Fixnum] partitions_into_future, how many partitions into the future should be covered
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean] true if dry_run is false
    # @return [String] reorg sql if dry run is true
    # @raise [ArgumentError] if  window size is nil or not greater than 0
    def append_partition_intervals(partition_interval, partition_size, days_into_future = 27, dry_run = false)
      partitions = Partition.all(adapter, table_name)
      if partitions.blank?
        raise "partitions must be properly initialized before appending"
      end
      latest_partition = partitions.latest_partition

      new_partition_data = partitions_to_append(latest_partition.timestamp, partition_interval, partition_size, days_into_future)

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
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    def drop_partitions_older_than_in_days(days_from_now, dry_run = false)
      timestamp = self.current_timestamp - @tum.days_to_time_unit(days_from_now)
      drop_partitions_older_than(timestamp, dry_run)
    end

    # drop partitions that are older than the given timestamp
    # @param [Fixnum] timestamp partitions older than this timestamp will be
    #                           dropped
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    def drop_partitions_older_than(timestamp, dry_run = false)
      partitions = Partition.all(adapter, table_name).older_than_timestamp(timestamp).compact

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
