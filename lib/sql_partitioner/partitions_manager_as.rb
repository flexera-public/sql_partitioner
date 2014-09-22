require "active_support"

module SqlPartitioner
  class PartitionsManager

    # Wrapper around append partition to add a partition to end with the
    # given window size
    # @param [Symbol] partition_interval: [:hours, :days, :months, :years]
    # @param [Fixnum] partition_size, intervals covered by the new partition
    # @param [Fixnum] partitions_into_future, how many partitions into the future should be covered
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean] true if dry_run is false
    # @return [String] reorg sql if dry run is true
    # @raise [ArgumentError] if  window size is nil or not greater than 0
    VALID_PARTITION_INTERVALS = [:second, :minute, :hour, :day, :month, :year]
    def append_partition_intervals(partition_interval, partition_size, partitions_into_future = 1, dry_run = false)
      if partition_interval.nil? || !VALID_PARTITION_INTERVALS.include?(partition_interval)
        _raise_arg_err "partition_interval must be one of: #{VALID_PARTITION_INTERVALS.inspect}"
      end
      if partition_size.nil? || partition_size <= 0
        _raise_arg_err "partition_size should be > 0"
      end
      if partitions_into_future.nil? || partitions_into_future <= 0
        _raise_arg_err "partitions_into_future should be > 0"
      end
      latest_partition = fetch_latest_partition
      latest_part_time = Time.at(from_time_unit(latest_partition.partition_timestamp))

      interval = case partition_interval
        when :second
          partition_size.seconds
        when :minute
          partition_size.minutes
        when :hour
          partition_size.hours
        when :day
          partition_size.days
        when :month
          partition_size.months
        when :year
          partition_size.years
      end

      # ensure partitions created at interval from latest thru target
      while (latest_part_time - @current_timestamp)/interval.to_i < partitions_into_future.to_i
        latest_part_time += interval
        puts "Appending Partition Time of #{latest_part_time} as only #{((latest_part_time - Time.now)/interval.to_i).round - 1} partitions_into_future at #{partition_size} #{partition_interval} each"  
        _append_partition(to_time_unit(latest_part_time.to_i), dry_run)
      end
      puts "Append: Latest Partition Time of #{latest_part_time} covers >= #{partitions_into_future} partitions_into_future at #{partition_size} #{partition_interval} each"
    end

  end
end
