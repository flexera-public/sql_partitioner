module SqlPartitioner
  class PartitionsManager < BasePartitionsManager
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


    # drop partitions that are older than days(input) from now
    # @param [Fixnum] days_from_now
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    def drop_partitions_older_than_in_days(days_from_now, dry_run = false)
      timestamp = self.current_timestamp + @tum.days_to_time_unit(days_from_now)
      drop_partitions_older_than(timestamp, dry_run)
    end
  end
end
