module SqlPartitioner
  class PartitionsManager < BasePartitionsManager

    # Initialize the partitions based on months relative to current timestamp.
    # Number of partition will be equal to the number of months  provided.
    # for example [-2,-1,0,1] will create 5 partitions of form
    #   until_2014_07_01    1404198000000000
    #   until_2014_08_01    1406876400000000
    #   until_2014_09_01    1409554800000000
    #   until_2014_10_01    1412146800000000
    #   future              MAXVALUE
    # In above example, -ve value for month will lead to partitions into past
    # and +ve value for month will lead to partitions into future
    #
    # NOTE: This also used initialization of either:
    #       { :current_time => calc_beginning_of_month } (easiest) 
    #       or
    #       { :current_timestamp => calc_beginning_of_month * time_unit_multiplier } 
    #
    # @param [Array] Array of months(Integer)
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean] true if not dry run
    # @return [String] sql to initialize partitions if dry run is true
    # @raise [ArgumentError] if days is not array or if one of the
    #                    days is not integer
    def initialize_partitioning_in_months(months, dry_run = false)
      _validate_initialize_partitioning_in_months_params(months)

      partition_data = {}
      months.sort.each do |months_from_now|
        until_timestamp = @tum.to_time_unit((Time.at(@tum.from_time_unit(self.current_timestamp)) + months_from_now.months).to_i)
        partition_name  = name_from_timestamp(until_timestamp)
        partition_data[partition_name] = until_timestamp
      end

      initialize_partitioning(partition_data, dry_run)
    end

    def _validate_initialize_partitioning_in_months_params(months)
      msg = "days should be Array but #{months.class} found"
      _raise_arg_err(msg) unless months.kind_of?(Array)
      months.each do |months_from_now|
       msg = "#{months_from_now} should be Integer, but"\
             " #{months_from_now.class} found"
       _raise_arg_err(msg) unless months_from_now.kind_of?(Integer)
      end
      true
    end
    private :_validate_initialize_partitioning_in_months_params


    #------------------------------------------

    # KEEP
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

      partitions = Partition.all(adapter, table_name)
      latest_partition = partitions.latest_partition
      latest_part_time = Time.at(@tum.from_time_unit(latest_partition.timestamp))

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
      while (latest_part_time - Time.now)/interval.to_i < partitions_into_future.to_i
        latest_part_time += interval
        puts "Appending Partition Time of #{latest_part_time} as only #{((latest_part_time - Time.now)/interval.to_i).round - 1} partitions_into_future at #{partition_size} #{partition_interval} each"  
        _append_partition(@tum.to_time_unit(latest_part_time.to_i), dry_run)
      end
      puts "Append: Latest Partition Time of #{latest_part_time} covers >= #{partitions_into_future} partitions_into_future at #{partition_size} #{partition_interval} each"
    end


    # drop partitions that are older than days(input) from now
    # @param [Fixnum] days_from_now
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    def drop_partitions_older_than_in_days(days_from_now, dry_run = false)
      timestamp = self.current_timestamp + @tum.days_to_time_unit(days_from_now)
      drop_partitions_older_than(timestamp, dry_run)
    end

    # KEEP
    # drop partitions that are older than the given timestamp
    # @param [Fixnum] timestamp partitions older than this timestamp will be
    #                           dropped
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    def drop_partitions_older_than(timestamp, dry_run = false)
      partitions = Partition.all(adapter, table_name).older_than_timestamp(timestamp)

      if partitions.blank?
        puts "Drop: No partitions older than #{timestamp}, i.e. #{Time.at(@tum.from_time_unit(timestamp))}"
      else
        partition_names = partitions.map(&:name)

        puts "Dropping partitions: #{partition_names.inspect}"
        drop_partitions(partition_names, dry_run)
      end
    end
  end
end
