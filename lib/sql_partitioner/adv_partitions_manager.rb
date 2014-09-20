module SqlPartitioner
  class AdvPartitionsManager < PartitionsManager

    # generate partition data of form {partition_name1 => timestamp1,
    # partition_name2 => timestamp2 ...} with interval = window_size between
    # each partitions.
    # if base_timestamp is nil it uses current timestamp as base
    #
    # for eg,
    # 1. input: (base_timestamp)b_t = nil, (end_timestamp)e_t = t6
    #           (current_time)t_now = t1, (window_size)w_s = 2
    #    partition_data: {until_t1 => t1, until_t3 => t3, until_t5 => t5,
    #                  until_t7 => t7}
    # 2. input: b_t = t2, e_t = t6 t_now = t1, w_s = 2
    #    partition_data: {until_t4 => t4, until_t6 => t6}
    # 3. input: b_t = t3, e_t = t6 t_now = t1, w_s = 2
    #    partition_data: {until_t5 => t5, until_t7 => t7}
    # 4. input: b_t = t3, e_t = t6 t_now = t7, w_s = 2
    #    partition_data: {}
    #
    # @param [Fixnum] base_timestamp timestamp after which partitions have
    #                                to be generated
    # @param [Fixnum] end_timestamp timestamp until which the partitions have
    #                               to be generated.
    # @param [Fixnum] window_size  interval between consecutive partitions
    #                              in days
    # @return [Hash] partition_data of form [partition_name, timestamp]
    def _build_partition_data(base_timestamp, end_timestamp, window_size)
      unless window_size.kind_of?(Integer) && window_size > 0
        _raise_arg_err "window_size should an Integer greater than 0"
      end
      partition_data = {}
      offset = @tum.days_to_time_unit(window_size)
      until_timestamp = base_timestamp || (self.current_timestamp - offset)
      while(until_timestamp <= end_timestamp) do
        until_timestamp += offset
        partition_name = name_from_timestamp(until_timestamp)
        partition_data[partition_name] =  until_timestamp
      end
      partition_data
    end
    private :_build_partition_data

    # finds the partitions to be deleted/archived and new partitions
    # to be added based on the start_date, end_date and window size in policy.
    #
    # @param [Hash] policy advance partition window policy
    # @return [Array,Hash] [to_be_dropped, to_be_added]
    #                  to_be_dropped : Array of partition_names to be dropped
    #                  to_be_added : partition data of new partitions to be
    #                                added
    def _prep_params_for_advance_partition(policy)
      start_date = policy[:active_partition_start_date]
      end_date = policy[:active_partition_end_date]
      window_size = policy[:partition_window_size_in_days]

      start_timestamp = @tum.to_time_unit(start_date.to_i)
      end_timestamp = @tum.to_time_unit(end_date.to_i)

      partition_info = @partitions_fetcher.fetch_partition_info_from_db

      max_partition = @partitions_fetcher.fetch_latest_partition(partition_info)
      to_be_dropped = partitions_older_than_timestamp(start_timestamp,
                                                      partition_info)
      unless max_partition
        raise "Atleast one non future partition expected, but none found"
      end
      to_be_added = _build_partition_data(max_partition.timestamp,
                                          end_timestamp,
                                          window_size)
      [to_be_dropped, to_be_added]
    end
    private :_prep_params_for_advance_partition

    # Given a partition window(start_date - end_date), drop all the partitions
    # that does not hold db records after start_date and create new partitions
    # until the end date in intervals of window size provided
    #
    # advance partition window policy has following attributes
    # :active_partition_start_date - start date of the partition window(Time)
    # :active_partition_end_date - end date of the partition window(Time)
    # :window_size - interval between new partitions if added(Integer)
    # :archive_policy - archive or drop the old partitions(String) Note:
    #                   this property is ignored as of now
    #
    # @param [Hash] policy advance partition window policy
    #
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean] true if not dry run
    # @return [Hash] SQLs to reorg and drop partitions if dry run is true
    #
    # @raise [ArgumentError] if policy has invalid values
    def advance_partition_window(policy, dry_run = false)
      _validate_advance_partition_policy(policy)
      to_be_dropped, to_be_added = _prep_params_for_advance_partition(policy)
      drop_partition_result = drop_partitions(to_be_dropped, dry_run)
      reorg_result = _reorg_future_partition(to_be_added, dry_run)
      if dry_run
        {:drop_sql => drop_partition_result, :reorg_sql => reorg_result}
      else
        drop_partition_result && reorg_result
      end
    end

    def manage_partitions(partition_size, drop_older_than, add_partitions_including, dry_run = false)
      time_now = Time.now.utc
      policy = {
        :active_partition_start_date   => time_now - TimeUnitManager.days_in_seconds(drop_older_than),
        :active_partition_end_date     => time_now + TimeUnitManager.days_in_seconds(add_partitions_including),
        :partition_window_size_in_days => window_size
      }

      result = partition_manager.advance_partition_window(policy, dry_run)
      if dry_run
        puts "DROP_SQL :  #{result[:drop_sql]}"
        puts "REORG_SQL : #{result[:reorg_sql]}"
      end
    end

    def _validate_advance_partition_policy(policy)
      start_date = policy[:active_partition_start_date]
      end_date = policy[:active_partition_end_date]
      window_size = policy[:partition_window_size_in_days]

      _raise_arg_err "invalid start date provided" unless start_date.kind_of?(Time)
      _raise_arg_err "invalid end date provided" unless end_date.kind_of?(Time)
      _raise_arg_err "start date should be less than end date" unless start_date < end_date
      _raise_arg_err "invalid window_size provided" unless window_size.kind_of?(Integer)
      _raise_arg_err "window size should be greater 0" unless window_size > 0
      true
    end
    private :_validate_advance_partition_policy


    # Initialize the partitions based on days relative to current timestamp.
    # Number of partition will be equal to the number of days  provided.
    # for example [-15,0,15] will create 4 partitions of form
    #   until_2014_03_17    1395077901193149
    #   until_2014_04_01    1396373901193398
    #   until_2014_04_16    1397669901193684
    #   future              MAXVALUE
    # In above example, -ve value for day will lead to partitions into past
    # and +ve value for day will lead to partitions into future
    #
    # @param [Array] Array of days(Integer)
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean] true if not dry run
    # @return [String] sql to initialize partitions if dry run is true
    # @raise [ArgumentError] if days is not array or if one of the
    #                    days is not integer
    def initialize_partitioning_in_days(days, dry_run = false)
      _validate_initialize_partitioning_in_days_params(days)
      partition_data = {}
      days.sort.each do |days_form_now|
        until_timestamp = self.current_timestamp + @tum.days_to_time_unit(days_form_now)
        partition_name  = name_from_timestamp(until_timestamp)
        partition_data[partition_name] = until_timestamp
      end
      partition_data[FUTURE_PARTITION_NAME] = FUTURE_PARTITION_VALUE
      initialize_partitioning(partition_data, dry_run)
    end

    # fetch all partitions from information schema or input that hold records
    # older than the timestamp provided
    #
    # @param [Fixnum] timestamp
    # @param [Array] partition_info Array of partition info structs. if nil
    #                partition info is fetched from db
    # @return [Array] Array of partition name(String) that hold data older
    #                 than given timestamp
    def partitions_older_than_timestamp(timestamp, partition_info = nil)
      @partitions_fetcher.non_future_partitions(partition_info).select do |p|
        timestamp > p.timestamp
      end.map(&:name)
    end

    # fetch all partitions from information schema or input that hold records
    # newer than the timestamp provided
    #
    # @param [Fixnum] timestamp
    # @param [Array] partition_info Array of partition info structs. if nil
    #                partition info is fetched from db
    # @return [Array] Array of partition name(String) that hold data older
    #                 than given timestamp
    def partitions_recent_than_timestamp(timestamp, partition_info = nil)
      current_partition = fetch_current_partition partition_info
      current_partition = current_partition && current_partition.partition_name
      recent_partitions = non_future_partitions(partition_info).select do |p|
                            timestamp <= p.partition_timestamp
                          end
      recent_partitions.map(&:name) - Array(current_partition)
    end

    # drop partitions that are older than the given timestamp
    # @param [Fixnum] timestamp partitions older than this timestamp will be
    #                           dropped
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    def drop_partitions_older_than(timestamp, dry_run = false)
      partitions = partitions_older_than_timestamp(timestamp)
      drop_partitions(partitions, dry_run)
    end


    # Add new partition holds data until the timestamp provided
    #
    # @param [Fixnum] until_timestamp, timestamp of the partition
    # @param [Boolean] dry run, default value is false. Query wont be executed
    #                  if dry_run is set to true
    # @return [Boolean]
    # @raise [ArgumentError] if timestamp provided is not Integer
    def create_partition(until_timestamp, dry_run = false)
      _validate_timestamp(until_timestamp)
      partition_name = name_from_timestamp(until_timestamp)
      create_sql = SqlPartitioner::SQL.create_partition(table_name,
                                                             partition_name,
                                                             until_timestamp)
      if dry_run
        create_sql
      else
        _execute_and_display_partition_info(create_sql)
      end
    end


    def _validate_initialize_partitioning_in_days_params(days)
      msg = "days should be Array but #{days.class} found"
      _raise_arg_err(msg) unless days.kind_of?(Array)
      days.each do |days_from_now|
       msg = "#{days_from_now} should be Integer, but"\
             " #{days_from_now.class} found"
       _raise_arg_err(msg) unless days_from_now.kind_of?(Integer)
      end
      true
    end
    private :_validate_initialize_partitioning_in_days_params


  end
end
