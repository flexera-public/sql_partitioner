module SqlPartitioner
  class BasePartitionsManager

    attr_accessor :table_name, :adapter, :logger, :current_timestamp

    FUTURE_PARTITION_NAME = 'future'

    FUTURE_PARTITION_VALUE = 'MAXVALUE'

    def initialize(options = {})
      @adapter            = options[:adapter]
      @tuc                = TimeUnitConverter.new(options[:time_unit] || :seconds)

      @current_timestamp  = @tuc.from_seconds((options[:current_time] || Time.now).to_i)
      @table_name         = options[:table_name]
      @logger             = options[:logger]
      @lock_wait_timeout  = options[:lock_wait_timeout]
    end

    #----------- Validation Helpers ---------------

    def _validate_positive_fixnum(parameter_name, parameter)
      _validate_class(parameter_name, parameter, Fixnum)

      if parameter <= 0
        _raise_arg_err "#{parameter_name} should be > 0"
      end
      true
    end
    private :_validate_positive_fixnum

    def _validate_class(parameter_name, parameter, expected_class)
      if !parameter.kind_of?(expected_class)
        _raise_arg_err("class of #{parameter_name} expected to be #{expected_class} but instead was #{parameter.class}")
      end
      true
    end
    private :_validate_class


    def _validate_timestamp(timestamp)
      return true if timestamp == FUTURE_PARTITION_VALUE

      _validate_positive_fixnum(:timestamp, timestamp)

      true
    end
    private :_validate_timestamp

    def _validate_partition_name(partition_name)
      _validate_class('partition_name', partition_name, String)
    end
    private :_validate_partition_name

    def _validate_partition_names(partition_names)
      _validate_class('partition_names', partition_names, Array)

      partition_names.each do |name|
        _validate_partition_name(name)
      end

      true
    end
    private :_validate_partition_names

    def _validate_partition_names_allowed_to_drop(partition_names)
      black_listed_partitions = [FUTURE_PARTITION_NAME]

      if active_partition = Partition.all(adapter, table_name).current_partition(self.current_timestamp)
        black_listed_partitions << active_partition.name
      end

      if (partition_names & black_listed_partitions).any?
       _raise_arg_err "current and future partition can never be dropped"
      end

      true
    end
    private :_validate_partition_names_allowed_to_drop

    def _validate_drop_partitions_names(partition_names)
      _validate_partition_names(partition_names)
      _validate_partition_names_allowed_to_drop(partition_names)

      true
    end
    private :_validate_drop_partitions_names

    def _validate_partition_data(partition_data)
      _validate_class('partition_data', partition_data, Hash)

      partition_data.each_pair do |key, value|
        _validate_partition_name(key)
        _validate_timestamp(value)

        if key == FUTURE_PARTITION_NAME && value != FUTURE_PARTITION_VALUE ||
           key != FUTURE_PARTITION_NAME && value == FUTURE_PARTITION_VALUE
          _raise_arg_err "future partition name '#{FUTURE_PARTITION_NAME}' must use timestamp '#{FUTURE_PARTITION_VALUE}',"\
                         "but got name #{key} and timestamp #{value}"
        end
      end

      true      
    end
    private :_validate_partition_data

    # initialize partitioning on the given table based on partition_data
    # provided.
    # partition data should be of form
    #   {partition_name1 => partition_timestamp_1 ,
    #    partition_name2 => partition_timestamp_2...}
    # For example:
    #   {'until_2014_03_17' => 1395077901193149,
    #    'until_2014_04_01' => 1396373901193398}
    #
    # @param [Hash] partition_data
    # @param [Boolean] dry_run, Defaults to false. If true, query wont be executed.
    # @raise [ArgumentError] if partition data is not hash or if one of name id
    #                    is not a String or if one of the value is not
    #                    Integer
    def initialize_partitioning(partition_data, dry_run = false)
      partition_data = partition_data.merge(FUTURE_PARTITION_NAME => FUTURE_PARTITION_VALUE)

      _validate_partition_data(partition_data)

      init_sql = SqlPartitioner::SQL.initialize_partitioning(table_name, partition_data)
      _execute_and_display_partition_info(init_sql, dry_run)
    end

    # Drop partitions by name
    # @param [Array] partition_names array of partition_names in String
    # @param [Boolean] dry_run Defaults to false. If true, query wont be executed.
    # @return [String] drop sql if dry run is true
    # @raise [ArgumentError] if input is not an Array or if partition name is
    #                        not a string
    def drop_partitions(partition_names, dry_run = false)
      _validate_drop_partitions_names(partition_names)

      drop_sql = SqlPartitioner::SQL.drop_partitions(table_name, partition_names)
      _execute_and_display_partition_info(drop_sql, dry_run)
    end

    # Reorgs future partition into partitions provided as input.
    #
    # @param [Hash] partition_data of form { partition_name1 => timestamp1..}
    # @param [Boolean] dry_run Defaults to false. If true, query wont be executed.
    # @return [Boolean] true if not dry run and query is executed else false
    # @return [String] sql if dry_run is true
    def reorg_future_partition(partition_data, dry_run = false)
      partition_data = partition_data.dup

      if partition_data.any?
        partition_data[FUTURE_PARTITION_NAME] = FUTURE_PARTITION_VALUE
      end

      _validate_partition_data(partition_data)

      reorg_sql = SqlPartitioner::SQL.reorg_partitions(table_name, partition_data, FUTURE_PARTITION_NAME)
      _execute_and_display_partition_info(reorg_sql, dry_run)
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
        seconds = @tuc.to_seconds(timestamp)
        "until_#{Time.at(seconds).utc.strftime("%Y_%m_%d")}"
      end
    end

    # executes the sql
    # @param [String] sql to be executed
    # @return [Boolean] true
    def _execute(sql)
      if @lock_wait_timeout
        SqlPartitioner::LockWaitTimeoutHandler.with_lock_wait_timeout(@adapter, @lock_wait_timeout) do
          adapter.execute(sql)
        end
      else
        adapter.execute(sql)
      end
    end
    private :_execute

    # executes the sql and then displays the partition info
    # @param [String] sql to be executed
    # @param [Boolean] dry_run Defaults to true. If true, query wont be executed.
    # @return [String/Boolean] returns SQL to be executed if dry_run=true
    def _execute_and_display_partition_info(sql, dry_run=true)
      if sql
        if dry_run
          sql
        else
          _execute(sql)
       
          log "\n#{Partition.to_log(Partition.all(adapter, table_name))}", false

          true
        end
      else
        false
      end
    end
    private :_execute_and_display_partition_info


    def _raise_arg_err(err_message)
      raise ArgumentError.new err_message
    end
    private :_raise_arg_err

  end
end
