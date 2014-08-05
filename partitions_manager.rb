module SchemaTools
  module Events
    class PartitionManager

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
        @adapter           = options[:adapter] || DataMapper.repository.adapter
        @current_timestamp = options[:current_timestamp] || Time.now.to_i * 1_000_000
        @table_name        = options[:table_name] || 'events'
        @logger            = options[:logger] || Merb.logger
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
      def self.name_from_timestamp(timestamp)
        if timestamp == FUTURE_PARTITION_VALUE
           FUTURE_PARTITION_NAME
        else
          "until_#{Time.at(timestamp/1_000_000).strftime("%Y_%m_%d")}"
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
        adapter.execute("SET lock_wait_timeout = #{timeout}")
        begin
          return block.call
        ensure
          adapter.execute("SET lock_wait_timeout = #{lock_wait_timeout_before}")
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


      def _validate_initialize_partitioning_params(partition_data)
        unless partition_data.kind_of?(Hash)
          _raise_arg_err "partition data should be Hash but"\
                         " #{partition_data.class} found"
        end
        partition_data.each do |key, value|
          unless key.kind_of?(String)
            _raise_arg_err "partition name:#{key} should be String,"\
                           "but #{key.class} found"
          end
          next if value == FUTURE_PARTITION_VALUE
          unless value.kind_of?(Integer)
            _raise_arg_err "partition timestamp:#{value} should be Integer,"\
                           "but #{key.class} found"
          end
        end
        true
      end
      private :_validate_initialize_partitioning_params

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
        new_partition_name = self.class.name_from_timestamp(until_timestamp)
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
        reorg_sql = SchemaTools::Events::SQL.reorg_partitions(table_name,
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
        select_sql = SchemaTools::Events::SQL.partition_info
        result = adapter.select(select_sql, adapter.schema_name, table_name)
        result.map do |partition|
          wrapper = OpenStruct.new(partition)
          if partition.partition_description ==  FUTURE_PARTITION_VALUE
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
        until_timestamp = latest_partition.partition_timestamp  +
                         (60 * 60 * 24 * partition_size * 1_000_000)
        _append_partition(until_timestamp, dry_run)
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
        partition_name = self.class.name_from_timestamp(until_timestamp)
        create_sql = SchemaTools::Events::SQL.create_partition(table_name,
                                                               partition_name,
                                                               until_timestamp)
        if dry_run
          create_sql
        else
          _execute_and_display_partition_info(create_sql)
        end
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
        drop_sql = SchemaTools::Events::SQL.drop_partitions(table_name,
                                                            partition_names)
        if dry_run
          drop_sql
        else
          drop_sql ? _execute_and_display_partition_info(drop_sql) : false
        end
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

      # drop partitions that are older than days(input) from now
      # @param [Fixnum] days_from_now
      # @param [Boolean] dry run, default value is false. Query wont be executed
      #                  if dry_run is set to true
      def drop_partitions_older_than_in_days(days_from_now, dry_run = false)
        timestamp = self.current_timestamp + (60 * 60 * 24 * days_from_now * 1_000_000)
        drop_partitions_older_than(timestamp, dry_run)
      end

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
          until_timestamp =  self.current_timestamp + (60 * 60 * 24 * days_form_now * 1_000_000)
          partition_name = self.class.name_from_timestamp(until_timestamp)
          partition_data[partition_name] = until_timestamp
        end
        partition_data[FUTURE_PARTITION_NAME] = FUTURE_PARTITION_VALUE
        initialize_partitioning(partition_data, dry_run)
      end


      # initialize partitioning on the given table based on partition_data
      # provided.
      # partition data should be of form
      #   {partition_name1 => partition_timestamp_1 ,
      #    partition_name2 => partition_timestamp_2...}
      # For example:
      #   {'until_2014_03_17' => 1395077901193149
      #    'until_2014_04_01' => 1396373901193398}
      #
      # @param [Hash] partition data
      # @param [Boolean] dry run, default value is false. Query wont be executed
      #                  if dry_run is set to true
      # @raise [ArgumentError] if partition data is not hash or if one of name id
      #                    is not a String or if one of the value is not
      #                    Integer
      def initialize_partitioning(partition_data, dry_run = false)
        _validate_initialize_partitioning_params(partition_data)
        init_sql = SchemaTools::Events::SQL.initialize_partitioning(table_name,
                                                                    partition_data)
        if dry_run
          init_sql
        else
          _execute_and_display_partition_info(init_sql)
        end
      end

      # fetch all partitions from information schema or input that hold events
      # older than the timestamp provided
      #
      # @param [Fixnum] timestamp
      # @param [Array] partition_info Array of partition info structs. if nil
      #                partition info is fetched from db
      # @return [Array] Array of partition name(String) that hold data older
      #                 than given timestamp
      def partitions_older_than_timestamp(timestamp, partition_info = nil)
        non_future_partitions(partition_info).select do |p|
          timestamp > p.partition_timestamp
        end.map(&:partition_name)
      end

      # fetch all partitions from information schema or input that hold events
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
        recent_partitions.map(&:partition_name) - Array(current_partition)
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
        non_future_partitions(partition_info).max{ |p| p.partition_timestamp }
      end

      #fetch the partition which is currently active. i.e  holds the events
      # generated now
      def fetch_current_partition(partition_info = nil)
        non_future_partitions(partition_info).select do |p|
          p.partition_timestamp > self.current_timestamp
        end.min { |p| p.partition_timestamp }
      end

      #fetch the partition with oldest timestamp
      def fetch_oldest_partition(partition_info = nil)
        non_future_partitions(partition_info).min { |p| p.partition_timestamp }
      end

      def future_partition?(partition)
        partition.partition_timestamp == FUTURE_PARTITION_VALUE
      end

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
        offset = window_size * 24 * 60 * 60 * 1_000_000
        until_timestamp = base_timestamp || (self.current_timestamp - offset)
        while(until_timestamp <= end_timestamp) do
          until_timestamp += offset
          partition_name = self.class.name_from_timestamp(until_timestamp)
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

        start_timestamp = start_date.to_i * 1_000_000
        end_timestamp = end_date.to_i * 1_000_000

        partition_info = fetch_partition_info_from_db

        max_partition = fetch_latest_partition(partition_info)

        to_be_dropped = partitions_older_than_timestamp(start_timestamp,
                                                        partition_info)
        unless max_partition
          raise "Atleast one non future partition expected, but none found"
        end
        to_be_added = _build_partition_data(max_partition.partition_timestamp,
                                            end_timestamp,
                                            window_size)
        [to_be_dropped, to_be_added]
      end
      private :_prep_params_for_advance_partition


      # Given a partition window(start_date - end_date), drop all the partitions
      # that does not hold the events after start_date and create new partitions
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

    end
  end
end
