module SchemaTools
  module Events
    class DataManager

      attr_accessor :table, :archive_table, :adapter, :logger

      def initialize(adapter = DataMapper.repository.adapter,
                     table_name = 'events',
                     archive_table_name = 'deleted_events',
                     logger = nil)
        @adapter = adapter
        @table = table_name
        @archive_table = archive_table_name
        @logger = logger || Merb.logger
      end

      def log(message)
        @logger.info "[#{self.class.name}]#{message}"
      end

      # deletes the events from events_table
      #
      # @param [Fixnum] timestamp  timestamp until which the events have to be
      #                            selected
      # @param [Fixnum] batch_size  limit and offset value
      #
      # @param [Boolean] dry_run dry run or actual archive of events
      def delete_events(timestamp, batch_size = nil, dry_run = true)
        log "----[Priming] Total events to be deleted: "\
            "#{prime_events(timestamp)}---"
        if dry_run
          log "----- Dry Run -------"
          select_events(timestamp, batch_size)
        else
          delete_events_actual(timestamp, batch_size)
        end
      end

      def delete_events_actual(timestamp, batch_size)
        delete_sql = SchemaTools::Events::SQL.delete(in_batches = !!batch_size)
        sql_args = !!batch_size ? [timestamp, batch_size] : [timestamp]
        batch_execute(:deleted) do
          execute_sql(delete_sql, *sql_args)
        end
      end

      # Moves the events from events table to deleted_events table.
      #
      # @param [Fixnum] timestamp  timestamp until which the events have to be
      #                            selected (rows with timestamps < timestamp)
      # @param [Fixnum] batch_size  limit and offset value
      #
      # @param [Boolean] dry_run dry run or actual archive of events
      def archive_events(timestamp, batch_size = nil, dry_run = true)
        log "----[Priming] Total events to be archived: "\
            "#{prime_events(timestamp)}---"
        if dry_run
          log "----- Dry Run -------"
          select_events(timestamp, batch_size)
        else
          archive_events_actual(timestamp, batch_size)
        end
      end

      def archive_events_actual(timestamp, batch_size)
        insert_sql = SchemaTools::Events::SQL.insert(in_batches = !!batch_size,
                                                     archive_table)
        delete_sql = SchemaTools::Events::SQL.delete(in_batches = !!batch_size)
        sql_args = !!batch_size ? [timestamp, batch_size] : [timestamp]
        batch_execute(:archived) do
          copied_count  = execute_sql(insert_sql, *sql_args)
          deleted_count = execute_sql(delete_sql, *sql_args)
          unless copied_count == deleted_count
            log "Warning! Inserted #{copied_count} into #{archive_table}" \
                " while deleted #{deleted_count} from events table"
          end
          deleted_count
        end
      end

      def execute_sql(sql, *args)
        adapter.execute(sql, *args).affected_rows
      end

      def prime_events(timestamp)
        started_at = Time.now
        total_count = count_events(timestamp)
        cool_down_for(self.class.cool_down_period(Time.now - started_at))
        total_count
      end

      # selects events in batches, uses offset to simulate batch selects
      #
      # @param [Fixnum] timestamp  timestamp until which the events have to be
      #                            selected
      # @param [Fixnum] batch_size  limit and offset value
      #
      def select_events(timestamp, batch_size)
        if batch_size
          select_sql = SchemaTools::Events::SQL.select_by_offset(in_batches = true)
          batch_execute(:selected) do |index|
            sql_args = [timestamp, batch_size, batch_size * index ]
            adapter.query(select_sql, *sql_args).count
          end
        else
          select_sql = SchemaTools::Events::SQL.select(in_batches = false)
          adapter.query(select_sql, timestamp).count
        end
      end

      def count_events(timestamp, batch_size = nil)
        adapter.query(SchemaTools::Events::SQL.count, timestamp).first
      end

      # helpers to enable caller to specifiy input in days
      def archive_events_older_than(days, batch_size = nil, dry_run = true)
        timestamp = self.class.days_to_timestamp(days)
        log "Archiving events older than '#{self.class.timestamp_to_date(timestamp)}'"
        archive_events(timestamp, batch_size, dry_run)
      end

      def delete_events_older_than(days, batch_size = nil, dry_run = true)
        timestamp = self.class.days_to_timestamp(days)
        log "Deleting events older than '#{self.class.timestamp_to_date(timestamp)}'"
        delete_events(timestamp, batch_size, dry_run)
      end

      def select_events_older_than(days, batch_size = nil)
        timestamp = self.class.days_to_timestamp(days)
        log "Selecting events older than '#{self.class.timestamp_to_date(timestamp)}'"
        select_events(timestamp, batch_size)
      end

      def count_events_older_than(days, batch_size = nil)
        timestamp = self.class.days_to_timestamp(days)
        log "Counting events older than '#{self.class.timestamp_to_date(timestamp)}'"
        count_events(timestamp, batch_size)
      end

      # Executes the queries to move (insert and delete) in loop until
      # there are no more records to move.
      #
      # @param [String] insert_sql sql that performs insert
      # @param [String] delete_sql sql that performs delete
      #
      def batch_execute(action)
        total_rows_affected = 0
        index = 0
        loop do
          started_at = Time.now
          rows_affected = yield(index)
          total_rows_affected += rows_affected
          log "Successfully #{action} #{total_rows_affected} events"
          break if rows_affected == 0
          cool_down_for(self.class.cool_down_period(Time.now - started_at))
          index += 1
        end

        total_rows_affected
      end

      def cool_down_for(sleep_time)
        log "Cooldown period: #{sleep_time} seconds"
        sleep sleep_time
      end

      def self.cool_down_period(query_execution_time)
        sleep_time = case query_execution_time
                     when 0..0.5
                        1
                     when 0.5..1
                        2
                     when 1..10
                        5
                     when 100..10
                        20
                     when 100..400
                        60
                     when 400..1000
                        4 * 60
                     when 1000..2000
                        8 * 60
                     else
                        (query_execution_time > 2000) ? 10 * 60 : 0
                     end
        sleep_time
      end

      def self.days_to_timestamp(days)
        time = Time.now.to_i - (60 * 60 * 24 * days)
        time  * 1_000_000
      end

      def self.timestamp_to_date(timestamp)
        Time.at(timestamp/1_000_000)
      end
    end
  end
end

