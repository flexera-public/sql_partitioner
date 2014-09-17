module SqlPartitioner
  class SQL
    def self.partition_info
      SqlPartitioner::SQL.compress_lines(<<-SQL)
        SELECT  *
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = ?
      SQL
    end

    def self.drop_partitions(table_name, partition_names)
      return nil if partition_names.empty?
      SqlPartitioner::SQL.compress_lines(<<-SQL)
        ALTER TABLE #{table_name}
        DROP PARTITION #{partition_names.join(',')}
      SQL
    end

    def self.create_partition(table_name, partition_name, until_timestamp)
      SqlPartitioner::SQL.compress_lines(<<-SQL)
        ALTER TABLE #{table_name}
        ADD PARTITION
        (PARTITION #{partition_name}
         VALUES LESS THAN (#{until_timestamp}))
      SQL
    end

    def self.reorg_partitions(table_name,
                              new_partition_data,
                              reorg_partition_name)
      return nil if new_partition_data.empty?
      partition_suq_query = new_partition_data.map do |partition_name, until_timestamp|
        "PARTITION #{partition_name} VALUES LESS THAN (#{until_timestamp})"
      end.join(',')
      SqlPartitioner::SQL.compress_lines(<<-SQL)
        ALTER TABLE #{table_name}
        REORGANIZE PARTITION #{reorg_partition_name} INTO
        (#{partition_suq_query})
      SQL
    end


    def self.initialize_partitioning(table_name, partition_data)
      partition_sub_query = partition_data.map do |partition_name, until_timestamp|
        "PARTITION #{partition_name} VALUES LESS THAN (#{until_timestamp})"
      end.join(',')
      SqlPartitioner::SQL.compress_lines(<<-SQL)
        ALTER TABLE #{table_name}
        PARTITION BY RANGE(timestamp)
        (#{partition_sub_query})
      SQL
    end


    # Replace sequences of whitespace (including newlines) with either
    # a single space or remove them entirely (according to param _spaced_).
    #
    # Copied from:
    #   https://github.com/datamapper/dm-core/blob/master/lib/dm-core/support/ext/string.rb
    #
    #   compress_lines(<<QUERY)
    #     SELECT name
    #     FROM users
    #   QUERY => "SELECT name FROM users"
    #
    # @param [String] string
    #   The input string.
    #
    # @param [TrueClass, FalseClass] spaced (default=true)
    #   Determines whether returned string has whitespace collapsed or removed.
    #
    # @return [String] The input string with whitespace (including newlines) replaced.
    #
    def self.compress_lines(string, spaced = true)
      string.split($/).map { |line| line.strip }.join(spaced ? ' ' : '')
    end

  end
end
