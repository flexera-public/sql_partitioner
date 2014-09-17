module SqlPartitioner
  class SQL
    # def self.select(in_batches = false)
    #   select_sql = "SELECT * FROM events WHERE timestamp < ?"
    #   select_sql += " LIMIT ?" if in_batches
    #   DataMapper::Ext::String.compress_lines(select_sql)
    # end

    # def self.select_by_offset(in_batches = false)
    #   DataMapper::Ext::String.compress_lines(<<-SQL)
    #     #{select(in_batches)}
    #     OFFSET ?
    #   SQL
    # end

    # def self.count
    #   count_sql = "SELECT COUNT(*) FROM events WHERE timestamp < ?"
    #   DataMapper::Ext::String.compress_lines(count_sql)
    # end

    # def self.delete(in_batches = false)
    #   delete_sql = "DELETE FROM events WHERE timestamp < ?"
    #   delete_sql += " LIMIT ?" if in_batches
    #   DataMapper::Ext::String.compress_lines(delete_sql)
    # end

    # def self.insert(in_batches = false,
    #                 target_table_name = 'deleted_events')
    #   DataMapper::Ext::String.compress_lines(<<-SQL)
    #   INSERT IGNORE INTO #{target_table_name}
    #   #{select(in_batches)}
    #   SQL
    # end

    def self.partition_info
      DataMapper::Ext::String.compress_lines(<<-SQL)
        SELECT  *
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = ?
      SQL
    end

    def self.drop_partitions(table_name, partition_names)
      return nil if partition_names.empty?
      DataMapper::Ext::String.compress_lines(<<-SQL)
        ALTER TABLE #{table_name}
        DROP PARTITION #{partition_names.join(',')}
      SQL
    end

    def self.create_partition(table_name, partition_name, until_timestamp)
      DataMapper::Ext::String.compress_lines(<<-SQL)
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
      DataMapper::Ext::String.compress_lines(<<-SQL)
        ALTER TABLE #{table_name}
        REORGANIZE PARTITION #{reorg_partition_name} INTO
        (#{partition_suq_query})
      SQL
    end


    def self.initialize_partitioning(table_name, partition_data)
      partition_sub_query = partition_data.map do |partition_name, until_timestamp|
        "PARTITION #{partition_name} VALUES LESS THAN (#{until_timestamp})"
      end.join(',')
      DataMapper::Ext::String.compress_lines(<<-SQL)
        ALTER TABLE #{table_name}
        PARTITION BY RANGE(timestamp)
        (#{partition_sub_query})
      SQL
    end

  end
end
