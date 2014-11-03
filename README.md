# SqlPartitioner
[![Build Status](https://travis-ci.org/rightscale/sql_partitioner.png)](https://travis-ci.org/rightscale/sql_partitioner)

SqlPartitioner provides a `PartitionManager` class to help maintain partitioned tables in MySQL.
If you have a table that is partitioned based on a timestamp, you will likely need to regularly add new partitions 
into the future as well as remove older partitions to free up space. This gem will help.

## Getting Started
You'll need to `require 'sql_partitioner'`.

Here's an example for initializing a `PartitionManager` instance, using `DataMapper`:

```ruby
partition_manager = SqlPartitioner::PartitionsManager.new(
    :lock_wait_timeout => 1,
    :adapter           => SqlPartitioner::DMAdapter.new(DataMapper.repository.adapter),
    :table_name        => 'my_partitioned_table',
    :logger            => Logger.new(STDOUT),
    :time_unit         => :micro_seconds
)
```

If you are using `ActiveRecord`, you can instead supply the following for `:adapter`:
```ruby
SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)
```

Here's an example for initializing partitioning on the table. It will create partitions of size 30 days, as needed, to cover 90 days into the future:

```ruby
days_into_future = 90
partition_size = 30
partition_size_unit = :days
dry_run = false
partition_manager.initialize_partitioning_in_intervals(days_into_future, partition_size_unit, partition_size, dry_run)
```

Here's an example for appending partitions to cover time periods into the future. It will create partitions of size 30 days, as needed, to cover 180 days into the future:

```ruby
days_into_future = 180
partition_size = 30
partition_size_unit = :days
dry_run = false
partition_manager.append_partition_intervals(partition_size_unit, partition_size, days_into_future, dry_run)
```

Here's an example for dropping partitions as needed to only cover 360 days of the past:

```ruby
days_into_past = 360
dry_run = false
partition_manager.drop_partitions_older_than_in_days(days_into_past, dry_run)
```

These operations can be helpful when creating a rake task that can initialize partitioning for a given table, and add/remove partitions as needed.

## Supported Features
SqlPartitioner works with MySQL partitioned tables that are partitioned by a timestamp, expressed as an integer representing either
seconds or micro-seconds.

You can use DataMapper or ActiveRecord.

Supported functionality:

- initializing partitioning on a table
- adding new partitions of a given size (expressed in months or days)
- removing partitions older than a given timestamp or number of days

You can run the above operations directly or pass a flag to only do a dry-run.

## Unsupported Features

Does not yet support databases other than MySQL.

## TODO

## Compatibility
Tested with Ruby 1.8.7 and 2.1.2.

## Contributing
Pull requests welcome.

## Maintained by

- [Dominic Metzger](https://github.com/dominicm)
- [Sumner McCarty](https://github.com/sumner-mccarty)
- [Jim Slattery](https://github.com/jim-slattery-rs)

## License
MIT License, see [LICENSE](LICENSE)
