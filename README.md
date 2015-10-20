# SqlPartitioner
[![Build Status](https://travis-ci.org/rightscale/sql_partitioner.png?branch=master)](https://travis-ci.org/rightscale/sql_partitioner)
[![Gem Version](https://badge.fury.io/rb/sql_partitioner.svg)](http://badge.fury.io/rb/sql_partitioner)
[![Coverage Status](https://coveralls.io/repos/rightscale/sql_partitioner/badge.svg?branch=master&service=github)](https://coveralls.io/github/rightscale/sql_partitioner?branch=master)
[![Dependency Status](https://gemnasium.com/rightscale/sql_partitioner.svg)](https://gemnasium.com/rightscale/sql_partitioner)

SqlPartitioner provides a `PartitionsManager` class to help maintain partitioned tables in MySQL.
If you have a table that is partitioned based on a timestamp, you will likely need to regularly add new partitions 
into the future as well as remove older partitions to free up space. This gem will help.

## Supported Features
SqlPartitioner works with MySQL partitioned tables that are partitioned by a `timestamp` column, expressed as an integer 
representing a Unix epoch timestamp in either seconds or micro-seconds.

You can use ActiveRecord or DataMapper.

Supported functionality:

- initializing partitioning on a table
- adding new partitions of a given size (expressed in months or days)
- removing partitions older than a given timestamp or number of days

You can run the above operations directly or pass a flag to only do a dry-run.

## Unsupported Features

Does not yet support databases other than MySQL. Target table can only be partitioned by its `timestamp` column representing seconds or micro-seconds.

## Getting Started
You'll need to `require 'sql_partitioner'`.

Here's an example for initializing a `PartitionsManager` instance, using `DataMapper`:

```ruby
partition_manager = SqlPartitioner::PartitionsManager.new(
    :table_name        => 'my_partitioned_table', # target table for partitioning operations
    :time_unit         => :micro_seconds, # or :seconds, as appropriate for the table's `timestamp` column
    :lock_wait_timeout => 1, #(seconds)
    :adapter           => SqlPartitioner::DMAdapter.new(DataMapper.repository.adapter),
    :logger            => Logger.new(STDOUT)
)
```

If you are using `ActiveRecord`, you can instead supply the following for `:adapter`:
```ruby
SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)
```

Regarding the `:lock_wait_timeout` option: any partitioning statement must acquire a table lock on the partitioned table, 
and while it is waiting to acquire this lock, any subsequent queries on that table will be blocked and have to wait. 
It may take a long time to acquire a table lock if there were already long-running queries in progress. 
Therefore, setting a short timeout (e.g. 1 second) ensures the partitioning statement will timeout quickly,
 so any other SQL operations on that table will not be delayed. 
If the partitioning command times-out, it will have to be retried later. 
MySQL's default value for [lock_wait_timeout](http://dev.mysql.com/doc/refman/5.5/en/server-system-variables.html#sysvar_lock_wait_timeout) is 1 year.

### Initialize partitioning
Here's an example for initializing partitioning on the table. It will create partitions of size 30 days, as needed, to cover 90 days into the future:

```ruby
days_into_future = 90
partition_size = 30
partition_size_unit = :days
dry_run = false
partition_manager.initialize_partitioning_in_intervals(days_into_future, partition_size_unit, partition_size, dry_run)
```

### Adding partitions
Here's an example for appending partitions to cover time periods into the future. It will create partitions of size 30 days, as needed, to cover 180 days into the future:

```ruby
days_into_future = 180
partition_size = 30
partition_size_unit = :days
dry_run = false
partition_manager.append_partition_intervals(partition_size_unit, partition_size, days_into_future, dry_run)
```

Here's an example for appending a single partition with the given name and "until" timestamp (using microseconds in this case):

```ruby
partition_data = {'until_2014_11_01' => 1414870869000000}
dry_run = false
partition_manager.reorg_future_partition(partition_data, dry_run)
```

### Dropping partitions
Here's an example for dropping partitions as needed to only cover 360 days of the past:

```ruby
days_into_past = 360
dry_run = false
partition_manager.drop_partitions_older_than_in_days(days_into_past, dry_run)
```

Here's an example for dropping a single partition, `until_2014_11_01`, by name: 

```ruby
partition_names = ['until_2014_11_01']
dry_run = false
partition_manager.drop_partitions(partition_names, dry_run)
```

### Suggested use:
The above operations can be helpful when creating a rake task that can initialize partitioning for a given table, 
and gets called periodically to add and remove partitions as needed.

## Compatibility
Tested with Ruby 1.8.7 and 2.1.2, and MySQL 5.5.

## Contributing
Pull requests welcome.

## Maintained by

- [Dominic Metzger](https://github.com/dominicm)
- [Sumner McCarty](https://github.com/sumner-mccarty)
- [Prakash Selvaraj](https://github.com/PrakashSelvaraj)
- [Jim Slattery](https://github.com/jim-slattery-rs)

## License
MIT License, see [LICENSE](LICENSE)
