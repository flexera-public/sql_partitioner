## 0.5.0

Features:

  - Added support for running specs in Travis CI
  - Improved specs to exercise both database adapters (ActiveRecord, DataMapper)
  
Bugfixes:

  - in Ruby 1.8.7 fixed `SqlPartitioner::Partition#to_log` output 

## 0.4.0 / 2014-10-02

Features:

  - Added development dependency: SimpleCov
  - Improved test coverage
  
Bugfixes:
  
  - Fixed return value for `_execute_and_display_partition_info` when SQL is executed.
    Before, it returned whatever `logger.info` happened to return. Now it returns true.