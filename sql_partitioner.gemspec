# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sql_partitioner/version'

Gem::Specification.new do |gem|
  gem.name             = "sql_partitioner"
  gem.version          = SqlPartitioner::VERSION

  gem.authors          = ['Dominic Metzger', 'Sumner McCarty', 'Prakash Selvaraj', 'Jim Slattery']
  gem.date             = "2014-11-04"

  gem.summary          = %q{SqlPartitioner helps maintain partitioned tables in MySQL.}
  gem.description      = <<-EOF
    This gem will help setup partitioning on a table, based on its `timestamp` column.
    Once you have a table that is partitioned based on a timestamp, you will likely need to
    regularly add new partitions into the future, and drop older partitions to free up space.
    This gem can help carry out such routine activities as well.
  EOF
  gem.homepage         = "https://github.com/rightscale/sql_partitioner"
  gem.email            = 'support@rightscale.com'
  gem.licenses         = ["MIT"]

  gem.files            = `git ls-files`.split($/)
  gem.executables      = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files       = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths    = [ "lib" ]
  gem.extra_rdoc_files = [ "LICENSE", "README.md" ]
  gem.rubygems_version = "1.8.26"


  # ---------------------------------------------------------------------
  # Test suite
  # ---------------------------------------------------------------------
  gem.add_development_dependency("rspec",        '3.0.0')
  gem.add_development_dependency("simplecov",    '0.9.1')

  gem.add_development_dependency("mysql", "2.8.1")
  gem.add_development_dependency("activerecord", '3.0.0')

  #-- DataMapper --------------------------------------------------------
  do_gems_version   = "0.10.7"
  dm_gems_version   = "1.2.0"

  gem.add_development_dependency("data_objects", do_gems_version)
  gem.add_development_dependency("do_mysql",     do_gems_version)

  gem.add_development_dependency('data_mapper',       dm_gems_version)
  gem.add_development_dependency('dm-mysql-adapter',  dm_gems_version)

#  gem.add_development_dependency('ruby-debug',   '0.10.4')
end
