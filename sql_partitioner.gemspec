# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sql_partitioner/version'

Gem::Specification.new do |gem|
  gem.name             = "sql_partitioner"
  gem.version          = SqlPartitioner::VERSION

  gem.authors          = ['Jim Slattery, Dominic Metzger, Sumner McCarty, Prakash Selvaraj']
  gem.date             = "2014-09-17"

  gem.summary          = %q{SQL Partitioning.}
  gem.description      = %q{SQL Partitioning}
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
#  gem.add_development_dependency('ruby-debug',   '0.10.4')
end
