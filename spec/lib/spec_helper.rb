require 'sql_partitioner'

require 'logger'

#require 'ruby-debug' # enable debugger support

require 'active_record'

# enable both should and expect syntax in rspec without deprecation warnings
RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with :rspec do |c|
    c.syntax = [:should, :expect]
  end

  config.before :all do
    require 'yaml'
    db_conf = YAML.load_file('spec/db_conf.yml')

    ActiveRecord::Base.establish_connection(db_conf["test"])

    sql = <<-SQL
      CREATE TABLE IF NOT EXISTS `events` (
        `id` bigint(20) NOT NULL AUTO_INCREMENT,
        `timestamp` bigint(20) unsigned NOT NULL DEFAULT '0',
        PRIMARY KEY (`id`,`timestamp`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    SQL
    ActiveRecord::Base.connection.execute(sql)
  end
end
