require 'sql_partitioner'

require 'logger'

#require 'ruby-debug' # enable debugger support

# enable both should and expect syntax in rspec without deprecation warnings
RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end
