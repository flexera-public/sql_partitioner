require File.expand_path("../spec_helper", File.dirname(__FILE__))
require 'ostruct'

class EventPartitionManagerSpec

  def self.test_partition_data(range, time)
    partition_data = []
    for i in range
      partition = OpenStruct.new
      partition.partition_timestamp = time + (i * 24 * 60 * 60 * 1_000_000)
      partition_data << partition
    end
    future_partition = OpenStruct.new
    future_partition.partition_timestamp = 'MAXVALUE'
    partition_data << future_partition
  end
end
describe "#EventPartitionManager" do
  before(:each) do
    @adapter = Object.new
    @ref_time = Time.mktime(2014,04,12)
    timestamp = @ref_time.to_i * 1_000_000
    @partition_manager = SqlPartitioner::AdvPartitionsManager.new(
      :adapter => @adapter,
      :current_timestamp => timestamp,
      :table_name        => 'events',
      :logger            => Logger.new(STDOUT),
      :time_unit         => :micro_seconds
    )
    test_partition_data = {"until_2014_01_01"=>"1388597901192257",
                           "until_2014_01_31"=>"1391189901192676",
                           "until_2014_03_02"=>"1393781901192921",
                           "until_2014_03_17"=>"1395077901193149",
                           "until_2014_04_01"=>"1396373901193398",
                           "until_2014_04_16"=>"1397669901193684",
                           "future"=>"MAXVALUE"}
    PartitionInfo = Struct.new(:partition_name, :partition_timestamp)
    @partition_info = test_partition_data.map do |key, value|
      value  =  value == "MAXVALUE" ? "MAXVALUE" : value.to_i
      PartitionInfo.new(key, value)
    end.sort do |x, y|
        if x.partition_timestamp == "MAXVALUE"
          1
        elsif y.partition_timestamp == "MAXVALUE"
          -1
        else
          x.partition_timestamp <=> y.partition_timestamp
        end
      end
    @adapter.stub(:schema_name).and_return('test_schema')
  end

  describe "#non_future_partitions" do
    context "when there is future partition" do
      it "should filter future partition" do
        actual = @partition_manager.partitions_fetcher.non_future_partitions(@partition_info)
        expect(actual.count).to eq(@partition_info.count - 1)
        expect(actual.detect { |p| p.partition_name == 'future' }).to be nil
      end
    end
    context "when input is empty" do
      it "should return empty array" do
        @partition_manager.partitions_fetcher.non_future_partitions([]).should be_empty
      end
    end
  end
  describe "#advance_partition_window" do
    before(:each) do
      time_now = @ref_time
      @policy = {:active_partition_start_date => time_now - (10 * 60 * 60 * 24),
                 :active_partition_end_date   => time_now + (15 * 60 * 60 * 24),
                 :partition_window_size_in_days => 10}
    end
    context "start and end date is proper" do
      it "should advance properly" do
        expected_drop_sql = "ALTER TABLE events DROP PARTITION "\
                            "until_2014_01_01,until_2014_01_31,"\
                            "until_2014_03_02,until_2014_03_17,"\
                            "until_2014_04_01"
        expected_reorg_sql = "ALTER TABLE events REORGANIZE PARTITION "\
                             "future INTO (PARTITION until_2014_04_26"\
                             " VALUES LESS THAN (1398533901193684),"\
                             "PARTITION until_2014_05_06 VALUES LESS THAN "\
                             "(1399397901193684),PARTITION future VALUES"\
                             " LESS THAN (MAXVALUE))"
        expected = {:drop_sql=>expected_drop_sql,
                    :reorg_sql=> expected_reorg_sql}
        @partition_manager.should_receive(:fetch_current_partition).and_return(nil)
        @partition_manager.should_receive(:fetch_partition_info_from_db).and_return(@partition_info)
        actual = @partition_manager.advance_partition_window(@policy, true)
        actual.should == expected
      end
    end
  end
  describe "_prep_params_for_advance_partition" do
    before(:each) do
      start_date = Time.mktime(2013,12,2)
      end_date =Time.mktime(2014,3,15)
      @policy = {:active_partition_start_date => start_date,
                 :active_partition_end_date   => end_date,
                 :partition_window_size_in_days => 10}
    end
    context "start date and end date is already partitioned" do
      it "should return empty array" do
        @partition_manager.should_receive(:fetch_partition_info_from_db).and_return(@partition_info)
        actual = @partition_manager.send(:_prep_params_for_advance_partition, @policy)
        actual.should == [[],{}]
      end
    end

    describe "with_lock_wait_timeout" do
      before(:each) do
        pending 'need db connection'

        @with_lock_wait_timeout = 1
      end
      context "with a new lock_wait_timeout value" do
        before(:each) do
          @orig_local_lock_wait_timeout  = DataMapper.repository.adapter.select("SELECT @@local.lock_wait_timeout").first
          @orig_global_lock_wait_timeout = DataMapper.repository.adapter.select("SELECT @@global.lock_wait_timeout").first

          @orig_local_lock_wait_timeout.should_not  == @with_lock_wait_timeout
          @orig_global_lock_wait_timeout.should_not == @with_lock_wait_timeout
        end
        it "should set and reset lock_wait_timeout" do
          @partition_manager.send(:with_lock_wait_timeout, @with_lock_wait_timeout) do
            DataMapper.repository.adapter.select("SELECT @@local.lock_wait_timeout").first.should      == @with_lock_wait_timeout
            DataMapper.repository.adapter.select("SELECT @@global.lock_wait_timeout").first.should_not == @with_lock_wait_timeout

            @partition_manager.adapter.execute("SELECT 1 FROM DUAL")
          end

          DataMapper.repository.adapter.select("SELECT @@local.lock_wait_timeout").first.should  == @orig_local_lock_wait_timeout
          DataMapper.repository.adapter.select("SELECT @@global.lock_wait_timeout").first.should == @orig_global_lock_wait_timeout
        end
      end
      context "with a second db connection" do
        before(:all) do
          skip "have to figure something out"
          # if the options passed are identical to the default repository, then no new connection is
          # opened but the existing one gets reused. Hence we merge some random stuff
          @ad = DataMapper.setup(:lock_events_connection, DataMapper.repository.adapter.options.merge("foobar" => 'something_random'))
        end
        after(:all) do
          # Not pretty to access a protected method if there is a cleaner way to close the connection, please let me know.
          DataMapper.repository(:lock_events_connection).adapter.send(:open_connection).dispose
        end
        context "and a new lock_wait_timeout getting set by the first db connection" do
          before(:each) do
            @orig_local_lock_wait_timeout  = DataMapper.repository(:lock_events_connection).adapter.select("SELECT @@local.lock_wait_timeout").first
            @orig_global_lock_wait_timeout = DataMapper.repository(:lock_events_connection).adapter.select("SELECT @@global.lock_wait_timeout").first

            @orig_local_lock_wait_timeout.should_not  == @with_lock_wait_timeout
            @orig_global_lock_wait_timeout.should_not == @with_lock_wait_timeout
          end
          it "should not affect the lock_wait_timeout value of the second db connection" do
            @partition_manager.send(:with_lock_wait_timeout, @with_lock_wait_timeout) do
              DataMapper.repository(:lock_events_connection).adapter.select("SELECT @@local.lock_wait_timeout").first.should_not  == @with_lock_wait_timeout
              DataMapper.repository(:lock_events_connection).adapter.select("SELECT @@global.lock_wait_timeout").first.should_not == @with_lock_wait_timeout

              @partition_manager.adapter.execute("SELECT 1 FROM DUAL")
            end
          end
        end
        context "and the second db connection holding a lock" do
          it "should timeout quickly" do
            @ad.execute("LOCK TABLE events WRITE")

            begin
              lambda do
                @partition_manager.send(:with_lock_wait_timeout, @with_lock_wait_timeout) do
                  @partition_manager.adapter.execute("LOCK TABLE events WRITE")
                end
              end.should raise_error(DataObjects::SQLError)
            ensure
              @ad.execute("UNLOCK TABLES")
            end
          end
        end
      end
    end

    context "when there is no max partition" do
      it "should raise exception" do
        future_partition = OpenStruct.new
        future_partition.partition_name = 'future'
        future_partition.partition_timestamp ='MAXVALUE'
        @partition_manager.should_receive(:fetch_partition_info_from_db).and_return([future_partition])
        lambda {
          @partition_manager.send(:_prep_params_for_advance_partition, @policy)
        }.should raise_error(/Atleast one non future partition expected, but none found/)
      end
    end
  end

  describe "#_build_partition_data" do
    context "when base timestamp is nil" do
      context "and end_timestamp into future" do
        it "should build correct data" do
          time_now = @ref_time.to_i * 1_000_000
          end_timestamp = time_now + (21 * 24 * 60 * 60 * 1_000_000)
          actual = @partition_manager.send(:_build_partition_data,
                                           nil, end_timestamp, 15)
          actual.size.should == 3

          end_timestamp = time_now + (10 * 24 * 60 * 60 * 1_000_000)
          actual = @partition_manager.send(:_build_partition_data,
                                           nil, end_timestamp, 20)
          actual.size.should == 2

          end_timestamp = time_now + (20 * 24 * 60 * 60 * 1_000_000)
          actual = @partition_manager.send(:_build_partition_data,
                                           nil, end_timestamp, 20)
          actual.size.should == 3
        end
      end
      context "and end_timestamp into past" do
        it "should return empty array" do
          time_now = @ref_time.to_i * 1_000_000
          end_timestamp = time_now - (21 * 24 * 60 * 60 * 1_000_000)
          actual = @partition_manager.send(:_build_partition_data,
                                           nil, end_timestamp, 15)
          actual.size.should == 0
        end
      end
    end
    context "when base timestamp > end timestamp" do
      it "should return empty array" do
        time_now = @ref_time.to_i * 1_000_000
        end_timestamp = time_now + (21 * 24 * 60 * 60 * 1_000_000)
        actual = @partition_manager.send(:_build_partition_data,
                                           end_timestamp+1, end_timestamp, 15)
        actual.size.should == 0
        actual = @partition_manager.send(:_build_partition_data,
                                           end_timestamp + 10, end_timestamp,
                                           15)
        actual.size.should == 0
      end
    end
    context "when base timestamp <= end timestamp" do
      it "should build correct data" do
        time_now = Time.now.to_i * 1_000_000
        end_timestamp = time_now + (30 * 24 * 60 * 60 * 1_000_000)
        base_timestamp = time_now - (20 * 24 * 60 * 60 * 1_000_000)

        actual = @partition_manager.send(:_build_partition_data,
                                           base_timestamp, end_timestamp, 15)
        actual.size.should == 4
        actual = @partition_manager.send(:_build_partition_data,
                                           end_timestamp, end_timestamp, 15)
        actual.size.should == 1
      end
    end
    context "when window size is 0" do
      it "should raise error" do
        lambda {
          @partition_manager.send(:_build_partition_data, nil, nil, 0)
        }.should raise_error(ArgumentError)
      end
    end
  end

  describe "#fetch_latest_partition" do
    context "when there is no latest partition" do
      before(:each) do
        partition = OpenStruct.new
        partition.partition_name = 'future'
        partition.partition_timestamp = 'MAXVALUE'
        @partition_manager.partitions_fetcher.should_receive(:fetch_partition_info_from_db).and_return([partition])
      end
      it "shoud return nil" do
        @partition_manager.partitions_fetcher.fetch_latest_partition.should be_nil
      end
    end
    context "when there is latest partition" do
      context "and into future" do
        before(:each) do
          @time_now = Time.now.to_i * 1_000_000
          @partition_info = EventPartitionManagerSpec.test_partition_data(-2..3,
                                                                          @time_now)
        end
        it "should return the latest partition" do
          expected_timestamp = @time_now + (3 * 24 * 60 * 60 * 1_000_000)
          actual = @partition_manager.partitions_fetcher.fetch_latest_partition(@partition_info)
          actual.should_not be_nil
          actual.partition_timestamp.should == expected_timestamp
        end
      end
      context "and into past" do
        before(:each) do
          @time_now = Time.now.to_i * 1_000_000
          @partition_info = EventPartitionManagerSpec.test_partition_data(-3..-1,
                                                                         @time_now)
        end

        it "should return the latest partition" do
          expected_timestamp = @time_now + (-1 * 24 * 60 * 60 * 1_000_000)
          actual = @partition_manager.partitions_fetcher.fetch_latest_partition(@partition_info)
          actual.should_not be_nil
          actual.partition_timestamp.should == expected_timestamp
        end
      end
    end
  end

  describe "#fetch_current_partition" do
    context "whent there is no current partition" do
      before(:each) do
        @time_now = @ref_time.to_i * 1_000_000
        @partition_info = EventPartitionManagerSpec.test_partition_data(-3..-1,
                                                                         @time_now)
      end
      it "should return nil" do
        @partition_manager.partitions_fetcher.fetch_current_partition(@partition_info).should be_nil
      end
    end
    context "when there is current_partition" do
      before(:each) do
        @time_now = @ref_time.to_i * 1_000_000
        @partition_info = EventPartitionManagerSpec.test_partition_data(-2..3,
                                                                        @time_now)
      end
      it "should return the latest partition" do
        expected_timestamp = @time_now + (1 * 24 * 60 * 60 * 1_000_000)
        actual = @partition_manager.partitions_fetcher.fetch_current_partition(@partition_info)
        actual.should_not be_nil
        actual.partition_timestamp.should == expected_timestamp
      end
    end
  end

  describe "#create_partition" do
    before(:each) do
      @expected = /ALTER TABLE events ADD PARTITION.*VALUES LESS THAN.*/
    end
    context "when dry_run" do
      it "should return sql" do
        @time_now = @ref_time.to_i * 1_000_000
        actual = @partition_manager.create_partition(@time_now, true)
        expect(actual.kind_of?(String)).to be true
        actual.should =~ @expected
      end
    end
    context "when no dry_run" do
      it "should execute query" do
        @time_now = @ref_time.to_i * 1_000_000
        @adapter.should_receive(:execute) do |sql|
                  sql.should =~ @expected
                  true
                end
        @partition_manager.should_receive(:display_partition_info).and_return(nil)
        @partition_manager.create_partition(@time_now)
      end
    end
  end


  describe "#drop_partitions" do
    before(:each) do
      @expected = "ALTER TABLE events DROP PARTITION hello,hello1"
    end
    context "when dry_run" do
      it "should return sql" do
        @time_now = @ref_time.to_i * 1_000_000
        @partition_manager.should_receive(:_validate_drop_partitions_params).and_return(true)
        actual = @partition_manager.drop_partitions(['hello', 'hello1'], true)
        expect(actual.kind_of?(String)).to be true
        actual.should == @expected
      end
    end
    context "when no dry_run" do
      it "should execute query" do
        @time_now = @ref_time.to_i * 1_000_000
        @partition_manager.should_receive(:_validate_drop_partitions_params).and_return(true)
        @adapter.should_receive(:execute).with(@expected).and_return(true)

        @partition_manager.should_receive(:display_partition_info).and_return(nil)
        @partition_manager.drop_partitions(['hello', 'hello1'])
      end
      context "and query is nil" do
        it "should not execute and return false" do
          @partition_manager.should_receive(:_validate_drop_partitions_params).and_return(true)
          @partition_manager.should_not_receive(:_execute_and_display_partition_info)
          expect(@partition_manager.send(:drop_partitions, [])).to be false
        end
      end
    end
  end

  describe "#_reorg_future_partition" do
    before(:each) do
      @partition_data = {'until_1' => 1, 'until_2' => 2 }
      @expected = "ALTER TABLE events REORGANIZE PARTITION future"\
                  " INTO (PARTITION until_1 VALUES LESS THAN (1),"\
                  "PARTITION until_2 VALUES LESS THAN (2),"\
                  "PARTITION future VALUES LESS THAN (MAXVALUE))"
    end
    context "when dry_run" do
      it "should return sql" do
        @time_now = @ref_time.to_i * 1_000_000
        actual = @partition_manager.send(:_reorg_future_partition,
                                         @partition_data, true)
        expect(actual.kind_of?(String)).to be true
        actual.should == @expected
      end
    end
    context "when no dry_run" do
      it "should execute query" do
        @time_now = Time.now.to_i * 1_000_000
        @adapter.should_receive(:execute).with(@expected).and_return(true)
        @partition_manager.should_receive(:display_partition_info).and_return(nil)
        @partition_manager.send(:_reorg_future_partition, @partition_data)

      end
      context "and query is nil" do
        it "should not execute and return false" do
          @partition_manager.should_not_receive(:_execute_and_display_partition_info)
          expect(@partition_manager.send(:_reorg_future_partition, {})).to be false
        end
      end
    end
  end

  describe "#initialize_partitioning" do
    before(:each) do
      @partition_data = {'until_1' => 1, 'until_2' => 2}
      @expected = "ALTER TABLE events PARTITION BY RANGE(timestamp) "\
                  "(PARTITION until_1 VALUES LESS THAN (1),"\
                  "PARTITION until_2 VALUES LESS THAN (2))"
    end
    context "when dry run" do
      it "should not execute query and return string" do
        actual = @partition_manager.initialize_partitioning(@partition_data, true)
        actual.should_not be_nil
        actual.should == @expected
      end
    end
    context "when not dry run" do
      it "should execute query" do
        @adapter.should_receive(:execute).with(@expected).and_return(true)
        @partition_manager.should_receive(:display_partition_info).and_return(nil)
        @partition_manager.initialize_partitioning(@partition_data)
      end
    end
  end

  describe "#initialize_partitioning_in_days" do
    context "when input is valid" do
      context "when dry_run" do
        it "should initialize with appropriate data" do
          @partition_manager.should_receive(:initialize_partitioning) do |params, dry_run|
            params['future'].should == 'MAXVALUE'
            params.size.should == 4
            expect(dry_run).to be false
            true
          end
          @partition_manager.initialize_partitioning_in_days([-15,0,15])
        end
      end

      context "when not dry_run" do
        it "should initialize with appropriate data" do
          @partition_manager.should_receive(:initialize_partitioning) do |params, dry_run|
            params['future'].should == 'MAXVALUE'
            params.size.should == 4
            expect(dry_run).to be true
            true
          end
          @partition_manager.initialize_partitioning_in_days([-15,0,15], true)
        end
      end
    end
  end

  describe "#partitions_older_than_timestamp" do
    before(:each) do
      @time_now = Time.now.to_i * 1_000_000
    end
    context "when there are no older partitions" do
      it "should return empty array" do
        @partition_info = EventPartitionManagerSpec.test_partition_data(2..4,
                                                                      @time_now)
        actual = @partition_manager.partitions_older_than_timestamp(@time_now,
                                                                    @partition_info)
        actual.should == []
      end
    end
    context "when there are older partitions" do
      it "should return empty array" do
        @partition_info = EventPartitionManagerSpec.test_partition_data(-4..-2,
                                                                      @time_now)
        actual = @partition_manager.partitions_older_than_timestamp(@time_now,
                                                                    @partition_info)
        actual.length == 3
      end
    end
  end

  describe "#_validate_timestamp" do
    context "when input is not valid" do
      it "should raise error" do
        lambda {
          @partition_manager.send(:_validate_timestamp, 'H')
        }.should raise_error(ArgumentError, /timestamp should be a positive integer/)
        lambda {
          @partition_manager.send(:_validate_timestamp, -1)
        }.should raise_error(ArgumentError, /timestamp should be a positive integer/)
        lambda {
          @partition_manager.send(:_validate_timestamp, nil)
        }.should raise_error(ArgumentError, /timestamp should be a positive integer/)
      end
    end
    context "when input is valid" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_timestamp, 10)).to be true
      end
    end
  end
  describe "#_validate_initialize_partitioning_in_days_params" do
    context "when params are valid" do
      it "should return true" do
        input = [-10, 20]
        expect(@partition_manager.send(:_validate_initialize_partitioning_in_days_params, input)).to be true
      end
    end
    context "when params are in valid" do
      it "should raise error" do
        input = "hello"
        lambda {
          @partition_manager.send(:_validate_initialize_partitioning_in_days_params, input)
        }.should raise_error(ArgumentError, /days should be Array.*String found/)
        input = ["hello", 1]
        lambda {
          @partition_manager.send(:_validate_initialize_partitioning_in_days_params, input)
        }.should raise_error(ArgumentError, /hello should be Integer.*/)
      end
    end
  end

  describe "#_validate_initialize_partitioning_params" do
    context "when params are valid" do
      it "should return true" do
        input = {'until_1' => 1234}
        expect(@partition_manager.send(:_validate_initialize_partitioning_params, input)).to be true
      end
    end
    context "when params are in valid" do
      it "should raise error" do
        input = "hello"
        lambda {
          @partition_manager.send(:_validate_initialize_partitioning_params, input)
        }.should raise_error(ArgumentError, /.*should be Hash.*String found/)
        input = {'until_1' => "1234"}
        lambda {
          @partition_manager.send(:_validate_initialize_partitioning_params, input)
        }.should raise_error(ArgumentError, /partition timestamp:1234 should be.*/)
        input = {123 => "1234"}
        lambda {
          @partition_manager.send(:_validate_initialize_partitioning_params, input)
        }.should raise_error(ArgumentError, /partition name:123 should be.*/)
      end
    end
  end

  describe "#_validate_drop_partitions_params" do
    context "when params are valid" do
      it "should return true" do
        input = ["hello", "test124"]
        @partition_manager.should_receive(:fetch_current_partition).and_return(nil)
        expect(@partition_manager.send(:_validate_drop_partitions_params, input)).to be true
      end
    end
    context "when params are in valid" do
      it "should raise error" do
        input = "hello"
        lambda {
          @partition_manager.send(:_validate_drop_partitions_params, input)
        }.should raise_error(ArgumentError, /should be array.*String found/)
        input = ["hello", 1]
        lambda {
          @partition_manager.send(:_validate_drop_partitions_params, input)
        }.should raise_error(ArgumentError, /Invalid value 1.*found/)
        @partition_manager.should_receive(:fetch_current_partition).and_return(nil)
        input = ["future", "hello"]
        lambda {
          @partition_manager.send(:_validate_drop_partitions_params, input)
        }.should raise_error(ArgumentError, /current and.*dropped/)
        current_partition = OpenStruct.new
        current_partition.partition_name = 'test123'
        @partition_manager.should_receive(:fetch_current_partition).and_return(current_partition)
        input = ["test123","hello"]
        lambda {
          @partition_manager.send(:_validate_drop_partitions_params, input)
        }.should raise_error(ArgumentError, /current and.*dropped/)
      end
    end
  end
end
