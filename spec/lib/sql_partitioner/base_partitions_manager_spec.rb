require File.expand_path("../spec_helper", File.dirname(__FILE__))

shared_examples_for "BasePartitionsManager with an adapter" do
  describe "#initialize_partitioning" do
    before(:each) do
      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      )
    end

    context "with some partitions passed" do
      it "should create the future partition and the partition specified" do
        @partition_manager.initialize_partitioning({'until_2014_03_17' => 1395014400})
        SqlPartitioner::Partition.all(adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ["until_2014_03_17", 1395014400],
          ["future", "MAXVALUE"]
        ]
      end
    end

    context "with no partition passed" do
      it "should create the future partition" do
        @partition_manager.initialize_partitioning({})
        SqlPartitioner::Partition.all(adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ["future", "MAXVALUE"]
        ]
      end
    end
  end

  describe "#reorg_future_partition" do
    before(:each) do
      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      )
    end

    context "without the future partition passed" do
      it "should add the future partition and reorganize it" do
        @partition_manager.initialize_partitioning({})

        @partition_manager.reorg_future_partition({'until_2014_03_17' => 1395014400})
        SqlPartitioner::Partition.all(adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ["until_2014_03_17", 1395014400],
          ["future", "MAXVALUE"]
        ]
      end
    end
  end

  describe "#_execute_and_display_partition_info" do
    before(:each) do
      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      )
    end
    context "with no sql statement to be executed" do
      it "should return false" do
        expect(@partition_manager.send(:_execute_and_display_partition_info, nil)).to be false
      end
    end
    context "with sql statement to be executed" do
      before(:each) do
        @sql_statement = "SELECT database()"
      end
      context "and dry_run == true" do
        before(:each) do
          @dry_run = true
        end
        it "should return the sql statement" do
          expect(@partition_manager.send(:_execute_and_display_partition_info, @sql_statement, @dry_run)).to be @sql_statement
        end
      end
      context "and dry_run == false" do
        before(:each) do
          @dry_run = false
        end
        it "should execute the sql statement & return true" do
          expect(@partition_manager.send(:_execute_and_display_partition_info, @sql_statement, @dry_run)).to be true
        end
      end
    end
  end

  describe "#drop_partitions" do
    before(:each) do
      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => adapter,
        :current_time => Time.utc(2014,04,16),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      )

      @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
      @partition_manager.initialize_partitioning(@partitions)
    end

    context "with an attempt to drop a regular partition" do
      before(:each) do
        @partition_to_drop = {'until_2014_03_17' => 1395014400}
      end
      it "should drop the partition specified" do
        @partition_manager.drop_partitions(@partition_to_drop.keys)
        SqlPartitioner::Partition.all(adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ['until_2014_04_17', 1397692800],
          ["future", "MAXVALUE"]
        ]
      end
    end
    context "with an attempt to drop the current partition" do
      before(:each) do
        @partition_to_drop = {'until_2014_04_17' => 1397692800}
      end
      it "should fail to drop the current partition" do
        lambda do
          @partition_manager.drop_partitions(@partition_to_drop.keys)
        end.should raise_error(ArgumentError)
      end
    end
    context "with an attempt to drop the future partition" do
      before(:each) do
        @partition_to_drop = {SqlPartitioner::Partition::FUTURE_PARTITION_NAME => SqlPartitioner::Partition::FUTURE_PARTITION_VALUE}
      end
      it "should fail to drop the future partition" do
        lambda do
          @partition_manager.drop_partitions(@partition_to_drop.keys)
        end.should raise_error(ArgumentError)
      end
    end
    context "with an attempt to drop a non-existing partition" do
      before(:each) do
        @partition_to_drop = {'until_2014_02_17' => 1392595200}
      end
      it "should fail to drop the non-existent partition" do
        lambda do
          @partition_manager.drop_partitions(@partition_to_drop.keys)
        end.should raise_error(statement_invalid_exception_class)
      end
    end
  end

  describe "#reorg_future_partition" do
    before(:each) do
      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => adapter,
        :current_time => Time.utc(2014,04,16),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      )

      @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
      @partition_manager.initialize_partitioning(@partitions)
    end

    context "with reorganizing the future partition into a partition with timestamp < existing partition" do
      before(:each) do
        @partition_to_reorg = {'until_2014_02_17' => 1392595200}
      end
      it "should fail to reorganize the future partition" do
        lambda do
          @partition_manager.reorg_future_partition(@partition_to_reorg)
        end.should raise_error(statement_invalid_exception_class)
      end
    end
    context "with reorganizing the future partition into one new partition with timestamp == existing partition" do
      before(:each) do
        @partition_to_reorg = {'until_2014_04_17' => 1397692800}
      end
      it "should fail to reorganize the future partition" do
        lambda do
          @partition_manager.reorg_future_partition(@partition_to_reorg)
        end.should raise_error(statement_invalid_exception_class)
      end
    end
    context "with reorganizing the future partition into one new partition with timestamp > existing partition" do
      before(:each) do
        @partition_to_reorg = {'until_2014_05_17' => 1400284800}
      end
      it "should succeed in reorganizing the future partition" do
        @partition_manager.reorg_future_partition(@partition_to_reorg)
        SqlPartitioner::Partition.all(adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ['until_2014_03_17', 1395014400],
          ['until_2014_04_17', 1397692800],
          ['until_2014_05_17', 1400284800],
          ["future",           "MAXVALUE"]
        ]
      end
    end
  end
end

describe "BasePartitionsManager with ARAdapter" do
  it_should_behave_like "BasePartitionsManager with an adapter" do
    let(:statement_invalid_exception_class) do
      ActiveRecord::StatementInvalid
    end
    let(:adapter) do
      SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)
    end
  end

  # TODO: Find a way to make this test also work for DM
  describe "#_execute" do
    before(:each) do
      @options = {
        :adapter      => SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection),
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      }
      @sql_statement = "SELECT @@local.lock_wait_timeout AS lock_wait_timeout"
    end

    context "with a timeout" do
      before(:each) do
        @partition_manager = SqlPartitioner::BasePartitionsManager.new(
          @options.merge(:lock_wait_timeout => 1)
        )
      end
      it "should return the result after changing lock_wait_timeout" do
        result = @partition_manager.send(:_execute, @sql_statement)
        result.each_hash{|hash| hash["lock_wait_timeout"].should == "1"}
      end
    end
    context "with no timeout" do
      before(:each) do
        @partition_manager = SqlPartitioner::BasePartitionsManager.new(@options)
      end
      it "should return the result without changing lock_wait_timeout" do
        result = @partition_manager.send(:_execute, @sql_statement)
        result.each_hash{|hash| hash["lock_wait_timeout"].should == "31536000" }
      end
    end
  end
end

describe "BasePartitionsManager with DM Adapter" do
  it_should_behave_like "BasePartitionsManager with an adapter" do
    let(:statement_invalid_exception_class) do
      DataObjects::SQLError
    end
    let(:adapter) do
      SqlPartitioner::DMAdapter.new(DataMapper.repository.adapter)
    end
  end
end

describe "BasePartitionsManager" do
  before(:each) do
    @adapter = Struct.new(:schema_name).new("sql_partitioner_test")

    @partition_manager = SqlPartitioner::BasePartitionsManager.new(
      :adapter      => @adapter,
      :current_time => Time.utc(2014,04,18),
      :table_name   => 'test_events',
      :logger       => SPEC_LOGGER
    )
  end

  describe "#log" do
    before(:each) do
      @to_log = "Log this!"
    end
    it "should not log a prefix with no prefix" do
      @partition_manager.logger.should_receive(:info).with(@to_log)
      @partition_manager.log(@to_log, prefix = false)
    end
    it "should log a prefix with a prefix" do
      @partition_manager.logger.should_receive(:info).with("[SqlPartitioner::BasePartitionsManager]#{@to_log}")
      @partition_manager.log(@to_log, prefix = true)
    end
  end

  describe "#_validate_partition_data" do
    context "when input is not valid" do
      it "should raise error when future partition is not pointing to proper value" do
        lambda {
          @partition_manager.send(:_validate_partition_data, {
              SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_NAME => 1
            }
          )
        }.should raise_error(ArgumentError, /future partition name/)
      end
      it "should raise error when non-future-partition is pointing to future-partition value" do
        lambda {
          @partition_manager.send(:_validate_partition_data, {
              "name" => SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_VALUE
            }
          )
        }.should raise_error(ArgumentError, /future partition name/)
      end
    end
    context "when input is valid" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_partition_data, {
            SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_NAME =>
            SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_VALUE
          }
        )).to be true
        expect(@partition_manager.send(:_validate_partition_data, { "any_string" => 123 })).to be true
      end
    end
  end

  describe "#_validate_positive_fixnum" do
    context "when input is not valid" do
      it "should raise error with a String" do
        lambda {
          @partition_manager.send(:_validate_positive_fixnum, :timestamp, 'H')
        }.should raise_error(ArgumentError, /expected to be Fixnum but instead was String/)
      end
      it "should raise error with nil" do
        lambda {
          @partition_manager.send(:_validate_positive_fixnum, :timestamp, nil)
        }.should raise_error(ArgumentError, /expected to be Fixnum but instead was NilClass/)
      end

      it "should raise error with negative integer" do
        lambda {
          @partition_manager.send(:_validate_positive_fixnum, :timestamp, -1)
        }.should raise_error(ArgumentError, /timestamp should be > 0/)
      end
    end
    context "when input is valid" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_positive_fixnum, :timestamp, 10)).to be true
      end
    end
  end

  describe "#_validate_class" do
    context "when class is not correct" do
      it "should raise error when a String provided when Integer expected" do
        lambda {
          @partition_manager.send(:_validate_class, :timestamp, 'string', Integer)
        }.should raise_error(ArgumentError, /expected to be Integer but instead was String/)
      end
      it "should raise error when nil provided when Fixnum expected" do
        lambda {
          @partition_manager.send(:_validate_class, :timestamp, nil, Fixnum)
        }.should raise_error(ArgumentError, /expected to be Fixnum but instead was NilClass/)
      end
    end
    context "when class is as expected" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_class, :timestamp, 'example', String)).to be true
        expect(@partition_manager.send(:_validate_class, :timestamp, {:a => 1}, Hash)).to be true
      end
    end
  end

  describe "#_validate_timestamp" do
    it "should return true if the future partition value is passed" do
      @partition_manager.send(:_validate_timestamp, SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_VALUE)
    end
  end

  describe "#_validate_partition_name" do
    context "when input is not valid" do
      it "should raise error when not String" do
        lambda {
          @partition_manager.send(:_validate_partition_name, 1)
        }.should raise_error(ArgumentError, /expected to be String/)
      end
    end
    context "when input is valid" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_partition_name, "bar")).to be true
      end
    end
  end

  describe "#_validate_partition_names" do
    context "when input is not valid" do
      it "should raise error when not an array is passed" do
        lambda {
          @partition_manager.send(:_validate_partition_names, {})
        }.should raise_error(ArgumentError, /expected to be Array/)
      end
      it "should raise error when not an array of strings is passed" do
        lambda {
          @partition_manager.send(:_validate_partition_names, [123])
        }.should raise_error(ArgumentError, /expected to be String/)
      end
    end
    context "when input is valid" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_partition_names, ["bar"])).to be true
      end
    end
  end

  describe "#_validate_partition_names_allowed_to_drop" do
    before(:each) do
      @current_partition = Struct.new(:partition_name, :partition_description).new("current", @partition_manager.current_timestamp.to_i + 1)
      @adapter.should_receive(:select).and_return([@current_partition])
    end
    context "when input is not valid" do
      it "should raise error with name of the future partition" do
        lambda {
          @partition_manager.send(:_validate_partition_names_allowed_to_drop, ["future"])
        }.should raise_error(ArgumentError, /current and future partition can never be dropped/)
      end
      it "should raise error with name of current partition" do
        lambda {
          @partition_manager.send(:_validate_partition_names_allowed_to_drop, [@current_partition.partition_name])
        }.should raise_error(ArgumentError, /current and future partition can never be dropped/)
      end
    end
    context "when input is valid" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_partition_names_allowed_to_drop, ["bar"])).to be true
      end
    end
  end

  describe "#name_from_timestamp" do
    context "with future partition timestamp" do
      it "should return future partition name" do
        result = @partition_manager.send(:name_from_timestamp, SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_VALUE)
        expect(result).to eq(SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_NAME)
      end
    end
    context "with a partition timestamp" do
      it "should return future partition name" do
        ts = Time.utc(2014, 01, 15)

        result = @partition_manager.send(:name_from_timestamp, ts.to_i)
        expect(result).to eq("until_2014_01_15")
      end
    end
  end
end