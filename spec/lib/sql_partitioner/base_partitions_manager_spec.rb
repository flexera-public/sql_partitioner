require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe "BasePartitionsManager with ARAdapter" do
  describe "#initialize_partitioning" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => Logger.new(STDOUT)
      )
    end

    context "with some partitions passed" do
      it "should create the future partition and the partition specified" do
        @partition_manager.initialize_partitioning({'until_2014_03_17' => 1395014400})
        SqlPartitioner::Partition.all(@ar_adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ["until_2014_03_17", 1395014400],
          ["future", "MAXVALUE"]
        ]
      end
    end

    context "with no partition passed" do
      it "should create the future partition" do
        @partition_manager.initialize_partitioning({})
        SqlPartitioner::Partition.all(@ar_adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ["future", "MAXVALUE"]
        ]
      end
    end
  end

  describe "#drop_partitions" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,16),
        :table_name   => 'test_events',
        :logger       => Logger.new(STDOUT)
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
        SqlPartitioner::Partition.all(@ar_adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
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
        @partition_to_drop = {SqlPartitioner::Partition::FUTURE_PARTITION_NAME => SqlPartitioner::Partition::FUTURE_PARTITION_NAME}
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
      it "should fail to drop the future partition" do
        lambda do
          @partition_manager.drop_partitions(@partition_to_drop.keys)
        end.should raise_error(ActiveRecord::StatementInvalid)
      end
    end
  end

  describe "#reorg_future_partition" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,16),
        :table_name   => 'test_events',
        :logger       => Logger.new(STDOUT)
      )

      @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
      @partition_manager.initialize_partitioning(@partitions)
    end

    context "with reorganizing the future partition into a partition with timestamp < existing partititon" do
      before(:each) do
        @partition_to_reorg = {'until_2014_02_17' => 1392595200}
      end
      it "should fail to reorganize the future partition" do
        lambda do
          @partition_manager.reorg_future_partition(@partition_to_reorg)
        end.should raise_error(ActiveRecord::StatementInvalid)
      end
    end
    context "with reorganizing the future partition into one new partition with timestamp == existing partititon" do
      before(:each) do
        @partition_to_reorg = {'until_2014_04_17' => 1397692800}
      end
      it "should fail to reorganize the future partition" do
        lambda do
          @partition_manager.reorg_future_partition(@partition_to_reorg)
        end.should raise_error(ActiveRecord::StatementInvalid)
      end
    end
    context "with reorganizing the future partition into one new partition with timestamp > existing partititon" do
      before(:each) do
        @partition_to_reorg = {'until_2014_05_17' => 1400284800}
      end
      it "should succeed in reorganizing the future partition" do
        @partition_manager.reorg_future_partition(@partition_to_reorg)
        SqlPartitioner::Partition.all(@ar_adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ['until_2014_03_17', 1395014400],
          ['until_2014_04_17', 1397692800],
          ['until_2014_05_17', 1400284800],
          ["future",           "MAXVALUE"]
        ]
      end
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
      :logger       => Logger.new(STDOUT)
    )
  end

  describe "#_validate_partition_data" do
    context "when input is not valid" do
      it "should raise error when future partion is not pointing to proper value" do
        lambda {
          @partition_manager.send(:_validate_partition_data, {
              SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_NAME => 1
            }
          )
        }.should raise_error(ArgumentError, /future partion name/)
      end
      it "should raise error when future partion is not pointing to proper value" do
        lambda {
          @partition_manager.send(:_validate_partition_data, {
              "name" => SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_VALUE
            }
          )
        }.should raise_error(ArgumentError, /future partion name/)
      end
    end
    context "when input is valid" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_partition_data, {
            SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_NAME =>
            SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_VALUE
          }
        )).to be true
      end
    end
  end

  describe "#_validate_positive_fixnum" do
    context "when input is not valid" do
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

  describe "#_validate_fixnum" do
    context "when input is not valid" do
      it "should raise error with a String" do
        lambda {
          @partition_manager.send(:_validate_fixnum, :timestamp,'H')
        }.should raise_error(ArgumentError, /expected to be fixnum but String found/)
      end
      it "should raise error with nil" do
        lambda {
          @partition_manager.send(:_validate_fixnum, :timestamp, nil)
        }.should raise_error(ArgumentError, /expected to be fixnum but NilClass found/)
      end
    end
    context "when input is valid" do
      it "should return true" do
        expect(@partition_manager.send(:_validate_fixnum, :timestamp, 10)).to be true
        expect(@partition_manager.send(:_validate_fixnum, :timestamp, -10)).to be true
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
        }.should raise_error(ArgumentError, /String expected/)
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
        }.should raise_error(ArgumentError, /should be array/)
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
