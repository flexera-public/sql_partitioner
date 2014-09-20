require File.expand_path("../spec_helper", File.dirname(__FILE__))
require 'ostruct'

describe "BasePartitionsManager" do
  before(:each) do
    @adapter = Struct.new(:schema_name).new("sql_partitioner_test")

    @partition_manager = SqlPartitioner::BasePartitionsManager.new(
      :adapter           => @adapter,
      :current_timestamp => Time.mktime(2014,04,12).to_i,
      :table_name        => 'events',
      :logger            => Logger.new(STDOUT)
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

  describe "#_validate_timestamp" do
    context "when input is not valid" do
      it "should raise error with a String" do
        lambda {
          @partition_manager.send(:_validate_timestamp, 'H')
        }.should raise_error(ArgumentError, /timestamp should be a positive integer/)
      end
      it "should raise error with negative integer" do
        lambda {
          @partition_manager.send(:_validate_timestamp, -1)
        }.should raise_error(ArgumentError, /timestamp should be a positive integer/)
      end
      it "should raise error with nil" do
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
        ts = Time.mktime(2014, 01, 15)

        result = @partition_manager.send(:name_from_timestamp, ts.to_i)
        expect(result).to eq("until_2014_01_15")
      end
    end
  end

end
