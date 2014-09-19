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

  describe "#_validate_partition_names" do
    context "when input is not valid" do
      it "should raise error when not an array is passed" do
        lambda {
          @partition_manager.send(:_validate_partition_names, {})
        }.should raise_error(ArgumentError, /should be array/)
      end
      it "should raise error when not every value is a String" do
        lambda {
          @partition_manager.send(:_validate_partition_names, ["foo", 1])
        }.should raise_error(ArgumentError, /String expected/)
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
end
