require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe "Partition" do
  describe "#current_partition" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'events',
        :logger       => Logger.new(STDOUT)
      )

      @partitions = {'until_2014_03_17' => 1395077901193149}
      @partition_manager.initialize_partitioning(@partitions)
    end

    it "should return nil when timestamp > partition.timestamp" do
      existing_partition_ts = @partitions.values.first
      SqlPartitioner::Partition.all(@ar_adapter, 'events').current_partition(existing_partition_ts + 1).should == nil
    end
    it "should return nil when timestamp == partition.timestamp" do
      existing_partition_ts = @partitions.values.first
      SqlPartitioner::Partition.all(@ar_adapter, 'events').current_partition(existing_partition_ts).should == nil
    end
    it "should return the partition when timestamp < partition.timestamp" do
      existing_partition_ts = @partitions.values.first
      SqlPartitioner::Partition.all(@ar_adapter, 'events').current_partition(existing_partition_ts - 1).name.should == @partitions.keys.first
    end
  end
end