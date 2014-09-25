require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe "SqlHelper" do
  before(:each) do
    @table_name = 'my_table'
  end
  describe ".drop_partitions" do
    it "should return nil when no partiton_names are passed" do
      SqlPartitioner::SQL.drop_partitions(@table_name, []).should == nil
    end
  end

  describe ".reorg_partitions" do
    it "should return nil when no partiton_names are passed" do
      SqlPartitioner::SQL.reorg_partitions(@table_name, [], "future").should == nil
    end
  end

  describe ".sort_partition_data" do
    before(:each) do
      @partition_data = {
        'until_2014_04_17' => 1397692800,
        SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_NAME =>
            SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_VALUE,
        'until_2014_03_17' => 1395014400,
      }
    end
    it "should sort into an array by timestamp (key) with the 'future' partition at the end" do
      SqlPartitioner::SQL.sort_partition_data(@partition_data).should == [
        ['until_2014_03_17', 1395014400],
        ['until_2014_04_17', 1397692800],
        [SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_NAME,
            SqlPartitioner::BasePartitionsManager::FUTURE_PARTITION_VALUE],
      ]
    end
  end
end