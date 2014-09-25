require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe "PartitionCollection" do
  before(:each) do
    @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

    @partition_manager = SqlPartitioner::BasePartitionsManager.new(
      :adapter      => @ar_adapter,
      :current_time => Time.utc(2014,04,18),
      :table_name   => 'events',
      :logger       => Logger.new(STDOUT)
    )
  end
  describe "#current_partition" do
    before(:each) do
      @partitions = {'until_2014_03_17' => 1395014400}
      @partition_manager.initialize_partitioning(@partitions)
    end

    it "should return nil when timestamp > partition.timestamp" do
      existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).first[1]
      SqlPartitioner::Partition.all(@ar_adapter, 'events').current_partition(existing_partition_ts + 1).should == nil
    end
    it "should return nil when timestamp == partition.timestamp" do
      existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).first[1]
      SqlPartitioner::Partition.all(@ar_adapter, 'events').current_partition(existing_partition_ts).should == nil
    end
    it "should return the partition when timestamp < partition.timestamp" do
      existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).first[1]
      SqlPartitioner::Partition.all(@ar_adapter, 'events').current_partition(existing_partition_ts - 1).name.should == @partitions.keys.first
    end
  end

  describe "#older_than_timestamp" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'events',
        :logger       => Logger.new(STDOUT)
      )

      @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
      @partition_manager.initialize_partitioning(@partitions)
    end

    it "should return nil when timestamp < oldest partition.timestamp" do
      existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).first[1]
      SqlPartitioner::Partition.all(@ar_adapter, 'events').older_than_timestamp(existing_partition_ts - 1).should == []
    end
    it "should return nil when timestamp == partition.timestamp" do
      existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).first[1]
      SqlPartitioner::Partition.all(@ar_adapter, 'events').older_than_timestamp(existing_partition_ts).should == []
    end
    it "should return the partition when timestamp < partition.timestamp" do
      existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).last[1]
      SqlPartitioner::Partition.all(@ar_adapter, 'events').older_than_timestamp(existing_partition_ts + 1).map{|p| [p.name, p.timestamp]}.should == [
        ['until_2014_03_17', 1395014400],
        ['until_2014_04_17', 1397692800],
      ]
    end
  end

  describe "#non_future_partitions" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'events',
        :logger       => Logger.new(STDOUT)
      )

      @partitions = {'until_2014_03_17' => 1395014400}
      @partition_manager.initialize_partitioning(@partitions)
    end

    it "should not return the future partition" do
      existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).first[1]
      SqlPartitioner::Partition.all(@ar_adapter, 'events').non_future_partitions.map{|p| [p.name, p.timestamp]}.should == [
        ['until_2014_03_17', 1395014400]
      ]
    end
  end

  describe "#latest_partition" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'events',
        :logger       => Logger.new(STDOUT)
      )
    end

    context "with some partitions" do 
      before(:each) do
        @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
        @partition_manager.initialize_partitioning(@partitions)
      end

      it "should return the latest partition" do
        existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).first[1]
        partition = SqlPartitioner::Partition.all(@ar_adapter, 'events').latest_partition

        partition.name.should      == 'until_2014_04_17'
        partition.timestamp.should == 1397692800
      end
    end

    context "with no partitions" do
      before(:each) do
        @partition_manager.initialize_partitioning({})
      end

      it "should not return the future partition" do
        SqlPartitioner::Partition.all(@ar_adapter, 'events').latest_partition.should == nil
      end
    end
  end

  describe "#oldest_partition" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'events',
        :logger       => Logger.new(STDOUT)
      )
    end

    context "with some partitions" do 
      before(:each) do
        @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
        @partition_manager.initialize_partitioning(@partitions)
      end

      it "should the oldest partition" do
        existing_partition_ts = SqlPartitioner::SQL.sort_partition_data(@partitions).last[1]
        partition = SqlPartitioner::Partition.all(@ar_adapter, 'events').oldest_partition

        partition.name.should      == 'until_2014_03_17'
        partition.timestamp.should == 1395014400
      end
    end

    context "with no partitions" do
      before(:each) do
        @partition_manager.initialize_partitioning({})
      end

      it "should not return the future partition" do
        SqlPartitioner::Partition.all(@ar_adapter, 'events').oldest_partition.should == nil
      end
    end
  end
end

