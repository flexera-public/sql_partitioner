require File.expand_path("../spec_helper", File.dirname(__FILE__))

shared_examples_for "PartitionCollection" do
  before(:each) do
    @partition_manager = SqlPartitioner::BasePartitionsManager.new(
      :adapter      => adapter,
      :current_time => Time.utc(2014,04,18),
      :table_name   => 'test_events',
      :logger       => SPEC_LOGGER
    )
  end

  context "with some partitions" do
    before(:each) do
      @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
      @sorted = SqlPartitioner::SQL.sort_partition_data(@partitions)
      @newest_partition_ts = @sorted.last[1]
      @oldest_partition_ts = @sorted.first[1]
      @partition_manager.initialize_partitioning(@partitions)
      @all_partitions = SqlPartitioner::Partition.all(adapter, 'test_events')
    end

    describe "#current_partition" do
      it "should return nil when timestamp > newest partition.timestamp" do
        @all_partitions.current_partition(@newest_partition_ts + 1).should == nil
      end
      it "should return nil when timestamp == newest partition.timestamp" do
        @all_partitions.current_partition(@newest_partition_ts).should == nil
      end
      it "should return the newest partition when timestamp < newest partition.timestamp" do
        @all_partitions.current_partition(@newest_partition_ts - 1).name.should == @sorted.last[0]
      end
      it "should return the oldest partition when timestamp < oldest partition.timestamp" do
        @all_partitions.current_partition(@oldest_partition_ts - 1).name.should == @sorted.first[0]
      end
    end

    describe "#older_than_timestamp" do
      it "should return nil when timestamp < oldest partition.timestamp" do
        @all_partitions.older_than_timestamp(@oldest_partition_ts - 1).should == []
      end
      it "should return nil when timestamp == partition.timestamp" do
        @all_partitions.older_than_timestamp(@oldest_partition_ts).should == []
      end
      it "should return the partition when timestamp > newest partition timestamp" do
        @all_partitions.older_than_timestamp(@newest_partition_ts + 1).map{|p| [p.name, p.timestamp]}.should == [
          ['until_2014_03_17', 1395014400],
          ['until_2014_04_17', 1397692800],
        ]
      end
    end

    describe "#newer_than_timestamp" do
      it "should return nil when timestamp > newest partition.timestamp" do
        @all_partitions.newer_than_timestamp(@newest_partition_ts + 1).should == []
      end
      it "should return the newest partition when timestamp == partition.timestamp" do
        @all_partitions.newer_than_timestamp(@newest_partition_ts).map{|p| [p.name, p.timestamp]}.should == [
            ['until_2014_04_17', 1397692800]
        ]
      end
      it "should return all the partition when timestamp < oldest partition timestamp" do
        @all_partitions.newer_than_timestamp(@oldest_partition_ts - 1).map{|p| [p.name, p.timestamp]}.should == [
            ['until_2014_03_17', 1395014400],
            ['until_2014_04_17', 1397692800],
        ]
      end
    end

  end


  describe "#non_future_partitions" do
    before(:each) do
      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      )

      @partitions = {'until_2014_03_17' => 1395014400}
      @partition_manager.initialize_partitioning(@partitions)
    end

    it "should not return the future partition" do
      SqlPartitioner::Partition.all(adapter, 'test_events').non_future_partitions.map{|p| [p.name, p.timestamp]}.should == [
        ['until_2014_03_17', 1395014400]
      ]
    end
  end

  describe "#latest_partition" do
    before(:each) do
      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      )
    end

    context "with some partitions" do 
      before(:each) do
        @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
        @partition_manager.initialize_partitioning(@partitions)
      end

      it "should return the latest partition" do
        partition = SqlPartitioner::Partition.all(adapter, 'test_events').latest_partition

        partition.name.should      == 'until_2014_04_17'
        partition.timestamp.should == 1397692800
      end
    end

    context "with no partitions" do
      before(:each) do
        @partition_manager.initialize_partitioning({})
      end

      it "should not return the future partition" do
        SqlPartitioner::Partition.all(adapter, 'test_events').latest_partition.should == nil
      end
    end
  end

  describe "#oldest_partition" do
    before(:each) do
      @partition_manager = SqlPartitioner::BasePartitionsManager.new(
        :adapter      => adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => SPEC_LOGGER
      )
    end

    context "with some partitions" do 
      before(:each) do
        @partitions = {'until_2014_03_17' => 1395014400, 'until_2014_04_17' => 1397692800}
        @partition_manager.initialize_partitioning(@partitions)
      end

      it "should the oldest partition" do
        partition = SqlPartitioner::Partition.all(adapter, 'test_events').oldest_partition

        partition.name.should      == 'until_2014_03_17'
        partition.timestamp.should == 1395014400
      end
    end
  end
end


shared_examples_for "Partition" do
  before(:each) do
    @partition_manager = SqlPartitioner::BasePartitionsManager.new(
      :adapter      => adapter,
      :current_time => Time.utc(2014,04,18),
      :table_name   => 'test_events',
      :logger       => SPEC_LOGGER
    )
  end
  describe ".all" do
    context "with no partitions" do
      it "should an empty PartitionCollection" do
        SqlPartitioner::Partition.all(adapter, 'test_events').should == []
      end
    end
    context "with some partitions" do
      before(:each) do
        @partitions = {'until_2014_03_17' => 1395014400}
        @partition_manager.initialize_partitioning(@partitions)
      end

      it "should return a PartitionCollection containing all the partitions" do
        SqlPartitioner::Partition.all(adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
          ['until_2014_03_17', 1395014400],
          ["future", "MAXVALUE"]
        ]
      end
    end
  end

  describe "#future_partition?" do
    before(:each) do
      @partitions = {'until_2014_03_17' => 1395014400}
      @partition_manager.initialize_partitioning(@partitions)
    end

    it "should return true for the future partition" do
      SqlPartitioner::Partition.all(adapter, 'test_events').last.future_partition?.should == true
    end
    it "should return false for non-future partition" do
      SqlPartitioner::Partition.all(adapter, 'test_events').first.future_partition?.should == false
    end
  end

  describe "#timestamp" do
    before(:each) do
      @partitions = {'until_2014_03_17' => 1395014400}
      @partition_manager.initialize_partitioning(@partitions)
    end

    it "should return 'MAXVALUE' for the future partition" do
      SqlPartitioner::Partition.all(adapter, 'test_events').last.timestamp.should == SqlPartitioner::Partition::FUTURE_PARTITION_VALUE
    end
    it "should return the timestamp for non-future partition" do
      SqlPartitioner::Partition.all(adapter, 'test_events').first.timestamp.should == @partitions.values.last
    end
  end

  describe "#to_log" do
    before(:each) do
      @partitions = {'until_2014_03_17' => 1395014400}
      @partition_manager.initialize_partitioning(@partitions)
    end

    it "should return 'none' for no partitions" do
      SqlPartitioner::Partition.to_log([]).should == 'none'
    end

    it "should return a pretty log message with partitions" do
      log_msg =  "---------------------------------------------------------------------------------------------\n" +
                 "ordinal_position   name               timestamp    table_rows   data_length   index_length   \n" +
                 "---------------------------------------------------------------------------------------------\n" +
                 "1                  until_2014_03_17   1395014400   0            16384         0              \n" +
                 "2                  future             MAXVALUE     0            16384         0              \n" +
                 "---------------------------------------------------------------------------------------------"

      SqlPartitioner::Partition.to_log(SqlPartitioner::Partition.all(adapter, 'test_events')).should == log_msg
    end
  end

end


describe "PartitionCollection with ARAdapter" do
  it_should_behave_like "PartitionCollection" do
    let(:adapter) do
      SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)
    end
  end
end

describe "Partition with ARAdapter" do
  it_should_behave_like "Partition" do
    let(:adapter) do
      SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)
    end
  end
end

describe "PartitionCollection with DMAdapter" do
  it_should_behave_like "PartitionCollection" do
    let(:adapter) do
      SqlPartitioner::DMAdapter.new(DataMapper.repository.adapter)
    end
  end
end

describe "Partition with DMAdapter" do
  it_should_behave_like "Partition" do
    let(:adapter) do
      SqlPartitioner::DMAdapter.new(DataMapper.repository.adapter)
    end
  end
end
