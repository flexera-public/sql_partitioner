require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe "BasePartitionsManager with ARAdapter" do
  describe "#initialize_partitioning_in_intervals" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::PartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => Logger.new(STDOUT)
      )
    end
    it "should initialize the database table as requested" do
      days_into_future = 50

      @partition_manager.initialize_partitioning_in_intervals(days_into_future)

      SqlPartitioner::Partition.all(@ar_adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
        ["until_2014_05_18", Time.utc(2014,05,18).to_i],
        ["until_2014_06_18", Time.utc(2014,06,18).to_i],
        ["future", "MAXVALUE"]
      ]
    end
  end
  describe "#append_partition_intervals" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::PartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => Logger.new(STDOUT)
      )
    end
    context "with an unpartitioned database table" do
      it "should raise a RuntimeError" do
        lambda{
          @partition_manager.append_partition_intervals(:months, 1, days_into_future = 30, dry_run = false)
        }.should raise_error(RuntimeError, /initialized/)
      end
    end
    context "with a partitioned database table" do
      context "and only the future partition exists" do
        before(:each) do
          @partition_manager.initialize_partitioning({})
        end
        it "should raise a RuntimeError" do
          lambda{
            @partition_manager.append_partition_intervals(:months, 1, days_into_future = 30, dry_run = false)
          }.should raise_error(RuntimeError, /initialized/)
        end
      end
      context "and the future partition as well as other partitions exist" do
        context "and all requested partitions already exist" do
          before(:each) do
            @partition_manager.initialize_partitioning({'until_2014_05_18' => Time.utc(2014,05,18).to_i})
          end
          it "should log and do nothing" do
            expect(@partition_manager.append_partition_intervals(:months, 1, days_into_future = 30, dry_run = false)).to eq({})
          end
        end
        context "and not all of the requested partitions exist yet" do
          before(:each) do
            @partition_manager.initialize_partitioning({'until_2014_05_18' => Time.utc(2014,05,18).to_i})
          end
          it "should log and do nothing" do
            expect(@partition_manager.append_partition_intervals(:months, 1, days_into_future = 60, dry_run = false)).to eq({
              'until_2014_06_18' => Time.utc(2014,06,18).to_i
            })
          end
        end
      end
    end
  end
  describe "#drop_partitions_older_than_in_days" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::PartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => Logger.new(STDOUT)
      )
    end
    it "should drop the requested partition" do
      @partition_manager.initialize_partitioning({
        'until_2014_02_18' => Time.utc(2014,02,18).to_i,
        'until_2014_03_18' => Time.utc(2014,03,18).to_i,
        'until_2014_04_18' => Time.utc(2014,04,18).to_i
      })
      @partition_manager.drop_partitions_older_than_in_days(days_from_now = 40)

      SqlPartitioner::Partition.all(@ar_adapter, 'test_events').map{|p| [p.name, p.timestamp]}.should == [
        ["until_2014_03_18", Time.utc(2014,03,18).to_i],
        ["until_2014_04_18", Time.utc(2014,04,18).to_i],
        ["future", "MAXVALUE"]
      ]
    end
  end

  describe "#drop_partitions_older_than" do
    before(:each) do
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @partition_manager = SqlPartitioner::PartitionsManager.new(
        :adapter      => @ar_adapter,
        :current_time => Time.utc(2014,04,18),
        :table_name   => 'test_events',
        :logger       => Logger.new(STDOUT)
      )
    end
    context "with no partition requested to be dropped" do
      before(:each) do
        @partition_manager.initialize_partitioning({
          'until_2014_03_18' => Time.utc(2014,03,18).to_i,
          'until_2014_04_18' => Time.utc(2014,04,18).to_i
        })
      end
      it "should not drop anything" do
        expect(@partition_manager.drop_partitions_older_than(Time.utc(2014,03,18).to_i)).to eq []
      end
    end
    context "with one partition requested to be dropped" do
      before(:each) do
        @partition_manager.initialize_partitioning({
          'until_2014_03_18' => Time.utc(2014,03,18).to_i,
          'until_2014_04_18' => Time.utc(2014,04,18).to_i
        })
      end
      it "should drop the requeuest partition to be dropped" do
        expect(@partition_manager.drop_partitions_older_than(Time.utc(2014,04,18).to_i)).to eq [
          'until_2014_03_18'
        ]
      end
    end
  end
end

describe "PartitionsManager" do
  before(:each) do
    @adapter = Struct.new(:schema_name).new("sql_partitioner_test")

    @partition_manager = SqlPartitioner::PartitionsManager.new(
      :adapter      => @adapter,
      :current_time => Time.utc(2014,01,01),
      :table_name   => 'test_events',
      :logger       => Logger.new(STDOUT)
    )
  end

  describe "#partitions_to_append" do
    it "should return the partitions to create" do
      partition_start_timestamp = Time.utc(2013,12,01).to_i
      partition_size   = 2
      days_into_future = 32

      expect(@partition_manager.partitions_to_append(partition_start_timestamp, :months, partition_size, days_into_future)).to eq({
        "until_2014_02_01"=>Time.utc(2014,02,01).to_i,
        "until_2014_04_01"=>Time.utc(2014,04,01).to_i
      })
    end
  end

  describe "#partitions_to_append_by_ts_range" do
    context "with end_timestamp == partition_start_timestamp" do
      before(:each) do
        @start_ts = Time.utc(2014,01,01).to_i
        @end_ts   = Time.utc(2014,01,01).to_i
      end
      it "should create the number of partitions requested" do
        months_to_cover = 1
        expect(@partition_manager.partitions_to_append_by_ts_range(@start_ts, @end_ts, :months, months_to_cover)).to eq({})
      end
    end
    context "with end_timestamp - partition_start_timestamp < partition_size_unit * partition_size" do
      before(:each) do
        @start_ts = Time.utc(2014,01,01).to_i
        @end_ts   = Time.utc(2014,01,02).to_i
      end
      it "should create the number of partitions requested" do
        months_to_cover = 1
        expect(@partition_manager.partitions_to_append_by_ts_range(@start_ts, @end_ts, :months, months_to_cover)).to eq({
            "until_2014_02_01"=>Time.utc(2014,02,01).to_i
          }
        )
      end
    end
    context "with end_timestamp - partition_start_timestamp == partition_size_unit * partition_size" do
      before(:each) do
        @start_ts = Time.utc(2014,01,01).to_i
        @end_ts   = Time.utc(2014,02,01).to_i
      end
      it "should create the number of partitions requested" do
        months_to_cover = 1
        expect(@partition_manager.partitions_to_append_by_ts_range(@start_ts, @end_ts, :months, months_to_cover)).to eq({
            "until_2014_02_01"=>Time.utc(2014,02,01).to_i
          }
        )
      end
    end
    context "with end_timestamp - partition_start_timestamp > partition_size_unit * partition_size" do
      before(:each) do
        @start_ts = Time.utc(2014,01,01).to_i
        @end_ts   = Time.utc(2014,03,01).to_i
      end
      context "with one months partition size" do
        it "should create one partition two months in the future from start_ts" do
          months_to_cover = 1
          expect(@partition_manager.partitions_to_append_by_ts_range(@start_ts, @end_ts, :months, months_to_cover)).to eq({
              "until_2014_02_01"=>Time.utc(2014,02,01).to_i,
              "until_2014_03_01"=>Time.utc(2014,03,01).to_i
            }
          )
        end
      end
      context "with two months partition size" do
        it "should create one partition two months in the future from start_ts" do
          months_to_cover = 2
          expect(@partition_manager.partitions_to_append_by_ts_range(@start_ts, @end_ts, :months, months_to_cover)).to eq({
              "until_2014_03_01"=>Time.utc(2014,03,01).to_i
            }
          )
        end
      end
      context "with 10 day partition size" do
        it "should create one partition two months in the future from start_ts" do
          days_to_cover = 20
          expect(@partition_manager.partitions_to_append_by_ts_range(@start_ts, @end_ts, :days, days_to_cover)).to eq({
              "until_2014_01_21" => Time.utc(2014,01,21).to_i,
              "until_2014_02_10" => Time.utc(2014,02,10).to_i,
              "until_2014_03_02" => Time.utc(2014,03,02).to_i
            }
          )
        end
      end
    end
    context "with end_timestamp < partition_start_timestamp" do
      before(:each) do
        @start_ts = Time.utc(2014,02,01).to_i
        @end_ts   = Time.utc(2014,01,01).to_i
      end
      it "should create the number of partitions requested" do
        months_to_cover = 1
        expect(@partition_manager.partitions_to_append_by_ts_range(@start_ts, @end_ts, :months, months_to_cover)).to eq({})
      end
    end
    context "with invalid parameters" do
      it "should raise an Argument error with a start_ts < 0" do
        lambda do
          expect(@partition_manager.partitions_to_append_by_ts_range(start_ts = -1, end_ts = 1, :months, 1)).to eq({})
        end.should raise_error(ArgumentError, /should be > 0/)
      end
      it "should raise an Argument error with a end_ts < 0" do
        lambda do
          expect(@partition_manager.partitions_to_append_by_ts_range(start_ts = 1, end_ts = -1, :months, 1)).to eq({})
        end.should raise_error(ArgumentError, /should be > 0/)
      end
      it "should raise an Argument error with an invalid partition_size_unit" do
        lambda do
          expect(@partition_manager.partitions_to_append_by_ts_range(start_ts = 1, end_ts = 1, :potatoes, 1)).to eq({})
        end.should raise_error(ArgumentError, /partition_size_unit must be one of/)
      end
      it "should raise an Argument error with an invalid partition_size" do
        lambda do
          expect(@partition_manager.partitions_to_append_by_ts_range(start_ts = 1, end_ts = 1, :months, -1)).to eq({})
        end.should raise_error(ArgumentError, /should be > 0/)
      end
    end
  end
end