require File.expand_path("../spec_helper", File.dirname(__FILE__))

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