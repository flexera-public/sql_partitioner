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

  describe "#partitions_to_append" do
    context "with latest partition being at same time as current_time" do
      before(:each) do
        @partition_start_timestamp = @partition_manager.current_timestamp
      end
      context "and we request to have 1 months in the future covered" do
        it "should create 2 partitions" do
          months_to_cover = 1
          expect(@partition_manager.partitions_to_append(@partition_start_timestamp, :months, 1, months_to_cover)).to eq({
              "until_2014_02_01"=>Time.utc(2014,02,01).to_i
            }
          )
        end
      end
    end
    context "with latest partition being 1 month in the past" do
      before(:each) do
        @partition_start_timestamp = Time.utc(2013,12,01).to_i
      end
      context "and we request to have 1 months in the future covered" do
        it "should create 2 partitions" do
          days_to_cover = 30
          expect(@partition_manager.partitions_to_append(@partition_start_timestamp, :months, 1, days_to_cover)).to eq({
              "until_2014_01_01"=>Time.utc(2014,01,01).to_i,
              "until_2014_02_01"=>Time.utc(2014,02,01).to_i
            }
          )
        end
      end
    end
    context "with latest partition covering 30 days in the future" do
      before(:each) do
        @partition_start_timestamp = Time.utc(2014,02,01).to_i
      end
      context "and we request to have 1 months in the future covered" do
        it "should create no extra partition" do
          days_to_cover = 30
          expect(@partition_manager.partitions_to_append(@partition_start_timestamp, :months, 1, days_to_cover)).to eq({})
        end
      end
      context "and we request to have 60 days in the future covered" do
        it "should create 2 extra partitions, due to the shortness of feburary requireng 2 partitions to cover 60 days" do
          days_to_cover = 60
          expect(@partition_manager.partitions_to_append(@partition_start_timestamp, :months, 1, days_to_cover)).to eq({
            "until_2014_03_01"=>Time.utc(2014,03,01).to_i,
            "until_2014_04_01"=>Time.utc(2014,04,01).to_i
            }
          )
        end
      end
      context "and we request to have 90 days in the future covered" do
        it "should create 2 extra partitions" do
          days_to_cover = 90
          expect(@partition_manager.partitions_to_append(@partition_start_timestamp, :months, 1, days_to_cover)).to eq({
              "until_2014_03_01"=>Time.utc(2014,03,01).to_i,
              "until_2014_04_01"=>Time.utc(2014,04,01).to_i
            }
          )
        end
      end
    end
  end
end