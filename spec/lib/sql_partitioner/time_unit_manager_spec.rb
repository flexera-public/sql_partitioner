require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe "TimeUnitManager" do
  describe "#initialize" do
    it "should initalize a new TimeUnitManager with a supported time unit" do
      SqlPartitioner::TimeUnitManager.new(SqlPartitioner::TimeUnitManager::SUPPORTED_TIME_UNITS.first)
    end
    it "should raise an ArgumentError with with a unsupported time unit" do
      lambda{ SqlPartitioner::TimeUnitManager.new(:potatoes) }.should raise_error(ArgumentError)
    end
  end

  describe "#from_time_unit_to_date_time" do
    before(:each) do
      @tum = SqlPartitioner::TimeUnitManager.new(:seconds)
    end
    it "should return a DateTime object" do
      timestamp = Time.utc(2014,01,15).to_i
      @tum.from_time_unit_to_date_time(timestamp).to_s.should == "2014-01-15T00:00:00+00:00"
    end
  end
  describe "#to_time_unit" do
    it "should return a timestamp in the configured time unit" do
      timestamp_in_secs = Time.utc(2014,01,15).to_i

      tum = SqlPartitioner::TimeUnitManager.new(:micro_seconds)
      tum.to_time_unit(timestamp_in_secs).should == timestamp_in_secs * 1_000_000

      tum = SqlPartitioner::TimeUnitManager.new(:seconds)
      tum.to_time_unit(timestamp_in_secs).should == timestamp_in_secs
    end
  end

  describe "#from_time_unit" do
    it "should return a timestamp in the configured time unit" do
      timestamp_in_secs = Time.utc(2014,01,15).to_i

      tum = SqlPartitioner::TimeUnitManager.new(:micro_seconds)
      tum.from_time_unit(timestamp_in_secs * 1_000_000).should == timestamp_in_secs

      tum = SqlPartitioner::TimeUnitManager.new(:seconds)
      tum.from_time_unit(timestamp_in_secs).should == timestamp_in_secs
    end
  end

  describe "#advance_date_time" do
    before(:each) do
      @date_time = DateTime.new(2014,01,31)
    end
    context "with days" do
      before(:each) do
        @time_unit = :days
      end
      it "should return the correct DateTime when adding" do
        SqlPartitioner::TimeUnitManager.advance_date_time(@date_time, @time_unit, days = 2).to_s.should == "2014-02-02T00:00:00+00:00"
      end
      it "should return the correct DateTime when subtracting" do
        SqlPartitioner::TimeUnitManager.advance_date_time(@date_time, @time_unit, days = -2).to_s.should == "2014-01-29T00:00:00+00:00"
      end
    end
    context "with months" do
      before(:each) do
        @time_unit = :months
      end
      it "should return the correct DateTime when adding" do
        SqlPartitioner::TimeUnitManager.advance_date_time(@date_time, @time_unit, months = 2).to_s.should == "2014-03-31T00:00:00+00:00"
      end
      it "should return the correct DateTime when subtracting" do
        SqlPartitioner::TimeUnitManager.advance_date_time(@date_time, @time_unit, months = -2).to_s.should == "2013-11-30T00:00:00+00:00"
      end
    end
  end

  describe ".time_unit_multiplier" do
    it "should return the proper seconds multiplier to get the specified time unit" do
      SqlPartitioner::TimeUnitManager.time_unit_multiplier(:micro_seconds).should == 1_000_000
      SqlPartitioner::TimeUnitManager.time_unit_multiplier(:seconds).should == 1
    end
  end
  
end