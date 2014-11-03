require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe "TimeUnitConverter" do
  describe "#initialize" do
    it "should initalize a new TimeUnitConverter with a supported time unit" do
      SqlPartitioner::TimeUnitConverter.new(SqlPartitioner::TimeUnitConverter::SUPPORTED_TIME_UNITS.first)
    end
    it "should raise an ArgumentError with with a unsupported time unit" do
      lambda{ SqlPartitioner::TimeUnitConverter.new(:potatoes) }.should raise_error(ArgumentError)
    end
  end

  describe "#to_date_time" do
    before(:each) do
      @tuc = SqlPartitioner::TimeUnitConverter.new(:seconds)
    end
    it "should return a DateTime object" do
      timestamp = Time.utc(2014,01,15).to_i
      @tuc.to_date_time(timestamp).to_s.should == "2014-01-15T00:00:00+00:00"
    end
  end
  describe "#from_seconds" do
    it "should return a timestamp in the configured time unit" do
      timestamp_in_secs = Time.utc(2014,01,15).to_i

      tum = SqlPartitioner::TimeUnitConverter.new(:micro_seconds)
      tum.from_seconds(timestamp_in_secs).should == timestamp_in_secs * 1_000_000

      tum = SqlPartitioner::TimeUnitConverter.new(:seconds)
      tum.from_seconds(timestamp_in_secs).should == timestamp_in_secs
    end
  end

  describe "#to_seconds" do
    it "should return a timestamp in the configured time unit" do
      timestamp_in_secs = Time.utc(2014,01,15).to_i

      tum = SqlPartitioner::TimeUnitConverter.new(:micro_seconds)
      tum.to_seconds(timestamp_in_secs * 1_000_000).should == timestamp_in_secs

      tum = SqlPartitioner::TimeUnitConverter.new(:seconds)
      tum.to_seconds(timestamp_in_secs).should == timestamp_in_secs
    end
  end

  describe "#advance_date_time" do
    before(:each) do
      @date_time = DateTime.new(2014,01,31)
    end
    context "using days" do
      before(:each) do
        @calendar_unit = :days
      end
      it "should return the correct DateTime when adding" do
        SqlPartitioner::TimeUnitConverter.advance_date_time(@date_time, @calendar_unit, days = 2).to_s.should == "2014-02-02T00:00:00+00:00"
      end
      it "should return the correct DateTime when subtracting" do
        SqlPartitioner::TimeUnitConverter.advance_date_time(@date_time, @calendar_unit, days = -2).to_s.should == "2014-01-29T00:00:00+00:00"
      end
    end
    context "using months" do
      before(:each) do
        @calendar_unit = :months
      end
      it "should return the correct DateTime when adding" do
        SqlPartitioner::TimeUnitConverter.advance_date_time(@date_time, @calendar_unit, months = 2).to_s.should == "2014-03-31T00:00:00+00:00"
      end
      it "should return the correct DateTime when subtracting" do
        SqlPartitioner::TimeUnitConverter.advance_date_time(@date_time, @calendar_unit, months = -2).to_s.should == "2013-11-30T00:00:00+00:00"
      end
    end
  end

  describe ".time_units_per_second" do
    it "should return the proper seconds multiplier to get the specified time unit" do
      SqlPartitioner::TimeUnitConverter.time_units_per_second(:micro_seconds).should == 1_000_000
      SqlPartitioner::TimeUnitConverter.time_units_per_second(:seconds).should == 1
    end
    it "all SUPPORTED_TIME_UNITS accounted for" do
      SqlPartitioner::TimeUnitConverter::SUPPORTED_TIME_UNITS.each do |u|
        SqlPartitioner::TimeUnitConverter.time_units_per_second(u).should be_a(Fixnum)
      end
    end
    it "unknown time unit should raise an error" do
      lambda {
        SqlPartitioner::TimeUnitConverter.time_units_per_second(:bogus)
      }.should raise_error(RuntimeError, /unknown time_unit :bogus/)
    end
  end
  
end