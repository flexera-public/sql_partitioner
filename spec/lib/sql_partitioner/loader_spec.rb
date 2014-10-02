require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe "Loader" do
  describe "#require_or_skip" do
    it "should call require & return true if a related constant is defined" do
      expect(SqlPartitioner::Loader.require_or_skip('sql_partitioner/adapters/ar_adapter', 'ActiveRecord')). to be true
    end
    it "should not call require & return false if a related constant is not defined" do
      expect(SqlPartitioner::Loader.require_or_skip('gratin/potato_peeler', 'Potatoes')). to be false
    end
  end
end