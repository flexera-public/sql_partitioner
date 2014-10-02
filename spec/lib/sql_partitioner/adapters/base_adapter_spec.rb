require File.expand_path("../../spec_helper", File.dirname(__FILE__))

describe "BaseAdapter" do
  before(:each) do
    @base_adapter = SqlPartitioner::BaseAdapter.new
  end
  describe "#select" do
    it "should raise an RuntimeError" do
      lambda{
        @base_adapter.select("SELECT database()")
      }.should raise_error(RuntimeError, /MUST BE IMPLEMENTED/)
    end
  end
  describe "#execute" do
    it "should raise an RuntimeError" do
      lambda{
        @base_adapter.execute("SELECT database()")
      }.should raise_error(RuntimeError, /MUST BE IMPLEMENTED/)
    end
  end
  describe "#schema_name" do
    it "should raise an RuntimeError" do
      lambda{
        @base_adapter.schema_name
      }.should raise_error(RuntimeError, /MUST BE IMPLEMENTED/)
    end
  end
end