require File.expand_path("../../spec_helper", File.dirname(__FILE__))

shared_examples_for "an adapter" do
  before(:each) do
  end
  describe ".select" do
    context "with multiple columns selected" do
      context "and no data returned" do
        it "should return an empty hash" do
          adapter.select("SELECT * FROM test_events").should == []
        end
      end
      context "and data returned" do
        before(:each) do
          adapter.execute("INSERT INTO test_events(timestamp) VALUES(1)")
          adapter.execute("INSERT INTO test_events(timestamp) VALUES(2)")
        end
        it "should return a Struct with accessors for each column" do
          result = adapter.select("SELECT * FROM test_events")

          result.should be_a_kind_of(Array)

          result.first.should be_a_kind_of(Struct)
          result.first.timestamp.to_i.should == 1

          result.last.should be_a_kind_of(Struct)
          result.last.timestamp.to_i.should  == 2
        end
      end
    end
    context "with a single column selected" do
      before(:each) do
        adapter.execute("INSERT INTO test_events(timestamp) VALUES(1)")
        adapter.execute("INSERT INTO test_events(timestamp) VALUES(2)")
      end
      it "should return the values without Struct" do
        result = adapter.select("SELECT timestamp FROM test_events")

        result.should be_a_kind_of(Array)

        result.first.to_i.should == 1
        result.last.to_i.should  == 2
      end
      it "should return an array even when only one row is selected" do
        result = adapter.select("SELECT timestamp FROM test_events LIMIT 1")

        result.should be_a_kind_of(Array)
      end
    end
  end
end

describe SqlPartitioner::ARAdapter do
  it_should_behave_like "an adapter" do
    let(:adapter) do 
      SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)
    end
  end
end

describe SqlPartitioner::DMAdapter do
  it_should_behave_like "an adapter" do
    let(:adapter) do
      SqlPartitioner::DMAdapter.new(DataMapper.repository.adapter)
    end
  end
end

