require File.expand_path("../spec_helper", File.dirname(__FILE__))

shared_examples_for "LockWaitTimeoutHandler" do
  describe ".with_lock_wait_timeout" do
    before(:each) do
      @with_lock_wait_timeout = 1
    end
    context "with a new lock_wait_timeout value" do
      before(:each) do
        @orig_local_lock_wait_timeout  = adapter.select("SELECT @@local.lock_wait_timeout").first
        @orig_global_lock_wait_timeout = adapter.select("SELECT @@global.lock_wait_timeout").first

        @orig_local_lock_wait_timeout.should_not  == @with_lock_wait_timeout
        @orig_global_lock_wait_timeout.should_not == @with_lock_wait_timeout
      end
      it "should set and reset lock_wait_timeout" do
        step = 0
        SqlPartitioner::LockWaitTimeoutHandler.with_lock_wait_timeout(adapter, @with_lock_wait_timeout) do
          adapter.select("SELECT @@local.lock_wait_timeout").first.to_s.should      == @with_lock_wait_timeout.to_s
          adapter.select("SELECT @@global.lock_wait_timeout").first.to_s.should_not == @with_lock_wait_timeout.to_s

          adapter.execute("SELECT 1 FROM DUAL")
          step += 1
        end

        adapter.select("SELECT @@local.lock_wait_timeout").first.to_s.should  == @orig_local_lock_wait_timeout.to_s
        adapter.select("SELECT @@global.lock_wait_timeout").first.to_s.should == @orig_global_lock_wait_timeout.to_s
        step.should == 1
      end
    end
    context "with a second db connection" do
      before(:each) do
        @adapter_2 = get_adapter_2.call
      end
      after(:each) do
        close_adapter_2_connection.call
      end
      context "and a new lock_wait_timeout getting set by the first db connection" do
        before(:each) do
          @orig_local_lock_wait_timeout  = @adapter_2.select("SELECT @@local.lock_wait_timeout").first.to_s
          @orig_global_lock_wait_timeout = @adapter_2.select("SELECT @@global.lock_wait_timeout").first.to_s

          @orig_local_lock_wait_timeout.should_not  == @with_lock_wait_timeout.to_s
          @orig_global_lock_wait_timeout.should_not == @with_lock_wait_timeout.to_s
        end
        it "should not affect the lock_wait_timeout value of the second db connection" do
          step = 0
          SqlPartitioner::LockWaitTimeoutHandler.with_lock_wait_timeout(adapter, @with_lock_wait_timeout) do
            @adapter_2.select("SELECT @@local.lock_wait_timeout").first.to_s.should_not  == @with_lock_wait_timeout.to_s
            @adapter_2.select("SELECT @@global.lock_wait_timeout").first.to_s.should_not == @with_lock_wait_timeout.to_s

            adapter.execute("SELECT 1 FROM DUAL")
            step += 1
          end
          step.should == 1
        end
      end
      context "and the second db connection holding a lock" do
        it "should timeout quickly" do
          @adapter_2.execute("LOCK TABLE test_events WRITE")

          step = 0
          begin
            lambda do
              SqlPartitioner::LockWaitTimeoutHandler.with_lock_wait_timeout(adapter, @with_lock_wait_timeout) do
                step += 1
                adapter.execute("LOCK TABLE test_events WRITE")
              end
            end.should raise_error(sql_error_class, /Lock wait timeout exceeded/)
          ensure
            adapter.execute("UNLOCK TABLES")
            step.should == 1
            step += 1
          end
          step.should == 2
        end
      end
    end
  end
end

describe "LockWaitTimeoutHandler with ARAdapter" do
  it_should_behave_like "LockWaitTimeoutHandler" do
    let(:sql_error_class) do
      ActiveRecord::StatementInvalid
    end
    let(:adapter) do
      SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)
    end
    let(:get_adapter_2) do
      lambda {
        # TODO: Figure out how to establish the connection only once using before(:all)
        # TODO: Not sure how to get a second db connection without a model
        class WaitLockTimeoutTest < ActiveRecord::Base; end

        db_conf = ActiveRecord::Base.connection.instance_variable_get(:@config)
        # if the options passed are identical to the default repository, then no new connection is
        # opened but the existing one gets reused. Hence we merge some random stuff
        WaitLockTimeoutTest.establish_connection(db_conf.merge(:stuff => "stuff"))
        
        SqlPartitioner::ARAdapter.new(WaitLockTimeoutTest.connection)
      }
    end
    let (:close_adapter_2_connection) do
      lambda {
        WaitLockTimeoutTest.connection.disconnect!
      }
    end
  end
end

describe "LockWaitTimeoutHandler with DMAdapter" do
  it_should_behave_like "LockWaitTimeoutHandler" do
    let(:sql_error_class) do
      DataObjects::SQLError
    end
    let(:adapter) do
      SqlPartitioner::DMAdapter.new(DataMapper.repository.adapter)
    end
    let(:get_adapter_2) do
      lambda {
        # if the options passed are identical to the default repository, then no new connection is
        # opened but the existing one gets reused. Hence we merge some random stuff
        DataMapper.setup(:lock_events_connection, DataMapper.repository.adapter.options.merge("foobar" => 'something_random'))
        SqlPartitioner::DMAdapter.new(DataMapper.repository(:lock_events_connection).adapter)
      }
    end
    let (:close_adapter_2_connection) do
      lambda {
        # @second_db_connection.disconnect!
        # Not pretty to access a protected method if there is a cleaner way to close the connection, please let me know.
        DataMapper.repository(:lock_events_connection).adapter.send(:open_connection).dispose
      }
    end
  end
end
