require File.expand_path("../spec_helper", File.dirname(__FILE__))

describe SqlPartitioner::LockWaitTimeoutHandler do
  describe ".with_lock_wait_timeout" do
    before(:each) do
      @with_lock_wait_timeout = 1
      @ar_adapter = SqlPartitioner::ARAdapter.new(ActiveRecord::Base.connection)

      @first_db_connection = ActiveRecord::Base.connection
    end
    context "with a new lock_wait_timeout value" do
      before(:each) do
        @orig_local_lock_wait_timeout  = @first_db_connection.select_value("SELECT @@local.lock_wait_timeout")
        @orig_global_lock_wait_timeout = @first_db_connection.select_value("SELECT @@global.lock_wait_timeout")

        @orig_local_lock_wait_timeout.should_not  == @with_lock_wait_timeout
        @orig_global_lock_wait_timeout.should_not == @with_lock_wait_timeout
      end
      it "should set and reset lock_wait_timeout" do
        step = 0
        SqlPartitioner::LockWaitTimeoutHandler.with_lock_wait_timeout(@ar_adapter, @with_lock_wait_timeout) do
          @first_db_connection.select_value("SELECT @@local.lock_wait_timeout").should      == @with_lock_wait_timeout.to_s
          @first_db_connection.select_value("SELECT @@global.lock_wait_timeout").should_not == @with_lock_wait_timeout.to_s

          @first_db_connection.execute("SELECT 1 FROM DUAL")
          step += 1
        end

        @first_db_connection.select_value("SELECT @@local.lock_wait_timeout").should  == @orig_local_lock_wait_timeout.to_s
        @first_db_connection.select_value("SELECT @@global.lock_wait_timeout").should == @orig_global_lock_wait_timeout.to_s
        step.should == 1
      end
    end
    context "with a second db connection" do
      before(:each) do
        # TODO: Figure out how to establish the connection only once using before(:all)
        # TODO: Not sure how to get a second db connection without a model
        class WaitLockTimeoutTest < ActiveRecord::Base; end

        db_conf = @first_db_connection.instance_variable_get(:@config)
        # if the options passed are identical to the default repository, then no new connection is
        # opened but the existing one gets reused. Hence we merge some random stuff
        WaitLockTimeoutTest.establish_connection(db_conf.merge(:stuff => "stuff"))
        @second_db_connection = WaitLockTimeoutTest.connection
      end
      after(:each) do
        @second_db_connection.disconnect!
      end
      context "and a new lock_wait_timeout getting set by the first db connection" do
        before(:each) do
          @orig_local_lock_wait_timeout  = @second_db_connection.select_value("SELECT @@local.lock_wait_timeout")
          @orig_global_lock_wait_timeout = @second_db_connection.select_value("SELECT @@global.lock_wait_timeout")

          @orig_local_lock_wait_timeout.should_not  == @with_lock_wait_timeout
          @orig_global_lock_wait_timeout.should_not == @with_lock_wait_timeout
        end
        it "should not affect the lock_wait_timeout value of the second db connection" do
          step = 0
          SqlPartitioner::LockWaitTimeoutHandler.with_lock_wait_timeout(@ar_adapter, @with_lock_wait_timeout) do
            @second_db_connection.select_value("SELECT @@local.lock_wait_timeout").should_not  == @with_lock_wait_timeout
            @second_db_connection.select_value("SELECT @@global.lock_wait_timeout").should_not == @with_lock_wait_timeout

            @first_db_connection.execute("SELECT 1 FROM DUAL")
            step += 1
          end
          step.should == 1
        end
      end
      context "and the second db connection holding a lock" do
        it "should timeout quickly" do
          @second_db_connection.execute("LOCK TABLE test_events WRITE")

          step = 0
          begin
            lambda do
              SqlPartitioner::LockWaitTimeoutHandler.with_lock_wait_timeout(@ar_adapter, @with_lock_wait_timeout) do
                step += 1
                @first_db_connection.execute("LOCK TABLE test_events WRITE")
              end
            end.should raise_error(ActiveRecord::StatementInvalid, /Lock wait timeout exceeded/)
          ensure
            @second_db_connection.execute("UNLOCK TABLES")
            step.should == 1
            step += 1
          end
          step.should == 2
        end
      end
    end
  end
end