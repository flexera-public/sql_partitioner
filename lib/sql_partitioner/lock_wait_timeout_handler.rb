module SqlPartitioner
  class LockWaitTimeoutHandler
    
    def self.with_lock_wait_timeout(adapter, timeout, &block)
      lock_wait_timeout_before = adapter.select("SELECT @@local.lock_wait_timeout").first
      adapter.execute("SET @@local.lock_wait_timeout = ?", timeout)
      begin
        return block.call
      ensure
        adapter.execute("SET @@local.lock_wait_timeout = ?", lock_wait_timeout_before.to_i)
      end
    end

  end
end
