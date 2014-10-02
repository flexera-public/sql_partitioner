module SqlPartitioner
  class Loader

    def self.require_or_skip(path, required_constant)
      if Object.const_defined?(required_constant)
        require path

        true
      else
        puts "{sql_partitioner} SKIPPING `require '#{path}'` because #{required_constant} is not defined."

        false
      end
    end

  end
end