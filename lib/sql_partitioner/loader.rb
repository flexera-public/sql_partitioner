module SqlPartitioner
  class Loader

    def self.require_or_skip(path, required_constant)
      if Object.const_defined?(required_constant)
        require path

        true
      else
        # "No need to `require '#{path}'` since #{required_constant} is not defined at this point."
        false
      end
    end

  end
end