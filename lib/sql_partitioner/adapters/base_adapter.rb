module SqlPartitioner

  class BaseAdapter
    # -- needs to return an array of structs or a single struct if array.size == 1
    def select(*args)
      raise "select(*args) MUST BE IMPLEMENTED!"
    end

    def execute(*args)
      aise "execute(*args) MUST BE IMPLEMENTED!"
    end

    def schema_name
      aise "schema_name MUST BE IMPLEMENTED!"
    end
  end

end
