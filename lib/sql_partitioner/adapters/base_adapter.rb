module SqlPartitioner

  class BaseAdapter
    # -- needs to return an array of structs or an array of values if columns selected == 1
    def select(*args)
      raise "select(*args) MUST BE IMPLEMENTED!"
    end

    def execute(*args)
      raise "execute(*args) MUST BE IMPLEMENTED!"
    end

    def schema_name
      raise "schema_name MUST BE IMPLEMENTED!"
    end
  end

end
