module SqlPartitioner
  class TimeUnitManager
    def initialize(time_unit)
      @time_unit = time_unit
    end

    def self.days_in_seconds(num_days)
      60 * 60 * 24 * num_days
    end

    def days_to_time_unit(num_days)
      to_time_unit(TimeUnitManager.days_in_seconds(num_days))
    end

    def from_time_unit_to_date_time(timestamp)
      seconds_since_unix_epoch = self.from_time_unit(timestamp)
      DateTime.strptime("#{seconds_since_unix_epoch}", '%s')
    end

    # converts from seconds to the configured time unit
    #
    # @param [Fixnum] timestamp timestamp in seconds
    #
    # @return [Fixnum] timestamp in configured time units
    def to_time_unit(timestamp)
      timestamp * time_unit_multiplier
    end

    # converts from the configured time unit to seconds
    #
    # @param [Fixnum] timestamp timestamp in the configured timeout units
    #
    # @return [Fixnum] timestamp in seconds
    def from_time_unit(timestamp)
      timestamp / time_unit_multiplier
    end

    def time_unit_multiplier
      self.class.time_unit_multiplier(@time_unit)
    end

    def advance(timestamp, time_unit_to_advance, value)
      date_time = from_time_unit_to_date_time(timestamp)
      self.class.advance_date_time(date_time, time_unit_to_advance, value)
    end

    def self.advance_date_time(date_time, time_unit, value)
      new_time = case time_unit
        when :day
          date_time + value
        when :month
          date_time >> value
      end

      new_time
    end

    # translates time_unit to a second multiplier to get the requested
    # time unit
    #
    # @return [Fixnum] multiplier
    def self.time_unit_multiplier(time_unit)
      if time_unit == :micro_seconds
        multiplier = 1_000_000
      else
        multiplier = 1
      end
    end
  end
end
