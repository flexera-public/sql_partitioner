module SqlPartitioner
  class TimeUnitConverter
    require 'date'

    MINUTE_AS_SECONDS = 60
    HOUR_AS_SECONDS   = 60 * MINUTE_AS_SECONDS
    DAY_AS_SECONDS    = 24 * HOUR_AS_SECONDS

    SUPPORTED_TIME_UNITS = [:seconds, :micro_seconds]

    # @param [Symbol] time_unit one of SUPPORTED_TIME_UNITS
    def initialize(time_unit)
      raise ArgumentError.new("Invalid time unit #{time_unit} passed") if !SUPPORTED_TIME_UNITS.include?(time_unit)

      @time_unit = time_unit
    end

    # @return [Fixnum] number of days represented in the configure time units
    def from_days(num_days)
      from_seconds(num_days * DAY_AS_SECONDS)
    end

    # @param [Fixnum] time_units_timestamp timestamp in configured time units
    # @return [DateTime] representation of the given timestamp
    def to_date_time(time_units_timestamp)
      DateTime.strptime("#{to_seconds(time_units_timestamp)}", '%s')
    end

    # converts from seconds to the configured time unit
    #
    # @param [Fixnum] timestamp_seconds timestamp in seconds
    #
    # @return [Fixnum] timestamp in configured time units
    def from_seconds(timestamp_seconds)
      timestamp_seconds * time_units_per_second
    end

    # converts from the configured time unit to seconds
    #
    # @param [Fixnum] time_units_timestamp timestamp in the configured time units
    #
    # @return [Fixnum] timestamp in seconds
    def to_seconds(time_units_timestamp)
      time_units_timestamp / time_units_per_second
    end

    def time_units_per_second
      self.class.time_units_per_second(@time_unit)
    end

    # @param [Fixnum] time_units_timestamp
    # @param [Symbol] calendar_unit unit for the given value, one of [:day(s), :month(s)]
    # @param [Fixnum] value in terms of calendar_unit to add to the time_units_timestamp
    #
    # @return [Fixnum] new timestamp in configured time units
    def advance(time_units_timestamp, calendar_unit, value)
      date_time = to_date_time(time_units_timestamp)
      date_time = self.class.advance_date_time(date_time, calendar_unit, value)
      from_seconds(date_time.to_time.to_i)
    end

    # @param [DateTime] date_time
    # @param [Symbol] calendar_unit unit for the given value, one of [:day(s), :month(s)]
    # @param [Fixnum] value in terms of calendar_unit to add to the date_time
    # @return [DateTime] result of advancing the given date_time by the given value
    def self.advance_date_time(date_time, calendar_unit, value)
      new_time = case calendar_unit
        when :days, :day
          date_time + value
        when :months, :month
          date_time >> value
      end

      new_time
    end

    # @param [Symbol] time_unit one of SUPPORTED_TIME_UNITS
    # @return [Fixnum] how many of the given time_unit are in 1 second
    def self.time_units_per_second(time_unit)
      if time_unit == :micro_seconds
        multiplier = 1_000_000
      else
        multiplier = 1
      end
    end
  end
end
