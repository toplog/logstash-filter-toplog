# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "set"
require "json"

class LogStash::Filters::TopLog < LogStash::Filters::Base

  config_name "toplog"
  milestone 1

  public
  def initialize(config = {})
    super

    @threadsafe = true

  end # def initialize

  public
  def register

    @logger.debug("Registered toplog plugin")
  end # def register

  public
  def filter(event)
    return unless filter?(event)

    @logger.debug(:debug_message => "TopLog args are ", :log_type => event["log_type"], :message => event["message"], :timestamp_format => event["timestamp_format"],
                   :timestamp_pattern => event["timestamp_pattern"])

    begin
      @timestamp_pattern = Regexp.new(event["timestamp_pattern"])

      @message = event["message"]

      if event["log_type"] == "r6_socialcloud"
        @message.gsub!(/(\[.*?\])/) do |m|
          m.gsub(/\s/, '-')
        end
      end

      messageTimestamp = @timestamp_pattern.match(@message).to_s

      @timestamp = DateTime.strptime(messageTimestamp, event["timestamp_format"])

      @unixTimestamp = @timestamp.strftime('%Q')

      event["toplog_timestamp"] = @unixTimestamp

      @message_epoch = @message.sub(@timestamp_pattern, event["toplog_timestamp"])

      @delimiters = event["delimiter"].nil? ? [" "] : get_delimiters(event["delimiter"])

      messageArray = @message_epoch.split(Regexp.union(@delimiters))

      #message according to message_columns array
      timestamp_index = messageArray.index(@unixTimestamp)
      #trim delimiters from first and last
      @delimiters.each do |delimiter|
        if delimiter.length > 1
          delimiter.chars.to_a.each do |delimiter_char|
            if messageArray.first[0] == delimiter_char
              messageArray.first[0] = ""
            end
            if messageArray.last[-1, 1] == delimiter_char
              messageArray.last[-1, 1] = ""
            end
          end
        end
      end

      #parse into columns
      event["toplog_columns"] = Hash.new
      messageArray.each_with_index do |column, index|
        if index == timestamp_index
          event["toplog_columns"][index.to_s] = messageTimestamp
        else
          event["toplog_columns"][index.to_s] = column
        end
      end

      @logger.debug(:debug_message => "TopLog fields are: ", :toplog => event["toplog"])


      unless event["default_source_location"].nil?
        if event["log_type"] == "r6_socialcloud"
          event["default_source"] = @message.scan(/\[[^\[\]].*?[^\[\]]\]/).first.gsub(/\[|\]/, "")
        else
          event["default_source"] = messageArray[event["default_source_location"].to_i]
        end

        event["default_source"] = event["default_source"].gsub("/", "-") unless event["default_source"].nil?
      end

      #remove fields no longer needed
      event.remove("delimiter")
      event.remove("default_source_location")
      event.remove("timestamp_format")
      event.remove("timestamp_pattern")
      event.remove("file")
      event.remove("offset")
      event.remove("key")
      if event["type"] == "manual"
        event.remove("host")
      end

    rescue Exception => msg
        @logger.error(msg)
    end
  end

  public
  def get_delimiters(delimiters)
    begin
      return JSON.parse(delimiters)
    rescue JSON::ParserError => e
      return [delimiters]
    end
  end
end # class Logstash::Filters::TopLog
