# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require "csv"

class LogStash::Codecs::CSV < LogStash::Codecs::Base

  config_name "csv"

  # Define the column separator value. If this is not specified, the default
  # is a comma `,`.
  # Optional.
  config :separator, :validate => :string, :default => ","

  # Define the character used to quote CSV fields. If this is not specified
  # the default is a double quote `"`.
  # Optional.
  config :quote_char, :validate => :string, :default => '"'

  # The character encoding used in this codec. Examples include "UTF-8" and
  # "CP1252".
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  def register
    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger

    @options = { :col_sep => @separator, :quote_char => @quote_char }
  end

  def decode(data)
    data = @converter.convert(data)
    begin
      values = CSV.parse_line(data, @options)
      decoded = {}
      values.each_with_index do |value, index|
        decoded["column#{(index+1)}"] = value
      end
      yield LogStash::Event.new(decoded)
    rescue CSV::MalformedCSVError => e
      @logger.info("CSV parse failure. Falling back to plain-text", :error => e, :data => data)
      yield LogStash::Event.new("message" => data, "tags" => ["_csvparsefailure"])
    end
  end

  def encode(event)
    csv_data = CSV.generate_line(event.to_hash.values, @options)
    @on_event.call(event, csv_data)
  end

end # class LogStash::Codecs::Plain
