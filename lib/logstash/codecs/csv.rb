# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require "csv"

class LogStash::Codecs::CSV < LogStash::Codecs::Base

  config_name "csv"

  # Define a list of column names (in the order they appear in the CSV,
  # as if it were a header line). If `columns` is not configured, or there
  # are not enough columns specified, the default column names are
  # "column1", "column2", etc. In the case that there are more columns
  # in the data than specified in this column list, extra columns will be auto-numbered:
  # (e.g. "user_defined_1", "user_defined_2", "column3", "column4", etc.)
  config :columns, :validate => :array, :default => []

  # Define the column separator value. If this is not specified, the default
  # is a comma `,`.
  # Optional.
  config :separator, :validate => :string, :default => ","

  # Define the character used to quote CSV fields. If this is not specified
  # the default is a double quote `"`.
  # Optional.
  config :quote_char, :validate => :string, :default => '"'

  # Treats the first line received as the hearder information, this information will
  # be used to compose the field names in the generated events. Note this information can
  # be reset on demand, useful for example when dealing with new files in the file input
  # or new request in the http_poller. Default => false
  config :include_headers, :validate => :boolean, :default => false

  # The character encoding used in this codec. Examples include "UTF-8" and
  # "CP1252".
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  def register
    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger

    @headers = false
    @options = { :col_sep => @separator, :quote_char => @quote_char }
  end

  def decode(data)
    data = @converter.convert(data)
    begin
      values = CSV.parse_line(data, @options)
      if @include_headers && !@headers
        @headers = true
        @options[:headers] = values
      else
        decoded = {}
        values.each_with_index do |fields, index|
          if fields.is_a?(String)  # No headers
            field_name =  ( !@columns[index].nil? ? @columns[index] : "column#{(index+1)}")
            decoded[field_name] = fields
          elsif fields.is_a?(Array) # Got headers
            decoded[fields[0]] = fields[1]
          end
        end
        yield LogStash::Event.new(decoded) if block_given?
      end
    rescue CSV::MalformedCSVError => e
      @logger.info("CSV parse failure. Falling back to plain-text", :error => e, :data => data)
      yield LogStash::Event.new("message" => data, "tags" => ["_csvparsefailure"]) if block_given?
    end
  end

  def encode(event)
    csv_data = CSV.generate_line(event.to_hash.values, @options)
    @on_event.call(event, csv_data)
  end

  def reset
    @headers = false
    @options.delete(:headers)
  end

end # class LogStash::Codecs::Plain
