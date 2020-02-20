# encoding: utf-8
require "logstash/codecs/csv"
require "logstash/event"

describe LogStash::Codecs::CSV do

  subject(:codec) { LogStash::Codecs::CSV.new(config) }
  let(:config)    { Hash.new }

  before(:each) do
    codec.register
  end

  describe "decode" do
    let(:data) { "big,bird,sesame street" }

    it "return an event from CSV data" do
      codec.decode(data) do |event|
        expect(event.get("column1")).to eq("big")
        expect(event.get("column2")).to eq("bird")
        expect(event.get("column3")).to eq("sesame street")
      end
    end

    describe "given column names" do
      let(:doc)    { "big,bird,sesame street" }
      let(:config) do
        { "columns" => ["first", "last", "address" ] }
      end

      it "extract all the values" do
        codec.decode(data) do |event|
          expect(event.get("first")).to eq("big")
          expect(event.get("last")).to eq("bird")
          expect(event.get("address")).to eq("sesame street")
        end
      end

      context "parse csv skipping empty columns" do

        let(:data)    { "val1,,val3" }

        let(:config) do
          { "skip_empty_columns" => true,
            "columns" => ["custom1", "custom2", "custom3"] }
        end

        it "extract all the values" do
          codec.decode(data) do |event|
            expect(event.get("custom1")).to eq("val1")
            expect(event.to_hash).not_to include("custom2")
            expect(event.get("custom3")).to eq("val3")
          end
        end
      end

      context "parse csv without autogeneration of names" do

        let(:data)    { "val1,val2,val3" }
        let(:config) do
          {  "autogenerate_column_names" => false,
             "columns" => ["custom1", "custom2"] }
        end

        it "extract all the values" do
          codec.decode(data) do |event|
            expect(event.get("custom1")).to eq("val1")
            expect(event.get("custom2")).to eq("val2")
            expect(event.get("column3")).to be_falsey
          end
        end
      end

    end

    describe "custom separator" do
      let(:data) { "big,bird;sesame street" }

      let(:config) do
        { "separator" => ";" }
      end

      it "return an event from CSV data" do
        codec.decode(data) do |event|
          expect(event.get("column1")).to eq("big,bird")
          expect(event.get("column2")).to eq("sesame street")
        end
      end
    end

    describe "quote char" do
      let(:data) { "big,bird,'sesame street'" }

      let(:config) do
        { "quote_char" => "'"}
      end

      it "return an event from CSV data" do
        codec.decode(data) do |event|
          expect(event.get("column1")).to eq("big")
          expect(event.get("column2")).to eq("bird")
          expect(event.get("column3")).to eq("sesame street")
        end
      end

      context "using the default one" do
        let(:data) { 'big,bird,"sesame, street"' }
        let(:config) { Hash.new }

        it "return an event from CSV data" do
          codec.decode(data) do |event|
            expect(event.get("column1")).to eq("big")
            expect(event.get("column2")).to eq("bird")
            expect(event.get("column3")).to eq("sesame, street")
          end
        end
      end

      context "using a null" do
        let(:data) { 'big,bird,"sesame" street' }
        let(:config) do
          { "quote_char" => "\x00" }
        end

        it "return an event from CSV data" do
          codec.decode(data) do |event|
            expect(event.get("column1")).to eq("big")
            expect(event.get("column2")).to eq("bird")
            expect(event.get("column3")).to eq('"sesame" street')
          end
        end
      end
    end

    describe "having headers" do

      let(:data) do
        [ "size,animal,movie", "big,bird,sesame street"]
      end

      let(:new_data) do
        [ "host,country,city", "example.com,germany,berlin"]
      end

      let(:config) do
        { "autodetect_column_names" => true }
      end

      it "include header information when requested" do
        codec.decode(data[0]) # Read the headers
        codec.decode(data[1]) do |event|
          expect(event.get("size")).to eq("big")
          expect(event.get("animal")).to eq("bird")
          expect(event.get("movie")).to eq("sesame street")
        end
      end
    end

    describe "using field convertion" do

      let(:config) do
        { "convert" => { "column1" => "integer", "column3" => "boolean" } }
      end
      let(:data)   { "1234,bird,false" }

      it "get converted values to the expected type" do
        codec.decode(data) do |event|        
          expect(event.get("column1")).to eq(1234)
          expect(event.get("column2")).to eq("bird")
          expect(event.get("column3")).to eq(false)
        end
      end

      context "when using column names" do

        let(:config) do
          { "convert" => { "custom1" => "integer", "custom3" => "boolean" },
            "columns" => ["custom1", "custom2", "custom3"] }
        end

        it "get converted values to the expected type" do
          codec.decode(data) do |event|
            expect(event.get("custom1")).to eq(1234)
            expect(event.get("custom2")).to eq("bird")
            expect(event.get("custom3")).to eq(false)
          end
        end
      end
    end
  end

  describe "encode" do
    context "not including headers" do
      let(:event) { LogStash::Event.new({"f1" => "v1", "f2" => "v2"}) }

      context "without columns" do
        let(:config) do
          { "include_headers" => false, "columns" => [] }
        end

        it "should encode to single CSV line" do
          codec.on_event do |e, d|
            expect(d.chomp.split(",").sort).to eq("v1,v2,1,#{event.timestamp}".split(",").sort)
          end
          codec.encode(event)
        end
      end

      context "with columns" do
        let(:config) do
          { "include_headers" => false, "columns" => ["f1", "f2"] }
        end

        it "should encode to single CSV line" do
          codec.on_event do |e, d|
            expect(d).to eq("v1,v2\n")
          end
          codec.encode(event)
        end
      end
    end

    context "including headers" do
      let(:event) { LogStash::Event.new({"f1" => "v1", "f2" => "v2"}) }

      context "without columns" do
        let(:config) do
          { "include_headers" => true, "columns" => [] }
        end

        it "should encode to two CSV line" do
          lines = []
          codec.on_event do |e, d|
            lines << d
          end
          codec.encode(event)
          expect(lines[0].chomp.split(",").sort).to eq("f1,f2,@version,@timestamp".split(",").sort)
          expect(lines[1].chomp.split(",").sort).to eq("v1,v2,1,#{event.timestamp}".split(",").sort)
        end
      end

      context "with columns" do
        let(:config) do
          { "include_headers" => true, "columns" => ["f1", "f2"] }
        end

        it "should encode to two CSV line" do
          lines = []
          codec.on_event do |e, d|
            lines << d
          end
          codec.encode(event)
          expect(lines[0]).to eq("f1,f2\n")
          expect(lines[1]).to eq("v1,v2\n")
        end
      end
    end
  end
end
