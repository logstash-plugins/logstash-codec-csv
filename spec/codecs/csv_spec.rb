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
        expect(event["column1"]).to eq("big")
        expect(event["column2"]).to eq("bird")
        expect(event["column3"]).to eq("sesame street")
      end
    end

    describe "given column names" do
      let(:doc)    { "big,bird,sesame street" }
      let(:config) do
        { "columns" => ["first", "last", "address" ] }
      end

      it "extract all the values" do
        codec.decode(data) do |event|
          expect(event["first"]).to eq("big")
          expect(event["last"]).to eq("bird")
          expect(event["address"]).to eq("sesame street")
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
          expect(event["column1"]).to eq("big,bird")
          expect(event["column2"]).to eq("sesame street")
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
          expect(event["column1"]).to eq("big")
          expect(event["column2"]).to eq("bird")
          expect(event["column3"]).to eq("sesame street")
        end
      end

      context "using the default one" do
        let(:data) { 'big,bird,"sesame, street"' }
        let(:config) { Hash.new }

        it "return an event from CSV data" do
          codec.decode(data) do |event|
            expect(event["column1"]).to eq("big")
            expect(event["column2"]).to eq("bird")
            expect(event["column3"]).to eq("sesame, street")
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
            expect(event["column1"]).to eq("big")
            expect(event["column2"]).to eq("bird")
            expect(event["column3"]).to eq('"sesame" street')
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
        { "include_headers" => true }
      end

      it "include header information when requested" do
        codec.decode(data[0]) # Read the headers
        codec.decode(data[1]) do |event|
          expect(event["size"]).to eq("big")
          expect(event["animal"]).to eq("bird")
          expect(event["movie"]).to eq("sesame street")
        end
      end

      it "reset headers and fetch the new ones" do
        data.each do |row|
          codec.decode(row)
        end
        codec.reset
        codec.decode(new_data[0]) # set the new headers
        codec.decode(new_data[1]) do |event|
          expect(event["host"]).to eq("example.com")
          expect(event["country"]).to eq("germany")
          expect(event["city"]).to eq("berlin")
        end
      end

    end
  end
end
