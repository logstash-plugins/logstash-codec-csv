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

  end
end
