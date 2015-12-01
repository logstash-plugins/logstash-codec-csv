# encoding: utf-8
require "logstash/codecs/csv"
require "logstash/event"

describe LogStash::Codecs::CSV do

  subject(:codec) { LogStash::Codecs::CSV.new }

  describe "decode" do

    let(:data) { "big,bird,sesame street" }

    before(:each) do
      codec.register
    end

    it "should return an event from CSV data" do
      codec.decode(data) do |event|
        expect(event["column1"]).to eq("big")
        expect(event["column2"]).to eq("bird")
        expect(event["column3"]).to eq("sesame street")

      end
    end
  end
end
