require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/compress_spooler"
require "msgpack"
require "zlib"

describe LogStash::Codecs::CompressSpooler do

  subject(:codec) { LogStash::Codecs::CompressSpooler.new }

  describe "register and close" do

    it "registers without any error" do
      expect { codec.register }.to_not raise_error
    end

    it "tearndown without erros" do
      expect { codec.close }.to_not raise_error
    end

  end

  describe "#decode" do

    let(:events) { [{"foo" => "bar", "baz" => {"bah" => ["a", "b", "c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"}] }
    let!(:zlib)   { Zlib::Deflate.new }
    let!(:data)   { zlib.deflate(MessagePack.pack(events), Zlib::FINISH) }

    before(:each) do
      zlib.close
    end

    it "return single properties as expected from message pack" do
      codec.decode(data) do |event|
        expect(event.to_hash).to include("foo" => "bar")
      end
    end

    it "return nested hash as expected from message pack" do
      codec.decode(data) do |event|
        expect(event.to_hash).to include("baz" => {"bah" => ["a", "b", "c"]} )
      end
    end

    it "return a proper codec timestamp from message pack" do
      codec.decode(data) do |event|
        expect( event["@timestamp"].to_iso8601).to eq("2014-05-30T02:52:17.929Z")
      end
    end

  end

  shared_examples 'Encoding data' do

    it "encode one element" do
      expect(results.size).to eq(1)
    end


    context "and inspecting compressed results" do

      let!(:zlib)   { Zlib::Inflate.new }
      let!(:events) { MessagePack.unpack(zlib.inflate(results.first)) }
      let(:unpack_event)   { events.first }

      before(:each) do
        zlib.finish
        zlib.close
      end

      it "return single properties as expected from message pack" do
        expect(unpack_event).to include("foo" => "bar")
      end

      it "return nested hash as expected from message pack" do
        expect(unpack_event).to include("baz" => {"bah" => ["a", "b", "c"]} )
      end

      it "return a proper codec timestamp from message pack" do
        expect(unpack_event["@timestamp"]).to eq("2014-05-30T02:52:17.929Z")
      end

      it "return the timestamp as iso8601" do
        expect(unpack_event["@timestamp"]).to eq(event["@timestamp"].to_iso8601)
      end
    end

  end

  describe "#encode" do
    subject(:codec) { LogStash::Codecs::CompressSpooler.new("spool_size" => 1) }

    let(:event)     { LogStash::Event.new(data) }
    let(:results)   { [] }

    context "encoding a ruby hash" do

      let(:data) { {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"} }

      before(:each) do
        codec.on_event{|data| results << data}
        codec.encode(event)
      end

      include_examples "Encoding data"
    end

    context "encoding a json with normalization" do

      let(:data) { LogStash::Json.load('{"foo": "bar", "baz": {"bah": ["a","b","c"]}, "@timestamp": "2014-05-30T02:52:17.929Z"}') }

      before(:each) do
        codec.on_event{|data| results << data}
        codec.encode(event)
      end
      include_examples "Encoding data"
    end

    context "when flushing pending data during close" do
      let(:data)  { {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"} }

      before(:each) do
        codec.on_event{|data| results << data}
        codec.encode(event)
        codec.close
      end
      include_examples "Encoding data"

      context "message spooling when flushing events to the compressor" do
        let(:spool_size) { 4 }
        subject(:codec) { LogStash::Codecs::CompressSpooler.new("spool_size" => spool_size) }
        let(:data) { {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z" } }

        before(:each) do
          codec.on_event{|data| results << data}
          spool_size.times do
            codec.encode(event)
          end
        end

        it "dont't lost messages fireing the compression process" do
          2.times { codec.encode(event) }
          buffer= codec.instance_variable_get(:@buffer)
          expect(buffer.size).to eq(2)
        end
      end

      context "message spooling if min flush time is set" do
        let(:spool_size) { 10 }
        min_flush_time = 2
        let(:min_flush_time) { min_flush_time }
        subject(:codec) { LogStash::Codecs::CompressSpooler.new("spool_size" => spool_size, "min_flush_time" => min_flush_time) }
        let(:data) { {"foo" => "bar"} }

        it "dont'f flush the initial messages, but flush after min flush time on next message" do
          codec.encode(event)
          buffer= codec.instance_variable_get(:@buffer)
          expect(buffer.size).to eq(1)

          codec.encode(event)
          buffer= codec.instance_variable_get(:@buffer)
          expect(buffer.size).to eq(2)

          sleep(min_flush_time + 1)

          buffer= codec.instance_variable_get(:@buffer)
          expect(buffer.size).to eq(2)

          codec.encode(event)
          buffer= codec.instance_variable_get(:@buffer)
          expect(buffer.size).to eq(0)
        end
      end
    end
  end
end
