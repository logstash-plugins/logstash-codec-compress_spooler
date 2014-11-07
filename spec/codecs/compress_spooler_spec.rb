require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/compress_spooler"

describe LogStash::Codecs::CompressSpooler do

  it "should work" do
    expect(true).to be true
  end

  context "#decode" do
    it "should return an event from msgpack data" do
      codec = LogStash::Codecs::CompressSpooler.new
      events = [{"foo" => "bar", "baz" => {"bah" => ["a", "b", "c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"}]

      z = Zlib::Deflate.new
      data = z.deflate(MessagePack.pack(events), Zlib::FINISH)
      z.close

      codec.decode(data) do |event|
        insist { event.is_a? LogStash::Event }
        insist { event["foo"] } == events.first["foo"]
        insist { event["baz"] } == events.first["baz"]
        insist { event["bah"] } == events.first["bah"]
        insist { event["@timestamp"].to_iso8601 } == events.first["@timestamp"]
      end
    end
  end

  context "#encode" do

    it "should return compressed data from pure ruby hash" do
      codec = LogStash::Codecs::CompressSpooler.new("spool_size" => 1)
      data = {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"}
      event = LogStash::Event.new(data)
      results = []
      codec.on_event{|data| results << data}

      # spool_size is one so encode twice to trigger encoding on the 2nd one
      codec.encode(event)
      codec.encode(event)

      insist { results.size } == 1
      z = Zlib::Inflate.new
      events = MessagePack.unpack(z.inflate(results.first))
      z.finish
      z.close

      insist { events.first["foo"] } == data["foo"]
      insist { events.first["baz"] } == data["baz"]
      insist { events.first["bah"] } == data["bah"]
      insist { events.first["@timestamp"] } == "2014-05-30T02:52:17.929Z"
      insist { events.first["@timestamp"] } == event["@timestamp"].to_iso8601
    end

    it "should return compressed data from deserialized json with normalization" do
      codec = LogStash::Codecs::CompressSpooler.new("spool_size" => 1)
      data = LogStash::Json.load('{"foo": "bar", "baz": {"bah": ["a","b","c"]}, "@timestamp": "2014-05-30T02:52:17.929Z"}')
      event = LogStash::Event.new(data)
      results = []
      codec.on_event{|data| results << data}

      # spool_size is one so encode twice to trigger encoding on the 2nd one
      codec.encode(event)
      codec.encode(event)

      insist { results.size } == 1
      z = Zlib::Inflate.new
      events = MessagePack.unpack(z.inflate(results.first))
      z.finish
      z.close

      insist { events.first["foo"] } == data["foo"]
      insist { events.first["baz"] } == data["baz"]
      insist { events.first["bah"] } == data["bah"]
      insist { events.first["@timestamp"] } == "2014-05-30T02:52:17.929Z"
      insist { events.first["@timestamp"] } == event["@timestamp"].to_iso8601
    end

    it "should support teardown and flush pending data" do
      codec = LogStash::Codecs::CompressSpooler.new("spool_size" => 1)
      data = {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"}
      event = LogStash::Event.new(data)
      results = []
      codec.on_event{|data| results << data}

      codec.encode(event)
      codec.teardown

      insist { results.size } == 1
      z = Zlib::Inflate.new
      events = MessagePack.unpack(z.inflate(results.first))
      z.finish
      z.close

      insist { events.first["foo"] } == data["foo"]
      insist { events.first["baz"] } == data["baz"]
      insist { events.first["bah"] } == data["bah"]
      insist { events.first["@timestamp"] } == "2014-05-30T02:52:17.929Z"
      insist { events.first["@timestamp"] } == event["@timestamp"].to_iso8601
    end
  end

end
