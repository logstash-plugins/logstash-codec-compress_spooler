# encoding: utf-8
require "logstash/codecs/base"

class LogStash::Codecs::CompressSpooler < LogStash::Codecs::Base
  config_name 'compress_spooler'
  config :spool_size, :validate => :number, :default => 50
  config :compress_level, :validate => :number, :default => 6

  public

  def register
    require "msgpack"
    require "zlib"
    @buffer = []
  end

  def decode(data)
    decompress(data).each do |event|
      yield(LogStash::Event.new(event))
    end
  end # def decode

  def encode(event)
    # use normalize to make sure returned Hash is pure Ruby for
    # MessagePack#pack which relies on pure Ruby object recognition
    @buffer << LogStash::Util.normalize(event.to_hash).merge(LogStash::Event::TIMESTAMP => event.timestamp.to_iso8601)
    # If necessary, we flush the buffer and get the data compressed
    if @buffer.length >= @spool_size
      @on_event.call(compress(@buffer, @compress_level))
      @buffer.clear
    end
  end # def encode

  def close
    return if @buffer.empty?
    @on_event.call(compress(@buffer, @compress_level))
    @buffer.clear
  end

  private

  def compress(data, level)
    z = Zlib::Deflate.new(level)
    result = z.deflate(MessagePack.pack(data), Zlib::FINISH)
    z.close
    result
  end

  def decompress(data)
    z = Zlib::Inflate.new
    result = MessagePack.unpack(z.inflate(data))
    z.finish
    z.close
    result
  end
end # class LogStash::Codecs::CompressSpooler
