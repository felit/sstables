require_relative 'sstable'
require 'tmpdir'
require 'benchmark'

Dir.mktmpdir do |dir|
  sstable = SSTable.new(dir)
  total = 0
  start = Time.now
  $stop = false

  Thread.abort_on_exception = true
  Thread.new do
    loop do
      sstable.flush
      sleep 1
    end
  end

  Thread.new do
    loop do
      break if $stop
      elapsed = Time.now - start
      puts "time:#{elapsed.round}\tsets:\t#{(total / elapsed).round}/s"
      sleep 1
    end
  end

  loop do
    key = Random.rand(50000).to_s
    value = Random.rand(100000).to_s
    sstable.set key, value
    actual = sstable.get(key)
    if actual == value
      total += 1
      sstable.delete(key) if Random.rand > 0.9
    else
      puts "ERROR bad key/value; expected #{value.inspect} got #{actual.inspect}"
    end
  end
end
