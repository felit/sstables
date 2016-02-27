require 'set'
require 'fileutils'
require 'tempfile'
require 'thread'

class SSTable
  def initialize(filepath)
    @dir = filepath
    reload_from_disk
  end

  def reload_from_disk
    @index = load_index
    @disktable = load_disk_table
    @memtable = {}
    @memtable_tombstones = Set.new
    @memtable_lock = Mutex.new
  end

  def get(key)
    raise ArgumentError unless String === key
    return nil if @memtable_tombstones.include?(key)

    @memtable_lock.synchronize do
      return @memtable[key] if @memtable.include?(key)
    end

    if @index.include?(key)
      offset = @index.offset(key)
      return @disktable.fetch_offset(offset)[:value]
    end

    nil
  end

  def set(key, value)
    raise ArgumentError unless String === key && String === value

    @memtable_lock.synchronize do
      @memtable_tombstones.delete(key)
      @memtable[key] = value
    end
  end

  def delete(key)
    raise ArgumentError unless String === key

    @memtable_lock.synchronize do
      @memtable_tombstones << key
    end
  end

  def flush
    @disktable.lock do
      @memtable_lock.synchronize do
        @disktable.combine_memtable(@memtable, @memtable_tombstones) do |new_table_path, new_index_path|
          FileUtils.mv(new_table_path, table_path)
          FileUtils.mv(new_index_path, index_path)
        end

        reload_from_disk
      end
    end
  end

  private

  def index_path
    File.join(@dir, 'index')
  end

  def table_path
    File.join(@dir, 'table')
  end

  def load_index
    return SSTable::Index.new unless File.exist?(index_path)

    SSTable::Index.from_file(index_path)
  end

  def load_disk_table
    return SSTable::DiskTable::NullTable.new unless File.exist?(table_path)

    SSTable::DiskTable::FileTable.new(table_path)
  end
end

class SSTable::DiskTable
  class Base
    def combine_memtable(memtable, memtable_tombstones)
      f_index = Tempfile.new('index')
      f_index.binmode
      f_table = Tempfile.create('table')
      f_table.binmode

      each do |kv|
        next if memtable_tombstones.include?(kv[:key])

        current_offset = f_table.tell
        current_value = kv[:value]

        if memtable.include?(kv[:key])
          current_value = memtable[kv[:key]]
        end

        if current_value.nil?
          require 'pry';binding.pry
        end
        f_table.write([kv[:key].length, current_value.length].pack('LL'))
        f_table.write(kv[:key])
        f_table.write(current_value)
        f_index.write([kv[:key], "\0", current_offset, "\0"].join)

        memtable.delete(kv[:key])
      end

      memtable.each do |k, v|
        next if memtable_tombstones.include?(k)

        current_offset = f_table.tell

        f_table.write([k.length, v.length].pack('LL'))
        f_table.write([k, v].join)
        f_index.write([k, "\0", current_offset, "\0"].join)
      end

      f_index.close
      f_table.close

      yield [f_table.path, f_index.path]
    end
  end

  class NullTable < Base
    def initialize
    end

    def fetch_offset
      raise 'Cannot fetch from an empty table!'
    end

    def each
    end

    def lock
      yield
    end
  end

  class FileTable < Base
    def initialize(f)
      @f = File.open(f, 'rb')
      @mutex = Mutex.new
    end

    def lock(&block)
      @mutex.synchronize(&block)
    end

    def fetch_offset(offset)
      @f.seek(offset)
      key_len, value_len = @f.read(8).unpack('LL')

      {
        key: @f.read(key_len),
        value: @f.read(value_len)
      }
    end

    def each
      @f.seek(0)
      while !@f.eof?
        h = fetch_offset(@f.tell)
        yield h
      end
    end
  end

end

class SSTable::Index
  def self.from_file(f)
    new(Hash[File.read(f).split("\0").each_slice(2).map { |k, v| [k, v.to_i] }])
  end

  def initialize(index = {})
    @index = index
  end

  def offset(key)
    @index.fetch(key)
  end

  def include?(key)
    @index.include?(key)
  end
end
