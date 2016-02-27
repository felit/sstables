require 'rspec'
require 'tmpdir'
require_relative '../sstable'

shared_examples_for 'basic kv operations' do
  it 'allows key-value get/set' do
    subject.set 'key-1', 'value-1'
    expect(subject.get 'key-1').to eq('value-1')
  end

  it 'does not let you set non-strings' do
    expect { subject.set 'key-1', nil }.to raise_error(ArgumentError)
    expect { subject.set 'key-1', 1234 }.to raise_error(ArgumentError)
  end

  it 'allows setting a key again' do
    subject.set 'key-1', 'value-1'
    expect(subject.get 'key-1').to eq('value-1')
    subject.set 'key-1', 'value-2'
    expect(subject.get 'key-1').to eq('value-2')
  end

  it 'knows a key does not exist' do
    expect(subject.get 'key-1').to be_nil
  end

  it 'deletes a key' do
    subject.set 'foo', 'bar'
    subject.delete 'foo'
    expect(subject.get 'foo').to be_nil
  end
end

describe 'SSTable from scratch' do
  around { |ex| Dir.mktmpdir { |d| @tmpdir = d; ex.run } }
  subject { SSTable.new(@tmpdir) }

  it_behaves_like 'basic kv operations'

  it 'flushes to disk' do
    subject.set 'foo', 'bar'
    subject.set 'bar', 'baz'
    subject.flush
    expect(subject.get('foo')).to eq('bar')
    expect(subject.get('bar')).to eq('baz')
  end

  it 'deletes keys when flushing to disk' do
    subject.set 'foo', 'bar'
    expect(subject.get('foo')).to eq('bar')
    subject.flush
    subject.delete 'foo'
    expect(subject.get('foo')).to be_nil
    subject.flush
    expect(subject.get('foo')).to be_nil
  end

  describe 'when loaded from disk' do
    around { |ex| Dir.mktmpdir { |d| @tmpdir = d; ex.run } }
    subject { SSTable.new(@tmpdir) }

    it_behaves_like 'basic kv operations'
  end
end
