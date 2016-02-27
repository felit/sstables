Playing with SSTables
======================

I'm only [10 years late to the party here][bigtable], but since we are
deploying Cassandra at work and I didn't learn anything about SSTables or
Log-Structured-Merge in data structures class, a little bit of homework is
required.

This repository implements a basic key-value store with an API modelled after
LevelDB:

```ruby
> table = SSTable.new('/path/to/workdir')
> table.set 'foo', 'bar'
> table.get 'foo'
=> 'bar'
> table.flush                # (writes to disk)

> table2 = SSTable.new('/path/to/workdir')
> table2 = table.get 'foo'
=> 'bar'
> table2.delete 'foo'
> table2.get 'foo'
=> nil
```

This implementation has two limitations:

1. all keys & values must be strings
2. keys must not contain null bytes

Implementation Details
-----------------
Inside of the directory given as a parameter to `SSTable.new`, two files are
created:

* `index`
* `table`

The *index* file contains a serialization of a Ruby hash.  The keys are the
keys the user inserted into the SSTable and the values are the byte offset of
that entry in the SSTable.

The *table* file contains a list of entries in the format:

  4-byte int - Length of Key
  4-byte int - Length of Value
  n-byte utf8 - Key
  n-byte utf8 - Value

I chose to use the length headers as a hack for easy iteration over the table
file, although I'm pretty sure it would be possible to iterate over the file
using offsets found in the index file. This would be preferable because it
lessens the storage overhead per-kv-pair, but would either require sorting the
index offsets (to avoid tons of disk seeks when iterating) or sorting the keys
and thus offsets (as LevelDB does, but I don't yet implement).

When get/set/delete operations are performed, they are not immediately written
to disk. Rather, they are applied in-memory to a *memtable* (which is just a
combination of a Ruby Hash and a Set of keys to remove). When the user calls
`SSTable#flush` then the contents of the memtable are merged with the SSTable
on disk.

Testing
-----------------
```bash
gem install rspec
rspec
```

[bigtable]: http://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf
