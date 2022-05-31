# schema

schema for storing a dagstore index in a key/value store

## prefixes

prefix `varint encoding of 0` -- contains a single system field - next free cursor

prefix `varint encoding of 1` -- contains a map[pieceCid]cursor

prefix `varint encoding of 100` onwards -- contains a map[cid]offset for a given pieceCid's index
