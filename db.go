package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

var (
	// LevelDB key value for storing next free cursor.
	keyNextCursor   uint64 = 0
	dskeyNextCursor datastore.Key
	// LevelDB key prefix for PieceCid to cursor table.
	// LevelDB keys will be built by concatenating PieceCid to this prefix.
	prefixPieceCidToCursor  uint64 = 1
	sprefixPieceCidToCursor string

	size    = binary.MaxVarintLen64
	cutsize = size + 2
)

func init() {
	buf := make([]byte, size)
	binary.PutUvarint(buf, keyNextCursor)
	dskeyNextCursor = datastore.NewKey(string(buf))

	buf = make([]byte, size)
	binary.PutUvarint(buf, prefixPieceCidToCursor)
	sprefixPieceCidToCursor = string(buf)
}

type DB struct {
	datastore.Batching
}

// NextCursor
func (db *DB) NextCursor(ctx context.Context) (uint64, string, error) {
	b, err := db.Get(ctx, dskeyNextCursor)
	if err != nil {
		return 0, "", err
	}

	cursor, _ := binary.Uvarint(b)
	return cursor, string(b) + "/", nil // adding "/" because query for datastore
}

// SetNextCursor
func (db *DB) SetNextCursor(ctx context.Context, cursor uint64) error {
	buf := make([]byte, size)
	binary.PutUvarint(buf, cursor)

	return db.Put(ctx, dskeyNextCursor, buf)
}

// SetPieceCidToCursor
func (db *DB) SetPieceCidToCursor(ctx context.Context, pieceCid cid.Cid, cursor uint64) error {
	key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixPieceCidToCursor, pieceCid.String()))

	value := make([]byte, size)
	binary.PutUvarint(value, cursor)

	return db.Put(ctx, key, value)
}

// GetPieceCidToCursor
func (db *DB) GetPieceCidToCursor(ctx context.Context, pieceCid cid.Cid) (uint64, error) {
	key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixPieceCidToCursor, pieceCid.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	cursor, _ := binary.Uvarint(b)
	return cursor, nil
}

// AllRecords
func (db *DB) AllRecords(ctx context.Context, cursor uint64) ([]index.Record, error) {
	var records []index.Record

	buf := make([]byte, size)
	binary.PutUvarint(buf, cursor)

	var q query.Query
	q.Prefix = string(buf)

	results, err := db.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		k := r.Key[cutsize:]

		m, err := multihash.FromHexString(k)
		if err != nil {
			panic(err)
		}

		kcid := cid.NewCidV1(cid.Raw, m)

		offset, _ := binary.Uvarint(r.Value)

		records = append(records, index.Record{
			Cid:    kcid,
			Offset: offset,
		})
	}

	return records, nil
}

// AddOffset
func (db *DB) AddOffset(ctx context.Context, cursorPrefix string, m multihash.Multihash, offset uint64) error {
	key := datastore.NewKey(fmt.Sprintf("%s%s", cursorPrefix, m.String()))

	value := make([]byte, size)
	binary.PutUvarint(value, offset)

	return db.Put(ctx, key, value)
}

// GetOffset
func (db *DB) GetOffset(ctx context.Context, cursorPrefix string, m multihash.Multihash) (uint64, error) {
	key := datastore.NewKey(fmt.Sprintf("%s%s", cursorPrefix, m.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	offset, _ := binary.Uvarint(b)
	return offset, nil
}
