package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/syndtr/goleveldb/leveldb/opt"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	log      = logging.Logger("dagstore-kv-backend")
	repopath string
	gi       int
)

func init() {
	logging.SetLogLevel("*", "info")

	flag.StringVar(&repopath, "repopath", "", "path for repo")
}

func main() {
	var err error
	repopath, err := ioutil.TempDir("", "dagstore-kv-backend")
	if err != nil {
		panic(err)
	}

	log.Infow("using repopath", "path", repopath)
	db, err := levelDs(repopath, false)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// prepare db
	db.SetNextCursor(ctx, 100)

	indicesPath := "/Users/nonsense/dagstore-indices/"
	log.Infow("using indicesPath", "path", indicesPath)

	indices := getAllIndices(indicesPath)

	for fcid, filepath := range indices {
		pieceCid, err := cid.Parse(fcid)
		if err != nil {
			panic(err)
		}

		subject, err := loadIndex(filepath)
		if err != nil {
			panic(err)
		}

		err = storeIndex(ctx, pieceCid, subject, db)
		if err != nil {
			panic(err)
		}

		subjectDb, err := loadIndexFromDb(ctx, db, pieceCid)
		if err != nil {
			panic(err)
		}

		ok, err := compareIndices(subject, subjectDb)
		if err != nil {
			panic(err)
		}
		if !ok {
			log.Fatal("compare failed")
		}
	}

	log.Infow("all good")

	time.Sleep(3 * time.Second)

	indicesSize, _ := DirSize(indicesPath)
	ldbSize, _ := DirSize(repopath)

	log.Infow("cursor overhead", "count", gi, "overhead", ByteCountSI(int64(gi*8)))
	log.Infow("size indices", "size", ByteCountSI(indicesSize))
	log.Infow("size leveldb", "size", ByteCountSI(ldbSize))
}

func levelDs(path string, readonly bool) (*DB, error) {
	ldb, err := levelds.NewDatastore(path, &levelds.Options{
		Compression:         ldbopts.SnappyCompression,
		NoSync:              true,
		Strict:              ldbopts.StrictAll,
		ReadOnly:            readonly,
		CompactionTableSize: 4 * opt.MiB,
	})
	if err != nil {
		return nil, err
	}

	return &DB{ldb}, nil
}

func getAllIndices(path string) map[string]string {
	result := make(map[string]string)

	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		name := f.Name()

		if strings.Contains(name, "full.idx") {
			filepath := path + name
			name = strings.ReplaceAll(name, ".full.idx", "")

			result[name] = filepath
		}
	}

	return result
}

func storeIndex(ctx context.Context, pieceCid cid.Cid, subject index.Index, db *DB) error {
	defer func(now time.Time) {
		log.Debugw("storeindex", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	// get and set next cursor (handle synchronization, maybe with CAS)
	cursor, keyCursorPrefix, err := db.NextCursor(ctx)
	if err != nil {
		return err
	}

	err = db.SetNextCursor(ctx, cursor+1)
	if err != nil {
		return err
	}

	// put pieceCid in pieceCid->cursor table
	err = db.SetPieceCidToCursor(ctx, pieceCid, cursor)
	if err != nil {
		return err
	}

	// process index and store entries
	switch idx := subject.(type) {
	case index.IterableIndex:
		i := 0
		err := idx.ForEach(func(m multihash.Multihash, offset uint64) error {
			i++
			gi++

			err := db.AddOffset(ctx, keyCursorPrefix, m, offset)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}

		log.Debugf(fmt.Sprintf("processed %d index entries for piece cid %s", i, pieceCid.String()))
	default:
		panic(fmt.Sprintf("wanted %v but got %v\n", multicodec.CarMultihashIndexSorted, idx.Codec()))
	}

	err = db.Sync(ctx, datastore.NewKey(keyCursorPrefix))
	if err != nil {
		return err
	}

	return nil
}

func loadIndex(path string) (index.Index, error) {
	defer func(now time.Time) {
		log.Debugw("loadindex", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	idxf, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer idxf.Close()

	subject, err := index.ReadFrom(idxf)
	if err != nil {
		return nil, err
	}

	return subject, nil
}

func loadIndexFromDb(ctx context.Context, db *DB, pieceCid cid.Cid) (index.Index, error) {
	cursor, err := db.GetPieceCidToCursor(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	records, err := db.AllRecords(ctx, cursor)
	if err != nil {
		return nil, err
	}

	mis := make(index.MultihashIndexSorted)
	err = mis.Load(records)
	if err != nil {
		return nil, err
	}

	return &mis, nil
}

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func compareIndices(subject, subjectDb index.Index) (bool, error) {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	subject.Marshal(w)

	var b2 bytes.Buffer
	w2 := bufio.NewWriter(&b2)

	subjectDb.Marshal(w2)

	res := bytes.Compare(b.Bytes(), b2.Bytes())

	return res == 0, nil
}
