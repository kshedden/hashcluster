package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/golang/snappy"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	dpath string = "/data/kshedden/Teal_Furnholm/hashes"

	numhash int = 100

	wg sync.WaitGroup

	qw int = 20

	db *leveldb.DB
)

func process(hash_ix int) {

	fname := fmt.Sprintf("%02d.bin.sz", hash_ix)
	fname = path.Join(dpath, fname)
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	rdr := snappy.NewReader(fid)

	qu := make([][]byte, qw)

	for jj := 0; ; jj++ {

		if jj%1000000 == 0 {
			fmt.Printf("%d  %d\n", hash_ix, jj)
		}
		var v uint32
		err = binary.Read(rdr, binary.LittleEndian, &v)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		pos := jj % qw
		qu[pos] = []byte(fmt.Sprintf("%d", v))

		// Not full yet
		if jj < qw {
			continue
		}

		r := (pos + qw/2) % qw

		var u []byte
		u, err = db.Get(qu[r], nil)
		if (err != nil) && (err != leveldb.ErrNotFound) {
			panic(err)
		}
		u = append(u, []byte(",")...)
		u = append(u, bytes.Join(qu, []byte(","))...)
		err = db.Put(qu[r], u, nil)
		if err != nil {
			panic(err)
		}
	}

	fid.Close()
	wg.Done()
}

func main() {

	var err error
	fname := path.Join(dpath, "clusters")
	err = os.RemoveAll(fname)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(fname, 0777)
	if err != nil {
		panic(err)
	}
	db, err = leveldb.OpenFile(fname, nil)
	if err != nil {
		panic(err)
	}

	for k := 0; k < 10; k++ {
		wg.Add(1)
		go process(k)
		//process(k)
	}

	wg.Wait()

	db.Close()
}
