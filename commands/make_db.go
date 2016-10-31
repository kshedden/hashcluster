// Create a leveldb database containing the sequence data.

package main

import (
	"compress/gzip"
	"fmt"
	"os"
	"path"

	"github.com/kshedden/hashcluster"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	sequence_file string = "All_Genes_Derep.fasta.gz"

	sequence_path string = "/data/kshedden/Teal_Furnholm"

	dbpath string = "/data/kshedden/Teal_Furnholm/sequence_db"
)

func main() {

	var err error
	err = os.RemoveAll(dbpath)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(dbpath, 0777)
	if err != nil {
		panic(err)
	}
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		panic(err)
	}

	fname := path.Join(sequence_path, sequence_file)
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	fr := hashcluster.NewFastaReader(rdr)

	var val []byte

	// Generate all the hashes
	for jj := 0; fr.Read(); jj++ {
		if jj%1000000 == 0 {
			fmt.Printf("%d\n", jj)
		}

		_, seq := fr.Get()
		key := fmt.Sprintf("%d", jj)

		if cap(val) < len(seq) {
			val = make([]byte, len(seq))
		}
		val = val[0:len(seq)]
		for i, v := range seq {
			val[i] = []byte(fmt.Sprintf("%d", v))[0]
		}

		db.Put([]byte(key), val, nil)
	}

	db.Close()
}
