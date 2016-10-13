package main

import (
	"compress/gzip"
	"hash/adler32"
	"os"
	"strings"

	"github.com/kshedden/hashcluster"
)

type empty_t struct{}

func main() {

	if len(os.Args) != 2 {
		panic("usage: dedup infile")
	}

	infile := os.Args[1]
	outfile := strings.Replace(infile, ".gz", "dedup.gz", 1)

	fr := hashcluster.NewFastaReader(infile)

	ofd, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}
	defer ofd.Close()
	out := gzip.NewWriter(ofd)
	defer out.Close()

	hash := adler32.New()

	seen := make(map[uint32]empty_t)

	// Generate all the hashes
	for fr.Read() {
		name, seq := fr.Get()

		hv := hash.Checksum([]byte(seq))
		_, ok := seen[hv]
		if !ok {
			out.Write([]byte(name))
			out.Write([]byte("\n"))
			out.Write([]byte(seq))
			out.Write("\n")
			seen[hv] = empty_t
		}
	}
}
