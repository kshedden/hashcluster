package main

import (
	"hash/fnv"
	"os"
	"strings"

	"github.com/kshedden/gzutils"
	"github.com/kshedden/hashcluster"
)

type empty_t struct{}

func main() {

	if len(os.Args) != 2 {
		panic("usage: dedup fastafile dropfile")
	}

	var empty empty_t

	infile := os.Args[1]
	outfile := strings.Replace(infile, ".fasta.gz", "_dedup.fasta.gz", 1)

	rdr := gzutils.NewGZFileReader(infile)
	defer rdr.Close()
	fr := hashcluster.NewFastaReader(rdr)

	out := gzutils.NewGZFileWriter(outfile)
	defer out.Close()

	logfile := strings.Replace(infile, ".fasta.gz", "_log.gz", 1)
	log := gzutils.NewGZFileWriter(logfile)
	defer log.Close()

	h := fnv.New64()

	seen := make(map[uint64]empty_t)

	// Generate all the hashes
	for fr.Read() {
		name, seq := fr.GetRaw()

		h.Reset()
		h.Write([]byte(seq))
		hv := h.Sum64()
		_, ok := seen[hv]
		if !ok {
			out.Write([]byte(">"))
			out.Write([]byte(name))
			out.Write([]byte("\n"))
			out.Write([]byte(seq))
			out.Write([]byte("\n"))
			seen[hv] = empty
		} else {
			log.Write([]byte(name))
			log.Write([]byte("\n"))
		}
	}
}
