package main

import (
	"compress/gzip"
	"fmt"
	"os"
	"path"

	"github.com/kshedden/gzutils"
	"github.com/kshedden/hashcluster"
)

var (
	//hdir string = "/nfs/kshedden/Teal_Furnholm/mockhashes"
	hdir string = "/nfs/kshedden/Teal_Furnholm/simhashes"

	//fasta_fname string = "/nfs/kshedden/Teal_Furnholm/NCBI_MOCK/NCBI_MOCK_PROT_SMALL_SORT.fasta.gz"
	fasta_fname string = "/nfs/kshedden/Teal_Furnholm/simulated.fasta.gz"

	numhash int = 100

	km int = 10
)

func main() {

	fid, err := os.Open(fasta_fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}

	hnames := make([]string, numhash)
	rnames := make([]string, numhash)
	for i := 0; i < numhash; i++ {
		fname := fmt.Sprintf("%02d_raw.bin.gz", i)
		fname = path.Join(hdir, fname)
		hnames[i] = fname
		fname = fmt.Sprintf("%02d.bin.gz", i)
		fname = path.Join(hdir, fname)
		rnames[i] = fname
	}

	hashout := gzutils.NewGZFileWriters(hnames)
	fname := path.Join(hdir, "names.gz")
	namesout := gzutils.NewGZFileWriter(fname)
	posname := path.Join(hdir, "pos.gz")
	posout := gzutils.NewGZFileWriter(posname)

	hashcluster.GenHashes(rdr, hashout.GetWriters(), namesout.GetWriter(),
		posout.GetWriter(), km)

	hashout.Close()
	namesout.Close()
	posout.Close()

	hashin := gzutils.NewGZFileReaders(hnames)
	hashout2 := gzutils.NewGZFileWriters(rnames)
	posin := gzutils.NewGZFileReader(posname)

	hashcluster.SortHashes(hashin.GetReaders(), posin.GetReader(), hashout2.GetWriters())

	hashout2.Close()
}
