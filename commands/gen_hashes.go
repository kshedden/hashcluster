package main

import (
	"compress/gzip"
	"fmt"
	"os"
	"path"

	"github.com/kshedden/hashcluster"
	"github.com/kshedden/szutils"
)

var (
	//fasta_fname string = "/nfs/kshedden/Teal_Furnholm/NCBI_MOCK/NCBI_MOCK_PROT_SMALL_SORT.fasta.gz"
	//hdir string = "/nfs/kshedden/Teal_Furnholm/mockhashes"

	fasta_fname string = "/nfs/kshedden/Teal_Furnholm/simulated.fasta.gz"
	hdir        string = "/nfs/kshedden/Teal_Furnholm/simhashes"

	//fasta_fname string = "/scratch/lsa_flux/kshedden/All_Genes_Derep.fasta.gz"
	//hdir string = "/data/kshedden/Teal_Furnholm/hashes"

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
		fname := fmt.Sprintf("%02d_raw.bin.sz", i)
		fname = path.Join(hdir, fname)
		hnames[i] = fname
		fname = fmt.Sprintf("%02d.bin.sz", i)
		fname = path.Join(hdir, fname)
		rnames[i] = fname
	}

	posname := path.Join(hdir, "pos.sz")

	hashout := szutils.NewFileWriters(hnames)
	fname := path.Join(hdir, "names.sz")
	namesout := szutils.NewFileWriter(fname)
	posout := szutils.NewFileWriter(posname)

	hashcluster.GenHashes(rdr, hashout.GetWriters(), namesout,
		posout, km)

	hashout.Close()
	namesout.Close()
	posout.Close()

	hashin := szutils.NewFileReaders(hnames)
	rout := szutils.NewFileWriters(rnames)
	posin := szutils.NewFileReader(posname)

	hashcluster.SortHashes(hashin.GetReaders(), rout.GetWriters(), posin)

	hashin.Close()
	posin.Close()
}
