package hashcluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
)

// PKHash is a projection kmer min-hash.  K-mers are hashed with a
// random projection of their sequence indicators.  The sequence is
// hashed as the minimum of its constituent k-mer hashes.
type PKHash struct {

	// The kmer length
	km int

	// Weights for each base at each position in the kmer
	kwt [][]float64
}

// NewPKHash returns a new PKHash instance with the given kmer width.
func NewPKHash(km int) *PKHash {

	ph := new(PKHash)
	ph.km = km

	ph.kwt = make([][]float64, km)
	for j := 0; j < km; j++ {
		ph.kwt[j] = make([]float64, 4)
		for i := 0; i < 4; i++ {
			ph.kwt[j][i] = rand.NormFloat64()
		}
	}

	return ph
}

// MinHash returns an overall min-hash for a sequence.
func (ph *PKHash) MinHash(seq []int) float64 {

	var h float64
	m := len(seq) - ph.km
	for i := 0; i < m; i++ {
		c := ph.Calc(seq[i : i+ph.km])
		if (i == 0) || (c < h) {
			h = c
		}
	}

	return h
}

// Calc returns a hash value for a single kmer.
func (ph *PKHash) Calc(seq []int) float64 {
	var h float64
	for j := 0; j < len(seq); j++ {
		h += ph.kwt[j][seq[j]]
	}
	return h
}

// GenHashes cycles through an ordered collection of sequences in
// fasta format and generates multiple hashes for each sequence,
// writing the results to the supplied Writers.  All hashes have the
// given kmer length km.  The length of the hashout array determines
// the number of hashes that are generated.
//
// The hashes are not produced in the same order as the input
// sequences, but all the output streams are ordered in the same way.
// The ordering information is written to posout and the names (in the
// ouput order) are written to namesout.
func GenHashes(fasta io.Reader, hashout []io.Writer, namesout io.Writer, posout io.Writer, km int) {

	type qrec struct {
		seq_ix    int64
		name      string
		hash_vals []float64
	}

	numhash := len(hashout)
	fr := NewFastaReader(fasta)

	hashes := make([]*PKHash, numhash)
	for k := 0; k < numhash; k++ {
		hashes[k] = NewPKHash(km)
	}

	rchan := make(chan *qrec)
	ncon := 10 // limit concurrency
	sem := make(chan bool, ncon)
	done := make(chan bool, 1)

	// Run this concurrently to harvest data from the channel and write to disk.
	go func() {
		for r := range rchan {
			err := binary.Write(posout, binary.LittleEndian, r.seq_ix)
			if err != nil {
				panic(err)
			}

			_, err = namesout.Write([]byte(r.name))
			if err != nil {
				panic(err)
			}
			_, err = namesout.Write([]byte("\n"))
			if err != nil {
				panic(err)
			}

			for k := 0; k < numhash; k++ {
				err = binary.Write(hashout[k], binary.LittleEndian, &r.hash_vals[k])
				if err != nil {
					panic(err)
				}
			}
		}
		done <- true
	}()

	// Generate all the hashes
	for jj := 0; fr.Read(); jj++ {
		name, seq := fr.Get()

		sem <- true

		// Generate all hashes for one sequence
		go func(seq []int, seq_ix int) {
			defer func() { <-sem }()
			hash_vals := make([]float64, numhash)
			for k := 0; k < numhash; k++ {
				hash_vals[k] = hashes[k].MinHash(seq)
			}
			rchan <- &qrec{seq_ix: int64(seq_ix), name: name, hash_vals: hash_vals}
		}(seq, jj)

		if jj%1000 == 0 {
			fmt.Printf("%d\n", jj)
		}
	}

	// Wait for all hashes to be computed then close the channel
	for k := 0; k < ncon; k++ {
		sem <- true
	}
	close(rchan)

	// Wait for the file writing to finish
	<-done
}

// The hashes are sorted and stored in row-wise as (index, hash),
// where index is int64 and hash is float64.
func SortHashes(in []io.Reader, pos io.Reader, out []io.Writer) {

	var idx []int64
	for {
		var x int64
		err := binary.Read(pos, binary.LittleEndian, &x)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		idx = append(idx, x)
	}

	numhash := len(in)
	ncon := 10 // limit concurrency
	sem := make(chan bool, ncon)
	for k := 0; k < numhash; k++ {
		sem <- true
		go sortbyhash(in[k], out[k], idx, sem)
		fmt.Printf("  %d\n", k)
	}

	// Wait for all sorting to complete before exiting
	for k := 0; k < ncon; k++ {
		sem <- true
	}
}

type drec struct {
	i int64
	h float64
}

type drecs []drec

func (a drecs) Len() int           { return len(a) }
func (a drecs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a drecs) Less(i, j int) bool { return a[i].h < a[j].h }

func sortbyhash(in io.Reader, out io.Writer, ixv []int64, sem chan bool) {

	defer func() { <-sem }()

	var seq []drec

	jj := 0
	for {
		var x float64
		err := binary.Read(in, binary.LittleEndian, &x)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		seq = append(seq, drec{ixv[jj], x})
		jj++
	}

	sort.Sort(drecs(seq))

	for _, v := range seq {
		err := binary.Write(out, binary.LittleEndian, &v)
		if err != nil {
			panic(err)
		}
	}
}
