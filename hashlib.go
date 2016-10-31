// These functions are used to generate and sort minhashes for a
// collection of biological sequences.
//
// The minhash is contructed from a kmer hash.  The kmer hash is
// applied on a rolling basis to the entire sequence.  The minhash is
// the minimum of these kmer hash values.
//
// The kmer hash is obtained by assigning iid standard Gaussian values
// to each base at each location in the kmer, then summing the values
// based on a given kmer sequence.
//
// The functions defined below generate a given number of minhashes
// for an input set of sequences.  In the first stage, the sequences
// are processed in a fixed order (not necessarily in their input
// order).  The order information and all minhash values are written
// to files.  In the second stage, each sequence of minhash values is
// argsorted.
//
// Example:
//
// The order in which the results are stored, relative to the ordering
// in the sequence file:
//   position = [3, 1, 0, 2]
//
// Two sequences of minhash values:
//   hash1 = [0.3, -0.5, -1.2, -4.5]
//   hash2 = [-0.2, -0.1, -3.2, 1.5]
//
// The final result:
//   srt1 = [2, 0, 1, 3]
//   srt2 = [0, 3, 1, 2]

package hashcluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"runtime"
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
		seq_ix    uint32
		name      string
		hash_vals []float32
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
			err := binary.Write(posout, binary.LittleEndian, uint32(r.seq_ix))
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
			hash_vals := make([]float32, numhash)
			for k := 0; k < numhash; k++ {
				hash_vals[k] = float32(hashes[k].MinHash(seq))
			}
			rchan <- &qrec{seq_ix: uint32(seq_ix), name: name, hash_vals: hash_vals}
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

// SortHashes argsorts sequences of minhash values.
func SortHashes(hashin []io.Reader, rout []io.Writer, posin io.Reader) {

	// Read position information
	var idx []uint32
	for {
		var x uint32
		err := binary.Read(posin, binary.LittleEndian, &x)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		idx = append(idx, x)
	}

	// Workspaces
	hashw := make([]float32, len(idx))
	inds := make([]uint32, len(idx))

	numhash := len(hashin)
	for k := 0; k < numhash; k++ {
		fmt.Printf("  %d\n", k)
		sortbyhash(hashin[k], rout[k], idx, hashw, inds)
		runtime.GC()
	}
}

// argsort is a helper that implements sort.Interface, as used by
// Argsort.
type argsort struct {
	s    []float32
	inds []uint32
}

func (a argsort) Len() int {
	return len(a.s)
}

func (a argsort) Less(i, j int) bool {
	return a.s[i] < a.s[j]
}

func (a argsort) Swap(i, j int) {
	a.s[i], a.s[j] = a.s[j], a.s[i]
	a.inds[i], a.inds[j] = a.inds[j], a.inds[i]
}

// Argsort sorts the elements of dst while tracking their original
// order.  At the conclusion of Argsort, dst will contain the original
// elements of dst but sorted in increasing order, and inds will
// contain the original position of the elements in the slice such
// that dst[i] = origDst[inds[i]].  It panics if the lengths of dst
// and inds do not match.
func argsort32(dst []float32, inds []uint32) {
	if len(dst) != len(inds) {
		panic("floats: length of inds does not match length of slice")
	}
	for i := range dst {
		inds[i] = uint32(i)
	}

	a := argsort{s: dst, inds: inds}
	sort.Sort(a)
}

func sortbyhash(hashin io.Reader, rout io.Writer, ixv []uint32, hashw []float32, inds []uint32) {

	for jj := 0; ; jj++ {
		var x float32
		err := binary.Read(hashin, binary.LittleEndian, &x)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		hashw[int(ixv[jj])] = x
	}

	argsort32(hashw, inds)

	err := binary.Write(rout, binary.LittleEndian, inds)
	if err != nil {
		panic(err)
	}
}
