package hashcluster

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
)

func TestMinHash1(t *testing.T) {

	km := 10
	slen := 1000
	nrep := 10

	rand.Seed(34879)

	seq1 := make([]int, slen)
	seq2 := make([]int, slen)

	rslt := make([]float64, 3)

	for jm, mm := range []int{slen, 5, 1} {
		var qs float64
		for i := 0; i < nrep; i++ {
			for j := 0; j < slen; j++ {
				seq1[j] = int(rand.Int63n(4))
				if (j+1)%mm == 0 {
					seq2[j] = int(rand.Int63n(4))
				} else {
					seq2[j] = seq1[j]
				}
			}

			ph := NewPKHash(km)
			h1 := ph.MinHash(seq1)
			h2 := ph.MinHash(seq2)

			hd := h1 - h2
			qs += hd * hd
		}

		rslt[jm] = qs / float64(nrep)
	}

	if rslt[0] != 0 {
		t.Fail()
	}
	if rslt[1] > rslt[2] {
		t.Fail()
	}
}

func TestGenHashes(t *testing.T) {

	rdr := prep()

	km := 10
	numhash := 10
	hashout := make([]io.Writer, numhash)
	hashoutb := make([]bytes.Buffer, numhash)
	for i := 0; i < numhash; i++ {
		hashout[i] = &hashoutb[i]
	}
	var namesout bytes.Buffer
	var posout bytes.Buffer

	GenHashes(rdr, hashout, &namesout, &posout, km)

	// Read the hashes back in with these readers
	hashin := make([]io.Reader, numhash)
	for i := 0; i < numhash; i++ {
		hashin[i] = bytes.NewReader(hashoutb[i].Bytes())
	}

	// Read the position information back
	posr := bytes.NewReader(posout.Bytes())

	// Write the sorted hashes here
	shashoutb := make([]bytes.Buffer, numhash)
	shashout := make([]io.Writer, numhash)

	for i := 0; i < numhash; i++ {
		shashout[i] = &shashoutb[i]
	}
	SortHashes(hashin, shashout, posr)
}
