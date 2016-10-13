package hashcluster

import (
	"bytes"
	"io"
	"testing"
)

var (
	namex []string = []string{"1", "2", "3"}
	seqr  []string = []string{
		">1\nATTAGCAG\nGCATTACC\n",
		">2\nTGACCGAA\nCTAGAGGC\n",
		">3\nGCGGTCAT\nGTGAAGGT\n",
	}
	seqx [][]int = [][]int{
		[]int{0, 1, 1, 0, 2, 3, 0, 2, 2, 3, 0, 1, 1, 0, 3, 3},
		[]int{1, 2, 0, 3, 3, 2, 0, 0, 3, 1, 0, 2, 0, 2, 2, 3},
		[]int{2, 3, 2, 2, 1, 3, 0, 1, 2, 1, 2, 0, 0, 2, 2, 1},
	}
	seqt []string = []string{
		"ATTAGCAGGCATTACC",
		"TGACCGAACTAGAGGC",
		"GCGGTCATGTGAAGGT",
	}
)

func prep() io.Reader {

	var buf bytes.Buffer
	for _, x := range seqr {
		buf.Write([]byte(x))
	}

	return bytes.NewReader(buf.Bytes())
}

func TestInts(t *testing.T) {

	raw := prep()
	rdr := NewFastaReader(raw)

	j := 0
	for rdr.Read() {
		name, seq := rdr.Get()

		if name != namex[j] {
			t.Fail()
		}
		if len(seq) != len(seqx[j]) {
			t.Fail()
		}
		for i := 0; i < len(seq); i++ {
			if seq[i] != seqx[j][i] {
				t.Fail()
			}
		}
		j++
	}
}

func TestRaw(t *testing.T) {

	raw := prep()
	rdr := NewFastaReader(raw)

	j := 0
	for rdr.Read() {
		name, seq := rdr.GetRaw()

		if name != namex[j] {
			t.Fail()
		}
		if len(seq) != len(seqx[j]) {
			t.Fail()
		}
		if seq != seqt[j] {
			t.Fail()
		}
		j++
	}
}
