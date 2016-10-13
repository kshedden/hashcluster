package hashcluster

import (
	"bufio"
	"io"
	"strings"
)

// FastaReader reads biological sequences in fasta format.
type FastaReader struct {
	scanner *bufio.Scanner
	names   []string
	seqraw  []string
}

// NewFastaReader creates a reader that produces sequence data in
// various formats from the given io.Reader that produces a text
// stream in fasta format.
func NewFastaReader(rdr io.Reader) *FastaReader {
	scanner := bufio.NewScanner(rdr)
	fr := &FastaReader{scanner: scanner}
	fr.Read() // Advance through the first sequence
	return fr
}

// Read advances the reader to the next sequence, returning false if
// the source stream is empty.
func (fr *FastaReader) Read() bool {

	if len(fr.seqraw) > 0 {
		return true
	}

	// Read to the next name
	for fr.scanner.Scan() {
		line := fr.scanner.Text()
		if strings.HasPrefix(line, ">") {
			fr.names = append(fr.names, line[1:])
			return true
		}
		fr.seqraw = append(fr.seqraw, line)
	}

	if len(fr.seqraw) > 0 {
		return true
	}

	return false
}

// Get returns the name and sequence for the current record in the
// fasta stream.  The sequence is returned as a slice of integers.
func (fr *FastaReader) Get() (string, []int) {

	seq := make([]int, 0)

	for _, sr := range fr.seqraw {
		for j := 0; j < len(sr); j++ {

			switch sr[j] {
			case uint8('A'):
				seq = append(seq, 0)
			case uint8('T'):
				seq = append(seq, 1)
			case uint8('G'):
				seq = append(seq, 2)
			case uint8('C'):
				seq = append(seq, 3)
			}
		}
	}

	na := fr.names[0]
	copy(fr.names, fr.names[1:])
	fr.names = fr.names[0 : len(fr.names)-1]
	fr.seqraw = fr.seqraw[:0]

	return na, seq
}

// GetRaw returns the name and sequence for the current record in the
// fasta stream.  The sequence is returned as a string.
func (fr *FastaReader) GetRaw() (string, string) {

	seq := strings.Join(fr.seqraw, "")

	na := fr.names[0]
	copy(fr.names, fr.names[1:])
	fr.names = fr.names[0 : len(fr.names)-1]
	fr.seqraw = fr.seqraw[:0]

	return na, seq
}
