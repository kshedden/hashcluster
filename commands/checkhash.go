package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"path"

	"github.com/kshedden/szutils"
)

var (
	numhash int = 100

	n_clust int = 1000

	dpath string = "/nfs/kshedden/Teal_Furnholm/simhashes"
)

func main() {

	var nc, nt int
	for k := 0; k < numhash; k++ {

		fname := fmt.Sprintf("%02d.bin.sz", k)
		fname = path.Join(dpath, fname)
		rdr := szutils.NewFileReader(fname)

		var rk []int
		for jj := 0; ; jj++ {
			var x uint32
			err := binary.Read(rdr, binary.LittleEndian, &x)
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			rk = append(rk, int(x))
		}

		for jj := 0; jj < len(rk); jj++ {
			b := rk[jj] % n_clust
			for j := -5; j < 5; j++ {
				if (jj+j >= 0) && (jj+j < len(rk)) {
					if (rk[jj+j] % n_clust) == b {
						nc++
					}
					nt++
				}
			}
		}
	}

	fmt.Printf("%d %d\n", nc, nt)
	fmt.Printf("%f\n", float64(nc)/float64(nt))
}
