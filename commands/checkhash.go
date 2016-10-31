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

	cnt map[int][]int
)

func main() {

	cnt = make(map[int][]int)

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
			for j := -10; j < 10; j++ {
				if j == 0 {
					continue
				}

				if (jj+j >= 0) && (jj+j < len(rk)) {
					if (rk[jj+j] % n_clust) == b {
						nc++
					}
					nt++
					cnt[rk[jj]] = append(cnt[rk[jj]], rk[jj+j])
				}
			}
		}
	}

	fmt.Printf("%d %d\n", nc, nt)
	fmt.Printf("%f\n", float64(nc)/float64(nt))

	nc = 0
	nw := 0
	for k, v := range cnt {

		b := k % n_clust

		mp := make(map[int]int)
		for _, u := range v {
			mp[u] = mp[u] + 1
		}

		for u, c := range mp {
			if c > 5 {
				if (u % n_clust) == b {
					nc++
				} else {
					nw++
				}
			}
		}
	}
	fmt.Printf("%d %d\n", nc, nc+nw)
	fmt.Printf("%f\n", float64(nc)/float64(nc+nw))
}
