package main

import (
	"compress/gzip"
	"fmt"
	"math/rand"
	"os"
	"path"
)

var (
	n_clust  int     = 100
	n_seq    int     = 10000
	mut_rate float64 = 0.1
	length           = 500
	dpath            = "/nfs/kshedden/Teal_Furnholm"
)

func main() {

	clust_centers := make([][]rune, n_clust)
	for k := 0; k < n_clust; k++ {
		clust := make([]rune, length)
		for i := 0; i < length; i++ {
			clust[i] = alphabet[rand.Int()%4]
		}
		clust_centers[k] = clust
	}

	out, err := os.Create(path.Join(dpath, "test.fasta.gz"))
	if err != nil {
		panic(err)
	}
	defer out.Close()

	wrt := gzip.NewWriter(out)

	for j := 0; j < n_seq; j++ {
		parent := clust_centers[j/n_clust]
		wrt.Write([]byte(fmt.Sprintf(">%04d\n", j)))
		for i := 0; i < length; i++ {
			if rand.Float64() < mut_rate {
				jx := rand.Int() % 4
				wrt.Write([]byte{byte(alphabet[jx])})
			} else {
				wrt.Write([]byte{byte(parent[i])})
			}
		}
		wrt.Write([]byte("\n"))
	}
}
