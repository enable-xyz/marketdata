package dataset

import (
	"fmt"
	"testing"

	"github.com/enable-xyz/marketdata/normalize"
)

func BenchmarkParquetRowGroups(b *testing.B) {
	row := testNormalizedRows(b)[0]
	for _, rowGroupBytes := range []int64{64 << 20, 256 << 20, 1024 << 20} {
		for _, dictionary := range []bool{false, true} {
			for _, bloom := range []bool{false, true} {
				name := fmt.Sprintf("rg_%dMiB/dictionary_%t/bloom_%t", rowGroupBytes>>20, dictionary, bloom)
				b.Run(name, func(b *testing.B) {
					for b.Loop() {
						options := DefaultWriterOptions(testHash("benchmark-policy"), testHash("benchmark-config"), testHash("benchmark-inputs"))
						options.RowGroupTargetBytes, options.Dictionary, options.BloomFilter = rowGroupBytes, dictionary, bloom
						rows := []normalize.Row{row}
						if _, err := BuildNormalizedPartition(b.Context(), b.TempDir(), &SliceNormalizedSource{Rows: rows}, options); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
