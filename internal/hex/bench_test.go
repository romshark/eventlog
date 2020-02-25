package hex_test

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/romshark/eventlog/internal/hex"
	"github.com/valyala/fasthttp"
)

func fmtFprintf(r *fasthttp.RequestCtx, i uint64) (int, error) {
	return fmt.Fprintf(r, "%x", i)
}

func BenchmarkWriteUint64(b *testing.B) {
	for _, b1 := range []uint64{
		0, 1, 2, 3, 11, 999, 1024,
		math.MaxUint64 / 3,
		math.MaxUint64,
	} {
		for _, b2 := range []struct {
			fnName string
			fn     func(*fasthttp.RequestCtx, uint64) (int, error)
		}{
			{"fmt.Fprintf", fmtFprintf},
			{"hex.WriteUint64", hex.WriteUint64},
		} {
			b.Run(fmt.Sprintf("%d_%s", b1, b2.fnName), func(b *testing.B) {
				req := new(fasthttp.RequestCtx)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := b2.fn(req, uint64(i)); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func strconvParseUint(s []byte) (uint64, error) {
	return strconv.ParseUint(string(s), 10, 64)
}

func BenchmarkReadUint64(b *testing.B) {
	for _, b1 := range []string{
		"0", "1", "2", "3", "11", "999", "1024",
		"18446744073709551615",
		"61489146919",
		"000000000000000",
	} {
		for _, b2 := range []struct {
			fnName string
			fn     func(s []byte) (uint64, error)
		}{
			{"fmt.Fprintf", strconvParseUint},
			{"hex.ReadUint64", hex.ReadUint64},
		} {
			b.Run(fmt.Sprintf("%s_%s", b1, b2.fnName), func(b *testing.B) {
				in := []byte(b1)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := b2.fn(in); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}
