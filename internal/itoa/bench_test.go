package itoa_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/romshark/eventlog/internal/itoa"
)

func std(w io.Writer, i uint32) error {
	_, err := fmt.Fprintf(w, "%d", i)
	return err
}

func BenchmarkItoa(b *testing.B) {
	for _, bb := range []struct {
		fnName string
		fn     func(io.Writer, uint32) error
	}{
		{"fmt.Fprintf", std},
		{"itoa.U32toa", itoa.U32toa},
	} {
		b.Run(bb.fnName, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if err := bb.fn(ioutil.Discard, uint32(i)); err != nil {
					panic(err)
				}
			}
		})
	}
}
