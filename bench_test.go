package main_test

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	apihttp "github.com/romshark/eventlog/api/http"
	"github.com/romshark/eventlog/client"
	evfile "github.com/romshark/eventlog/eventlog/file"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func newBenchmarkSetup(b *testing.B) (clt client.Client, teardown func()) {
	fileName := fmt.Sprintf(
		"benchmark_%s_%s",
		b.Name(),
		time.Now().Format(time.RFC3339Nano),
	)

	l, err := evfile.NewFile(fileName)
	panicOnErr(err)

	ln := fasthttputil.NewInmemoryListener()

	api := apihttp.NewAPIHTTP(l)
	go func() {
		if err := api.Serve(ln); err != nil {
			panic(err)
		}
	}()

	teardown = func() {
		ln.Close()
		os.Remove(fileName)
	}

	clt = client.NewHTTP(
		&fasthttp.Client{
			Dial: func(addr string) (net.Conn, error) {
				return ln.Dial()
			},
		},
		"test",
	)

	return
}

func BenchmarkFileHTTP_Append_P128(b *testing.B) {
	clt, teardown := newBenchmarkSetup(b)
	defer teardown()

	payload := []byte(`{
		"example": "benchmark",
		"foo": null,
		"bar": 52.7775,
		"baz": false,
		"fazz": "4ff21935-b005-4bd3-936e-10d4692a8843"
	}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		panicOnErr(clt.AppendBytes(payload))
	}
}

func BenchmarkFileHTTP_AppendCheck_P128(b *testing.B) {
	clt, teardown := newBenchmarkSetup(b)
	defer teardown()

	payload := []byte(`{
		"example": "benchmark",
		"foo": null,
		"bar": 52.7775,
		"baz": false,
		"fazz": "4ff21935-b005-4bd3-936e-10d4692a8843"
	}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		panicOnErr(clt.AppendCheckBytes(uint64(i), payload))
	}
}
