package main_test

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/romshark/eventlog/client"
	evfile "github.com/romshark/eventlog/eventlog/file"
	ffhttp "github.com/romshark/eventlog/frontend/fasthttp"

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

	server := ffhttp.New(l)
	httpServer := &fasthttp.Server{
		Handler:     server.Serve,
		ReadTimeout: 10 * time.Millisecond,
	}

	go func() {
		if err := httpServer.Serve(ln); err != nil {
			panic(err)
		}
	}()

	teardown = func() {
		if err := httpServer.Shutdown(); err != nil {
			panic(err)
		}
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
		_, _, _, err := clt.AppendBytes(payload)
		panicOnErr(err)
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

	_, newVersion, _, err := clt.AppendBytes(payload)
	panicOnErr(err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, newVersion, _, err = clt.AppendCheckBytes(newVersion, payload)
		panicOnErr(err)
	}
}

func BenchmarkFileHTTP_Read_1K(b *testing.B) {
	clt, teardown := newBenchmarkSetup(b)
	defer teardown()

	const numEvents = 1000

	var offset string

	for i := 0; i < numEvents; i++ {
		o, _, _, err := clt.AppendBytes([]byte(`{
			"example": "benchmark",
			"foo": null,
			"bar": 52.7775,
			"baz": false,
			"fazz": "4ff21935-b005-4bd3-936e-10d4692a8843"
		}`))
		panicOnErr(err)
		if i < 1 {
			offset = o
		}
	}

	for i := 0; i < b.N; i++ {
		events, err := clt.Read(offset, 0)
		if len(events) != numEvents {
			panic(fmt.Errorf("unexpected number of events: %d", len(events)))
		}
		panicOnErr(err)
	}
}
