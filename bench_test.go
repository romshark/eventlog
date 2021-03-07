package main_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/romshark/eventlog/client"
	"github.com/romshark/eventlog/eventlog"
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

func newBenchmarkSetup(b *testing.B) *client.Client {
	filePath := fmt.Sprintf(
		"%s/benchmark_%s_%s",
		b.TempDir(),
		b.Name(),
		time.Now().Format(time.RFC3339Nano),
	)

	meta := map[string]string{
		"foo":  "bar",
		"bazz": "42",
	}
	panicOnErr(evfile.Create(filePath, meta, 0777))

	l, err := evfile.Open(filePath)
	panicOnErr(err)

	ln := fasthttputil.NewInmemoryListener()

	server := ffhttp.New(nil, eventlog.New(l))
	httpServer := &fasthttp.Server{
		Handler:     server.Serve,
		ReadTimeout: 10 * time.Millisecond,
	}

	go func() {
		if err := httpServer.Serve(ln); err != nil {
			panic(err)
		}
	}()

	b.Cleanup(func() {
		if err := httpServer.Shutdown(); err != nil {
			panic(err)
		}
	})

	return client.New(
		client.NewHTTP(
			"test",
			log.New(os.Stderr, "ERR", log.LstdFlags),
			&fasthttp.Client{
				Dial: func(addr string) (net.Conn, error) {
					return ln.Dial()
				},
			},
			nil,
		),
	)
}

func BenchmarkFileHTTP_Append_P128(b *testing.B) {
	clt := newBenchmarkSetup(b)

	label := "BenchmarkEvent"
	payload := []byte(`{
		"example": "benchmark",
		"foo": null,
		"bar": 52.7775,
		"baz": false,
		"fazz": "4ff21935-b005-4bd3-936e-10d4692a8843"
	}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := clt.Append(
			context.Background(),
			eventlog.Event{
				Label:       label,
				PayloadJSON: payload,
			},
		)
		panicOnErr(err)
	}
}

func BenchmarkFileHTTP_AppendCheck_P128(b *testing.B) {
	clt := newBenchmarkSetup(b)

	label := "BenchmarkEvent"
	payload := []byte(`{
		"example": "benchmark",
		"foo": null,
		"bar": 52.7775,
		"baz": false,
		"fazz": "4ff21935-b005-4bd3-936e-10d4692a8843"
	}`)

	_, newVersion, _, err := clt.Append(
		context.Background(),
		eventlog.Event{
			Label:       label,
			PayloadJSON: payload,
		},
	)
	panicOnErr(err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, newVersion, _, err = clt.AppendCheck(
			context.Background(),
			newVersion,
			eventlog.Event{
				Label:       label,
				PayloadJSON: payload,
			},
		)
		panicOnErr(err)
	}
}

func BenchmarkFileHTTP_Read_1K(b *testing.B) {
	clt := newBenchmarkSetup(b)

	const numEvents = uint(1000)

	var offset string

	label := "BenchmarkEvent"
	payload := []byte(`{
		"example": "benchmark",
		"foo": null,
		"bar": 52.7775,
		"baz": false,
		"fazz": "4ff21935-b005-4bd3-936e-10d4692a8843"
	}`)

	for i := uint(0); i < numEvents; i++ {
		o, _, _, err := clt.Append(context.Background(), eventlog.Event{
			Label:       label,
			PayloadJSON: payload,
		})
		panicOnErr(err)
		if i < 1 {
			offset = o
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		counter := uint(0)
		panicOnErr(clt.Scan(
			context.Background(),
			offset,
			numEvents,
			func(
				offset string,
				timestamp time.Time,
				label []byte,
				payload []byte,
				next string,
			) error {
				counter++
				return nil
			},
		))
		if counter != numEvents {
			panic(fmt.Errorf("unexpected number of events: %d", counter))
		}
	}
}
