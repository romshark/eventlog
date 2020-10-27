package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fasthttp/websocket"
	"github.com/romshark/eventlog/client"
	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/inmem"
	fhttpfront "github.com/romshark/eventlog/frontend/fasthttp"
	"github.com/romshark/eventlog/internal/hex"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func test(t *testing.T, f func(*testing.T, Setup)) {
	setupHTTP := func(t *testing.T) (s Setup) {
		s.DB = eventlog.New(inmem.New())
		t.Cleanup(func() {
			if err := s.DB.Close(); err != nil {
				panic(fmt.Errorf("closing eventlog: %s", err))
			}
		})

		inMemListener := fasthttputil.NewInmemoryListener()
		t.Cleanup(func() {
			require.NoError(t, inMemListener.Close())
		})

		server := &fasthttp.Server{
			Handler: fhttpfront.New(
				log.New(os.Stderr, "ERR", log.LstdFlags),
				s.DB,
			).Serve,
		}

		go func() {
			require.NoError(t, server.Serve(inMemListener))
		}()

		s.Client = client.New(
			client.NewHTTP(
				"localhost",
				log.New(os.Stderr, "ERR", log.LstdFlags),
				&fasthttp.Client{
					Dial: func(addr string) (net.Conn, error) {
						return inMemListener.Dial()
					},
				},
				&websocket.Dialer{
					NetDialContext: func(
						ctx context.Context,
						network string,
						addr string,
					) (net.Conn, error) {
						return inMemListener.Dial()
					},
				},
			),
		)
		return
	}

	setupInmem := func(t *testing.T) (s Setup) {
		s.DB = eventlog.New(inmem.New())
		t.Cleanup(func() {
			if err := s.DB.Close(); err != nil {
				panic(fmt.Errorf("closing eventlog: %s", err))
			}
		})

		s.Client = client.New(client.NewInmem(s.DB))
		return
	}

	t.Run("HTTP", func(t *testing.T) { f(t, setupHTTP(t)) })
	t.Run("Inmem", func(t *testing.T) { f(t, setupInmem(t)) })
}

func TestAppend(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)
		first := s.DB.FirstOffset()

		// Append 1
		offset1, newVersion1, tm, err := s.Client.Append(
			context.Background(),
			makeEvent(t, "foo", Doc{"foo": "bar"}),
		)
		r.NoError(err)
		r.Equal(first, fromHex(t, offset1))
		r.Greater(fromHex(t, newVersion1), first)
		r.WithinDuration(time.Now(), tm, time.Second)

		next, err := scanExpect(t, s.DB, first, 10,
			ExpectedEvent{Label: "foo", Payload: Doc{"foo": "bar"}},
		)
		r.NoError(err)
		r.Equal(fromHex(t, newVersion1), next)

		// Append multiple
		offset2, newVersion2, tm2, err := s.Client.Append(
			context.Background(),
			makeEvent(t, "baz", Doc{"baz": "faz"}),
			makeEvent(t, "maz", Doc{"maz": "taz"}),
		)
		r.NoError(err)
		r.Equal(fromHex(t, newVersion1), fromHex(t, offset2))
		r.Greater(fromHex(t, newVersion2), fromHex(t, offset2))
		r.WithinDuration(time.Now(), tm2, time.Second)

		next, err = scanExpect(t, s.DB, first, 10,
			ExpectedEvent{Label: "foo", Payload: Doc{"foo": "bar"}},
			ExpectedEvent{Label: "baz", Payload: Doc{"baz": "faz"}},
			ExpectedEvent{Label: "maz", Payload: Doc{"maz": "taz"}},
		)
		r.NoError(err)
		r.Equal(fromHex(t, newVersion2), next)
	})
}

func TestAppendErrNoEvents(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		iv, err := s.Client.Version(context.Background())
		r.NoError(err)

		of, vr, tm, err := s.Client.Append(context.Background())
		r.NoError(err)
		r.Zero(of)
		r.Zero(vr)
		r.Zero(tm)

		av, err := s.Client.Version(context.Background())
		r.NoError(err)
		r.Equal(iv, av)
	})
}

func TestAppendCheck(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		first := s.DB.FirstOffset()

		// Try mismatching version
		offset1, newVersion1, tm, err := s.Client.AppendCheck(
			context.Background(),
			"1",
			makeEvent(t, "foo", Doc{"foo": "bar"}),
			makeEvent(t, "baz", Doc{"baz": "faz"}),
		)
		r.Error(err)
		r.True(
			errors.Is(err, client.ErrMismatchingVersions),
			"unexpected error: (%T) %s", err, err.Error(),
		)
		r.Zero(offset1)
		r.Zero(newVersion1)
		r.Zero(tm)

		// Try matching version
		offset1, newVersion1, tm, err = s.Client.AppendCheck(
			context.Background(),
			"0",
			makeEvent(t, "foo", Doc{"foo": "bar"}),
			makeEvent(t, "baz", Doc{"baz": "faz"}),
		)
		r.NoError(err)
		r.Equal(first, fromHex(t, offset1))
		r.Greater(fromHex(t, newVersion1), fromHex(t, offset1))
		r.WithinDuration(time.Now(), tm, time.Second)

		next, err := scanExpect(
			t, s.DB, first, 10,
			ExpectedEvent{Label: "foo", Payload: Doc{"foo": "bar"}},
			ExpectedEvent{Label: "baz", Payload: Doc{"baz": "faz"}},
		)
		r.NoError(err)
		r.Equal(fromHex(t, newVersion1), next)

		offset2, newVersion2, tm2, err := s.Client.AppendCheck(
			context.Background(),
			newVersion1,
			makeEvent(t, "taz", Doc{"taz": "maz"}),
			makeEvent(t, "kaz", Doc{"kaz": "jaz"}),
		)
		r.NoError(err)
		r.Equal(fromHex(t, newVersion1), fromHex(t, offset2))
		r.Greater(fromHex(t, newVersion2), fromHex(t, offset2))
		r.WithinDuration(time.Now(), tm2, time.Second)

		next, err = scanExpect(t, s.DB, first, 10,
			ExpectedEvent{Label: "foo", Payload: Doc{"foo": "bar"}},
			ExpectedEvent{Label: "baz", Payload: Doc{"baz": "faz"}},
			ExpectedEvent{Label: "taz", Payload: Doc{"taz": "maz"}},
			ExpectedEvent{Label: "kaz", Payload: Doc{"kaz": "jaz"}},
		)
		r.NoError(err)
		r.Equal(fromHex(t, newVersion2), next)
	})
}

func TestAppendCheckNoEvents(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		iv, err := s.Client.Version(context.Background())
		r.NoError(err)

		of, vr, tm, err := s.Client.AppendCheck(context.Background(), iv)
		r.NoError(err)
		r.Zero(of)
		r.Zero(vr)
		r.Zero(tm)

		av, err := s.Client.Version(context.Background())
		r.NoError(err)
		r.Equal(iv, av)
	})
}

func TestAppendCheckErrNoAssumedVersion(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		iv, err := s.Client.Version(context.Background())
		r.NoError(err)

		of, vr, tm, err := s.Client.AppendCheck(
			context.Background(),
			"",
			makeEvent(t, "foo", Doc{"foo": "bar"}),
		)
		r.Error(err)
		r.True(
			errors.Is(err, client.ErrInvalidVersion),
			"unexpected error: (%T) %s", err, err.Error(),
		)
		r.Zero(of)
		r.Zero(vr)
		r.Zero(tm)

		av, err := s.Client.Version(context.Background())
		r.NoError(err)
		r.Equal(iv, av)
	})
}

func TestRead(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		offsets := make([]uint64, 3)
		times := make([]time.Time, len(offsets))

		for i := range offsets {
			var err error
			offsets[i], _, times[i], err = s.DB.Append(
				makeEvent(t, "", Doc{"index": i}),
			)
			r.NoError(err)
		}

		// Read all
		e, err := scanClient(
			t,
			context.Background(),
			s.Client,
			"0",
			uint(len(offsets)),
		)
		r.NoError(err)
		r.Len(e, len(offsets))
		for i, e := range e {
			r.Equal(offsets[i], fromHex(t, e.Offset))
			r.Equal(times[i].Unix(), e.Time.Unix())
			r.Equal("", string(e.Label))
			r.Equal(Doc{"index": float64(i)}, e.Payload)
		}

		// Read first
		ev, err := s.Client.Read(context.Background(), "0")
		expectedEvent := makeEvent(t, "", Doc{"index": float64(0)})
		r.NoError(err)
		r.Equal(offsets[0], fromHex(t, ev.Offset))
		r.Equal(times[0].Unix(), ev.Time.Unix())
		r.Equal(expectedEvent.Label, string(ev.Label))
		r.Equal(expectedEvent.PayloadJSON, ev.Payload)

		// Read last 2
		e, err = scanClient(
			t,
			context.Background(),
			s.Client,
			fmt.Sprintf("%x", offsets[1]),
			2,
		)
		r.NoError(err)
		r.Len(e, 2)

		r.Equal(offsets[1], fromHex(t, e[0].Offset))
		r.Equal(times[1].Unix(), e[0].Time.Unix())
		r.Equal("", string(e[0].Label))
		r.Equal(Doc{"index": float64(1)}, e[0].Payload)

		r.Equal(offsets[2], fromHex(t, e[1].Offset))
		r.Equal(times[2].Unix(), e[1].Time.Unix())
		r.Equal("", string(e[1].Label))
		r.Equal(Doc{"index": float64(2)}, e[1].Payload)

		// Read at latest version
		v, err := s.Client.Version(context.Background())
		r.NoError(err)
		e, err = scanClient(t, context.Background(), s.Client, v, uint(10))
		r.Error(err)
		r.True(
			errors.Is(err, client.ErrOffsetOutOfBound),
			"unexpected error: (%T) %s", err, err.Error(),
		)
		r.Len(e, 0)
	})
}

func TestAppendInvalid(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		for _, t1 := range []struct {
			name  string
			input string
		}{
			{"empty", ``},
			{"syntax error", `{foo:"bar"}`},
			{"empty object", `{}`},
			{"array of values", `["bar", "foo", 42]`},
		} {
			t.Run(t1.name, func(t *testing.T) {
				r := require.New(t)

				_, _, _, err := s.Client.Append(
					context.Background(),
					eventlog.Event{PayloadJSON: []byte(t1.input)},
				)
				r.Error(err)
				r.True(
					errors.Is(err, client.ErrInvalidPayload),
					"unexpected error: (%T) %s", err, err.Error(),
				)

				v, err := s.Client.Version(context.Background())
				r.NoError(err)
				r.Zero(fromHex(t, v))
			})
		}
	})
}

func TestVersion(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		nextExpected := "0"
		for i := 0; i < 3; i++ {
			v1, err := s.Client.Version(context.Background())
			r.NoError(err)
			r.Equal(nextExpected, v1)

			_, newVersion, _, err := s.Client.AppendCheck(
				context.Background(),
				v1,
				makeEvent(t, "", Doc{"index": i}),
			)
			r.NoError(err)
			nextExpected = newVersion

			v2, err := s.Client.Version(context.Background())
			r.NoError(err)
			r.Equal(newVersion, v2)

			r.NoError(err)
		}
	})
}

func TestBegin(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		vBegin, err := s.Client.Begin(context.Background())
		r.NoError(err)

		r.Equal("0", vBegin)

		_, _, _, err = s.Client.AppendCheck(
			context.Background(),
			vBegin,
			makeEvent(t, "", Doc{"foo": "bar"}),
		)
		r.NoError(err)

		vBegin2, err := s.Client.Begin(context.Background())
		r.NoError(err)
		r.Equal(vBegin, vBegin2)
	})
}

func TestListen(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		versionChan1 := make(chan string, 1)
		go func() {
			if err := s.Client.Listen(context.Background(), func(v []byte) {
				versionChan1 <- string(v)
			}); err != nil {
				panic(err)
			}
		}()

		versionChan2 := make(chan string, 1)
		go func() {
			if err := s.Client.Listen(context.Background(), func(v []byte) {
				versionChan2 <- string(v)
			}); err != nil {
				panic(err)
			}
		}()

		time.Sleep(100 * time.Millisecond)
		_, newVersion, _, err := s.Client.Append(
			context.Background(),
			makeEvent(t, "", Doc{"foo": "bar"}),
		)
		r.NoError(err)

		r.Equal(newVersion, <-versionChan1)
		r.Equal(newVersion, <-versionChan2)
	})
}

func TestListenCancel(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		errChan := make(chan error, 1)
		versionTriggered := uint32(0)

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			errChan <- s.Client.Listen(ctx, func(v []byte) {
				atomic.AddUint32(&versionTriggered, 1)
			})
		}()

		cancel()
		r.Equal(context.Canceled, <-errChan)
		r.Zero(atomic.LoadUint32(&versionTriggered))
	})
}

func TestTryAppend(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		assumed, err := s.Client.Begin(context.Background())
		r.NoError(err)

		// Append
		_, v1, _, err := s.Client.Append(
			context.Background(),
			makeEvent(t, "first", Doc{"first": "1"}),
		)
		r.NoError(err)

		_, v2, _, err := s.Client.Append(
			context.Background(),
			makeEvent(t, "second", Doc{"second": "2"}),
		)
		r.NoError(err)

		_, v3, _, err := s.Client.Append(
			context.Background(),
			makeEvent(t, "third", Doc{"third": "3"}),
		)
		r.NoError(err)

		syncCalled := uint32(0)
		transactionCalled := uint32(0)

		offset, newVersion, tm, err := s.Client.TryAppend(
			context.Background(),
			assumed,
			// Transaction
			func() (events []eventlog.Event, err error) {
				atomic.AddUint32(&transactionCalled, 1)
				return []eventlog.Event{
					makeEvent(t, "fourth", Doc{"fourth": "4"}),
					makeEvent(t, "fifth", Doc{"fifth": "5"}),
				}, nil
			},
			// Sync
			func() (string, error) {
				switch atomic.AddUint32(&syncCalled, 1) {
				case 1:
					return v1, nil
				case 2:
					return v2, nil
				case 3:
					return v3, nil
				}
				return "", nil
			},
		)
		r.NoError(err)
		r.Equal(v3, offset)
		r.Greater(fromHex(t, newVersion), fromHex(t, v3))
		r.WithinDuration(time.Now(), tm, time.Second)

		r.Equal(uint32(3), atomic.LoadUint32(&syncCalled))
		r.Equal(uint32(4), atomic.LoadUint32(&transactionCalled))
	})
}

func TestTryAppendTransactionErr(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		assumed, err := s.Client.Begin(context.Background())
		r.NoError(err)

		syncCalled := uint32(0)
		transactionCalled := uint32(0)

		errTransactionFail := fmt.Errorf("transaction failure")

		offset, newVersion, tm, err := s.Client.TryAppend(
			context.Background(),
			assumed,
			// Transaction
			func() (events []eventlog.Event, err error) {
				atomic.AddUint32(&transactionCalled, 1)
				return nil, errTransactionFail
			},
			// Sync
			func() (string, error) {
				atomic.AddUint32(&syncCalled, 1)
				return "1", nil
			},
		)
		r.Error(err)
		r.Equal(errTransactionFail, err)
		r.Zero(offset)
		r.Zero(newVersion)
		r.Zero(tm)

		r.Equal(uint32(0), atomic.LoadUint32(&syncCalled))
		r.Equal(uint32(1), atomic.LoadUint32(&transactionCalled))
	})
}

func TestTryAppendSyncErr(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		r := require.New(t)

		assumed, err := s.Client.Begin(context.Background())
		r.NoError(err)

		_, _, _, err = s.DB.Append(makeEvent(t, "first", Doc{"first": 1}))
		r.NoError(err)

		syncCalled := uint32(0)
		transactionCalled := uint32(0)

		errSyncFail := fmt.Errorf("transaction failure")

		offset, newVersion, tm, err := s.Client.TryAppend(
			context.Background(),
			assumed,
			// Transaction
			func() (events []eventlog.Event, err error) {
				atomic.AddUint32(&transactionCalled, 1)
				return []eventlog.Event{makeEvent(t, "", Doc{"foo": 42})}, nil
			},
			// Sync
			func() (string, error) {
				atomic.AddUint32(&syncCalled, 1)
				return "", errSyncFail
			},
		)
		r.Error(err)
		r.Equal(errSyncFail, err)
		r.Zero(offset)
		r.Zero(newVersion)
		r.Zero(tm)

		r.Equal(uint32(1), atomic.LoadUint32(&syncCalled))
		r.Equal(uint32(1), atomic.LoadUint32(&transactionCalled))
	})
}

type ExpectedEvent struct {
	Label   string
	Payload Doc
}

func scanExpect(
	t *testing.T,
	l *eventlog.EventLog,
	offset,
	limit uint64,
	expected ...ExpectedEvent,
) (uint64, error) {
	actual := make([]ExpectedEvent, 0, len(expected))
	nextOffset, err := l.Scan(offset, limit, func(
		offset uint64,
		timestamp uint64,
		label []byte,
		payload []byte,
	) error {
		var data Doc
		if err := json.Unmarshal(payload, &data); err != nil {
			return fmt.Errorf("unexpected error: %w", err)
		}
		actual = append(actual, ExpectedEvent{
			Label:   string(label),
			Payload: data,
		})
		return nil
	})
	if err != nil {
		return 0, err
	}

	require.Equal(t, expected, actual)
	return nextOffset, nil
}

func scanClient(
	t *testing.T,
	ctx context.Context,
	c *client.Client,
	offset string,
	limit uint,
) ([]Event, error) {
	events := make([]Event, 0, limit)
	err := c.Scan(
		ctx,
		offset,
		limit,
		func(
			offset string,
			timestamp time.Time,
			label []byte,
			payload []byte,
			next string,
		) error {
			ev := Event{Event: client.Event{
				Offset:  offset,
				Time:    timestamp,
				Payload: payload,
				Next:    next,
			}}
			if len(label) > 0 {
				ev.Label = make([]byte, len(label))
				copy(ev.Label, label)
			}

			if err := json.Unmarshal(payload, &ev.Payload); err != nil {
				return fmt.Errorf("unexpected error: %w", err)
			}
			events = append(events, ev)
			return nil
		},
	)
	return events, err
}

type Event struct {
	client.Event
	Payload Doc
}

func fromHex(t *testing.T, s string) uint64 {
	i, err := hex.ReadUint64([]byte(s))
	require.NoError(t, err)
	return i
}

func makeEvent(t *testing.T, label string, d Doc) eventlog.Event {
	b, err := json.Marshal(d)
	require.NoError(t, err)
	return eventlog.Event{
		Label:       label,
		PayloadJSON: b,
	}
}

type Doc map[string]interface{}

type Setup struct {
	DB     *eventlog.EventLog
	Client *client.Client
}
