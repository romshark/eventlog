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

func TestAppend(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	first := s.DB.FirstOffset()

	offset1, newVersion1, tm, err := s.Client.Append(
		context.Background(),
		Doc{"foo": "bar"},
	)
	r.NoError(err)
	r.Equal(first, fromHex(t, offset1))
	r.Greater(fromHex(t, newVersion1), first)
	r.WithinDuration(time.Now(), tm, time.Second)

	next, err := scanExpect(t, s.DB, first, 10, Doc{"foo": "bar"})
	r.NoError(err)
	r.Equal(fromHex(t, newVersion1), next)

	offset2, newVersion2, tm2, err := s.Client.Append(
		context.Background(),
		Doc{"baz": "faz"},
		Doc{"maz": "taz"},
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion1), fromHex(t, offset2))
	r.Greater(fromHex(t, newVersion2), fromHex(t, offset2))
	r.WithinDuration(time.Now(), tm2, time.Second)

	next, err = scanExpect(t, s.DB, first, 10,
		Doc{"foo": "bar"},
		Doc{"baz": "faz"},
		Doc{"maz": "taz"},
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion2), next)
}

func TestAppendErrInvalid(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	iv, err := s.Client.Version(context.Background())
	r.NoError(err)

	of, vr, tm, err := s.Client.Append(context.Background())
	r.Error(err)
	r.True(errors.Is(err, client.ErrInvalidPayload))
	r.Zero(of)
	r.Zero(vr)
	r.Zero(tm)

	av, err := s.Client.Version(context.Background())
	r.NoError(err)
	r.Equal(iv, av)
}

func TestAppendJSON(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	first := s.DB.FirstOffset()

	// Append 1
	offset1, newVersion1, tm, err := s.Client.AppendJSON(
		context.Background(),
		toJson(t, Doc{"foo": "bar"}),
	)
	r.NoError(err)
	r.Equal(first, fromHex(t, offset1))
	r.Greater(fromHex(t, newVersion1), first)
	r.WithinDuration(time.Now(), tm, time.Second)

	next, err := scanExpect(t, s.DB, first, 10, Doc{"foo": "bar"})
	r.NoError(err)
	r.Equal(fromHex(t, newVersion1), next)

	// Append multiple
	offset2, newVersion2, tm2, err := s.Client.AppendJSON(
		context.Background(),
		toJsonArray(t, Doc{"baz": "faz"}, Doc{"maz": "taz"}),
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion1), fromHex(t, offset2))
	r.Greater(fromHex(t, newVersion2), fromHex(t, offset2))
	r.WithinDuration(time.Now(), tm2, time.Second)

	next, err = scanExpect(t, s.DB, first, 10,
		Doc{"foo": "bar"},
		Doc{"baz": "faz"},
		Doc{"maz": "taz"},
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion2), next)
}

func TestAppendCheck(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	first := s.DB.FirstOffset()

	// Try mismatching version
	offset1, newVersion1, tm, err := s.Client.AppendCheck(
		context.Background(),
		"1",
		Doc{"foo": "bar"},
		Doc{"baz": "faz"},
	)
	r.Error(err)
	r.True(errors.Is(err, client.ErrMismatchingVersions))
	r.Zero(offset1)
	r.Zero(newVersion1)
	r.Zero(tm)

	// Try matching version
	offset1, newVersion1, tm, err = s.Client.AppendCheck(
		context.Background(),
		"0",
		Doc{"foo": "bar"},
		Doc{"baz": "faz"},
	)
	r.NoError(err)
	r.Equal(first, fromHex(t, offset1))
	r.Greater(fromHex(t, newVersion1), fromHex(t, offset1))
	r.WithinDuration(time.Now(), tm, time.Second)

	next, err := scanExpect(
		t, s.DB, first, 10,
		Doc{"foo": "bar"},
		Doc{"baz": "faz"},
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion1), next)

	offset2, newVersion2, tm2, err := s.Client.AppendCheck(
		context.Background(),
		newVersion1,
		Doc{"taz": "maz"},
		Doc{"kaz": "jaz"},
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion1), fromHex(t, offset2))
	r.Greater(fromHex(t, newVersion2), fromHex(t, offset2))
	r.WithinDuration(time.Now(), tm2, time.Second)

	next, err = scanExpect(t, s.DB, first, 10,
		Doc{"foo": "bar"},
		Doc{"baz": "faz"},
		Doc{"taz": "maz"},
		Doc{"kaz": "jaz"},
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion2), next)
}

func TestAppendCheckErrInvalid(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	iv, err := s.Client.Version(context.Background())
	r.NoError(err)

	of, vr, tm, err := s.Client.AppendCheck(context.Background(), iv)
	r.Error(err)
	r.True(errors.Is(err, client.ErrInvalidPayload))
	r.Zero(of)
	r.Zero(vr)
	r.Zero(tm)

	av, err := s.Client.Version(context.Background())
	r.NoError(err)
	r.Equal(iv, av)
}

func TestAppendCheckErrNoAssumedVersion(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	iv, err := s.Client.Version(context.Background())
	r.NoError(err)

	of, vr, tm, err := s.Client.AppendCheck(
		context.Background(),
		"",
		Doc{"foo": "bar"},
	)
	r.Error(err)
	r.Equal("no assumed version", err.Error())
	r.Zero(of)
	r.Zero(vr)
	r.Zero(tm)

	av, err := s.Client.Version(context.Background())
	r.NoError(err)
	r.Equal(iv, av)
}

func TestAppendCheckJSON(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	first := s.DB.FirstOffset()

	// Try mismatching version
	offset1, newVersion1, tm, err := s.Client.AppendCheckJSON(
		context.Background(),
		"1",
		toJsonArray(t, Doc{"foo": "bar"}, Doc{"baz": "faz"}),
	)
	r.Error(err)
	r.True(errors.Is(err, client.ErrMismatchingVersions))
	r.Zero(offset1)
	r.Zero(newVersion1)
	r.Zero(tm)

	// Try matching version
	offset1, newVersion1, tm, err = s.Client.AppendCheckJSON(
		context.Background(),
		"0",
		toJsonArray(t, Doc{"foo": "bar"}, Doc{"baz": "faz"}),
	)
	r.NoError(err)
	r.Equal(first, fromHex(t, offset1))
	r.Greater(fromHex(t, newVersion1), fromHex(t, offset1))
	r.WithinDuration(time.Now(), tm, time.Second)

	next, err := scanExpect(
		t, s.DB, first, 10,
		Doc{"foo": "bar"},
		Doc{"baz": "faz"},
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion1), next)

	offset2, newVersion2, tm2, err := s.Client.AppendCheckJSON(
		context.Background(),
		newVersion1,
		toJsonArray(t, Doc{"taz": "maz"}, Doc{"kaz": "jaz"}),
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion1), fromHex(t, offset2))
	r.Greater(fromHex(t, newVersion2), fromHex(t, offset2))
	r.WithinDuration(time.Now(), tm2, time.Second)

	next, err = scanExpect(t, s.DB, first, 10,
		Doc{"foo": "bar"},
		Doc{"baz": "faz"},
		Doc{"taz": "maz"},
		Doc{"kaz": "jaz"},
	)
	r.NoError(err)
	r.Equal(fromHex(t, newVersion2), next)
}

func TestRead(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	offsets := make([]uint64, 3)
	times := make([]time.Time, len(offsets))

	for i := range offsets {
		var err error
		offsets[i], _, times[i], err = s.DB.Append(
			toJson(t, Doc{"index": i}),
		)
		r.NoError(err)
	}

	// Read all
	e, err := s.Client.Read(
		context.Background(),
		"0", uint64(len(offsets)),
	)
	r.NoError(err)
	r.Len(e, len(offsets))
	for i, e := range e {
		r.Equal(offsets[i], fromHex(t, e.Offset))
		r.Equal(times[i].Unix(), e.Time.Unix())
		r.Equal(map[string]interface{}{"index": float64(i)}, e.Payload)
	}

	// Read first
	e, err = s.Client.Read(context.Background(), "0", 1)
	r.NoError(err)
	r.Len(e, 1)
	r.Equal(offsets[0], fromHex(t, e[0].Offset))
	r.Equal(times[0].Unix(), e[0].Time.Unix())
	r.Equal(map[string]interface{}{"index": float64(0)}, e[0].Payload)

	// Read last 2
	e, err = s.Client.Read(
		context.Background(),
		fmt.Sprintf("%x", offsets[1]),
		2,
	)
	r.NoError(err)
	r.Len(e, 2)

	r.Equal(offsets[1], fromHex(t, e[0].Offset))
	r.Equal(times[1].Unix(), e[0].Time.Unix())
	r.Equal(map[string]interface{}{"index": float64(1)}, e[0].Payload)

	r.Equal(offsets[2], fromHex(t, e[1].Offset))
	r.Equal(times[2].Unix(), e[1].Time.Unix())
	r.Equal(map[string]interface{}{"index": float64(2)}, e[1].Payload)

	// Read at latest version
	v, err := s.Client.Version(context.Background())
	r.NoError(err)
	e, err = s.Client.Read(context.Background(), v, 10)
	r.Error(err)
	r.True(errors.Is(err, client.ErrOffsetOutOfBound))
	r.Len(e, 0)
}

func TestAppendJSONInvalid(t *testing.T) {
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
			s := setup(t)
			r := require.New(t)

			_, _, _, err := s.Client.AppendJSON(
				context.Background(),
				[]byte(t1.input),
			)
			r.Error(err)
			r.True(errors.Is(err, client.ErrInvalidPayload))

			v, err := s.Client.Version(context.Background())
			r.NoError(err)
			r.Zero(fromHex(t, v))
		})
	}
}

func TestVersion(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	nextExpected := "0"
	for i := 0; i < 3; i++ {
		v1, err := s.Client.Version(context.Background())
		r.NoError(err)
		r.Equal(nextExpected, v1)

		_, newVersion, _, err := s.Client.AppendCheck(
			context.Background(),
			v1,
			Doc{"index": i},
		)
		r.NoError(err)
		nextExpected = newVersion

		v2, err := s.Client.Version(context.Background())
		r.NoError(err)
		r.Equal(newVersion, v2)

		r.NoError(err)
	}
}

func TestBegin(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	vBegin, err := s.Client.Begin(context.Background())
	r.NoError(err)

	r.Equal("0", vBegin)

	_, _, _, err = s.Client.AppendCheck(
		context.Background(),
		vBegin,
		Doc{"foo": "bar"},
	)
	r.NoError(err)

	vBegin2, err := s.Client.Begin(context.Background())
	r.NoError(err)
	r.Equal(vBegin, vBegin2)
}

func TestListen(t *testing.T) {
	s := setup(t)
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
		Doc{"foo": "bar"},
	)
	r.NoError(err)

	r.Equal(newVersion, <-versionChan1)
	r.Equal(newVersion, <-versionChan2)
}

func TestListenCancel(t *testing.T) {
	s := setup(t)
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
}

func setup(t *testing.T) (s struct {
	DB     *eventlog.EventLog
	Server *fasthttp.Server
	Client *client.Client
}) {
	i, err := inmem.NewInmem()
	require.NoError(t, err)

	s.DB = eventlog.New(i)
	t.Cleanup(func() {
		if err := s.DB.Close(); err != nil {
			panic(fmt.Errorf("closing eventlog: %s", err))
		}
	})

	inMemListener := fasthttputil.NewInmemoryListener()
	t.Cleanup(func() {
		require.NoError(t, inMemListener.Close())
	})

	s.Server = &fasthttp.Server{
		Handler: fhttpfront.New(
			log.New(os.Stderr, "ERR", log.LstdFlags),
			s.DB,
		).Serve,
	}

	go func() {
		require.NoError(t, s.Server.Serve(inMemListener))
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

func TestTryAppend(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	assumed, err := s.Client.Begin(context.Background())
	r.NoError(err)

	// Append
	_, v1, _, err := s.Client.Append(
		context.Background(),
		Doc{"first": "1"},
	)
	r.NoError(err)

	_, v2, _, err := s.Client.Append(
		context.Background(),
		Doc{"second": "2"},
	)
	r.NoError(err)

	_, v3, _, err := s.Client.Append(
		context.Background(),
		Doc{"third": "3"},
	)
	r.NoError(err)

	syncCalled := uint32(0)
	transactionCalled := uint32(0)

	offset, newVersion, tm, err := s.Client.TryAppend(
		context.Background(),
		assumed,
		// Transaction
		func() (events []map[string]interface{}, err error) {
			atomic.AddUint32(&transactionCalled, 1)
			return Docs{
				Doc{"fourth": "4"},
				Doc{"fifth": "5"},
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
}

func scanExpect(
	t *testing.T,
	l *eventlog.EventLog,
	offset,
	n uint64,
	expected ...Doc,
) (uint64, error) {
	actual := make(Docs, 0, len(expected))
	nextOffset, err := l.Scan(offset, n, func(
		timestamp uint64,
		payload []byte,
		offset uint64,
	) error {
		var data Doc
		if err := json.Unmarshal(payload, &data); err != nil {
			return fmt.Errorf("unexpected error: %w", err)
		}
		actual = append(actual, data)
		return nil
	})
	if err != nil {
		return 0, err
	}

	require.Equal(t, expected, actual)
	return nextOffset, nil
}

func fromHex(t *testing.T, s string) uint64 {
	i, err := hex.ReadUint64([]byte(s))
	require.NoError(t, err)
	return i
}

func toJsonArray(t *testing.T, d ...Doc) []byte {
	if l := len(d); l < 2 {
		t.Fatalf("too little documents (%d) to marshal", l)
	}
	b, err := json.Marshal(d)
	require.NoError(t, err)
	return b
}

func toJson(t *testing.T, d Doc) []byte {
	b, err := json.Marshal(d)
	require.NoError(t, err)
	return b
}

type Doc = map[string]interface{}
type Docs = []map[string]interface{}
