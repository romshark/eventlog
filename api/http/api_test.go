package http_test

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	apihttp "github.com/romshark/eventlog/api/http"
	clt "github.com/romshark/eventlog/client"
	engineinmem "github.com/romshark/eventlog/eventlog/inmem"
	"github.com/romshark/eventlog/internal/consts"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

type (
	expected []map[string]interface{}
	Payload  map[string]interface{}
)

func check(
	t *testing.T,
	actual []clt.Event,
	expected expected,
) {
	require.Len(t, actual, len(expected))
	for i, payload := range expected {
		a := actual[i]

		require.WithinDuration(t, time.Now(), a.Time, 3*time.Second)

		if i > 0 {
			previousTime := actual[i-1].Time
			require.False(t, a.Time.Unix() < previousTime.Unix())
		}

		for k, v := range payload {
			require.Contains(t, a.Payload, k)
			require.Equal(t, v, a.Payload[k])
		}
	}
}

type Setup struct {
	HTTPAPI *apihttp.APIHTTP
	Client  clt.Client
}

func NewSetup(t *testing.T) (
	setup Setup,
	teardown func(),
) {
	l, err := engineinmem.NewInmem()
	require.NoError(t, err)

	ln := fasthttputil.NewInmemoryListener()

	setup.HTTPAPI = apihttp.NewAPIHTTP(l)
	require.NotNil(t, setup.HTTPAPI)

	go func() {
		if err := setup.HTTPAPI.Serve(ln); err != nil {
			panic(err)
		}
	}()

	teardown = func() {
		if err := ln.Close(); err != nil {
			panic(err)
		}
	}

	setup.Client = clt.NewHTTP(
		&fasthttp.Client{
			Dial: func(addr string) (net.Conn, error) {
				return ln.Dial()
			},
		},
		"test",
	)

	return
}

// TestAppendRead assumes regular reading and writing to succeed
func TestAppendRead(t *testing.T) {
	s, teardown := NewSetup(t)
	defer teardown()

	// Append first event
	require.NoError(t, s.Client.Append(Payload{"ix": 1}))

	// Append second event
	require.NoError(t, s.Client.Append(Payload{"ix": 2}))

	// Append third event
	require.NoError(t, s.Client.Append(Payload{"ix": 3}))

	// Read all events
	events, err := s.Client.Read(0, 0)
	require.NoError(t, err)

	check(t, events, expected{
		{"ix": float64(1)},
		{"ix": float64(2)},
		{"ix": float64(3)},
	})
}

// TestAppendReadUTF8 assumes regular reading and writing
// events with UTF-8 encoded payloads to succeed
func TestAppendReadUTF8(t *testing.T) {
	s, teardown := NewSetup(t)
	defer teardown()

	// Append first event
	require.NoError(t, s.Client.Append(Payload{
		"ключ":     "значение",
		"გასაღები": "მნიშვნელობა",
	}))

	// Read event
	events, err := s.Client.Read(0, 0)
	require.NoError(t, err)

	check(t, events, expected{
		{
			"ключ":     "значение",
			"გასაღები": "მნიშვნელობა",
		},
	})
}

// TestReadN assumes no errors when reading a limited slice
func TestReadN(t *testing.T) {
	s, teardown := NewSetup(t)
	defer teardown()

	const numEvents = 10

	for i := 0; i < numEvents; i++ {
		require.NoError(
			t,
			s.Client.Append(Payload{"index": i}),
		)
	}

	// Read the first half of events
	events, err := s.Client.Read(0, 5)
	require.NoError(t, err)

	check(t, events, expected{
		{"index": float64(0)},
		{"index": float64(1)},
		{"index": float64(2)},
		{"index": float64(3)},
		{"index": float64(4)},
	})

	// Read the second half of events
	events, err = s.Client.Read(5, 5)
	require.NoError(t, err)

	check(t, events, expected{
		{"index": float64(5)},
		{"index": float64(6)},
		{"index": float64(7)},
		{"index": float64(8)},
		{"index": float64(9)},
	})
}

// TestAppendVersionMismatch assumes an ErrMismatchingVersions
// to be returned when trying to append with an outdated offset
func TestAppendVersionMismatch(t *testing.T) {
	s, teardown := NewSetup(t)
	defer teardown()

	// Append first event
	require.NoError(t, s.Client.AppendCheck(0, Payload{"index": "0"}))

	// Try to append second event on reserved offset
	err := s.Client.AppendCheck(0, Payload{"index": "1"})
	require.Error(t, err)
	require.True(t, errors.Is(err, clt.ErrMismatchingVersions))

	events, err := s.Client.Read(0, 0)
	require.NoError(t, err)

	check(t, events, expected{
		{"index": "0"},
	})
}

// TestReadEmptyLog assumes an ErrOffsetOutOfBound error
// to be returned when reading at offset 0 on an empty event log
func TestReadEmptyLog(t *testing.T) {
	s, teardown := NewSetup(t)
	defer teardown()

	events, err := s.Client.Read(0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, clt.ErrOffsetOutOfBound))

	require.Len(t, events, 0)
}

// TestReadOffsetOutOfBound assumes ErrOffsetOutOfBound
// to be returned when reading with an offset
// that's >= the length of the log
func TestReadOffsetOutOfBound(t *testing.T) {
	s, teardown := NewSetup(t)
	defer teardown()

	// Append first event
	require.NoError(t, s.Client.Append(Payload{"index": "0"}))

	events, err := s.Client.Read(1, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, clt.ErrOffsetOutOfBound))

	check(t, events, expected{})
}

// TestAppendInvalidPayload assumes ErrOffsetOutOfBound
// to be returned when reading with an offset
// that's >= the length of the log
func TestAppendInvalidPayload(t *testing.T) {
	for input, successExpectation := range consts.JSONValidationTest() {
		t.Run(fmt.Sprintf("%t_%s", successExpectation, input), func(t *testing.T) {
			s, teardown := NewSetup(t)
			defer teardown()

			// Append first event
			require.NoError(t, s.Client.Append(Payload{"index": "0"}))

			events, err := s.Client.Read(1, 0)
			require.Error(t, err)
			require.True(t, errors.Is(err, clt.ErrOffsetOutOfBound))

			check(t, events, expected{})
		})
	}
}
