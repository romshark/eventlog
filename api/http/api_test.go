package http_test

import (
	"errors"
	"fmt"
	"net"
	"strconv"
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

const timeTolerance = 1 * time.Second

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
	offset1, newVersion1, tm1, err := s.Client.Append(Payload{"ix": 1})
	require.NoError(t, err)
	hexGreater(t, newVersion1, offset1)
	require.WithinDuration(t, time.Now(), tm1, timeTolerance)

	// Append second event
	offset2, newVersion2, tm2, err := s.Client.Append(Payload{"ix": 2})
	require.NoError(t, err)
	hexGreater(t, offset2, offset1)
	hexGreater(t, newVersion2, offset2)
	require.GreaterOrEqual(t, tm2.Unix(), tm1.Unix())

	// Append third event
	offset3, newVersion3, tm3, err := s.Client.Append(Payload{"ix": 3})
	require.NoError(t, err)
	hexGreater(t, offset3, offset2)
	hexGreater(t, newVersion3, offset3)
	require.GreaterOrEqual(t, tm3.Unix(), tm2.Unix())

	// Read all events
	events, err := s.Client.Read(offset1, 0)
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
	offset, _, _, err := s.Client.Append(Payload{
		"ключ":     "значение",
		"გასაღები": "მნიშვნელობა",
	})
	require.NoError(t, err)

	// Read event
	events, err := s.Client.Read(offset, 0)
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
	var firstOffset string

	offsets := make([]string, 0, numEvents)

	for i := 0; i < numEvents; i++ {
		offset, _, _, err := s.Client.Append(Payload{"index": i})
		if i == 0 {
			firstOffset = offset
		}
		require.NoError(t, err)
		offsets = append(offsets, offset)
	}

	// Read the first half of events
	events, err := s.Client.Read(firstOffset, 5)
	require.NoError(t, err)

	check(t, events, expected{
		{"index": float64(0)},
		{"index": float64(1)},
		{"index": float64(2)},
		{"index": float64(3)},
		{"index": float64(4)},
	})

	// Read the second half of events
	events, err = s.Client.Read(offsets[5], 5)
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
	offsetFirst, _, _, err := s.Client.Append(Payload{"index": "0"})
	require.NoError(t, err)

	// Try to append second event on an outdated/invalid version
	offset, newVersion, tm, err := s.Client.AppendCheck(
		offsetFirst,
		Payload{"index": "1"},
	)
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, clt.ErrMismatchingVersions),
		"unexpected error: %s",
		err,
	)
	require.Zero(t, offset)
	require.Zero(t, newVersion)
	require.Zero(t, tm)

	events, err := s.Client.Read(offsetFirst, 0)
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

	events, err := s.Client.Read("0", 0)
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, clt.ErrOffsetOutOfBound),
		"unexpected error: %s",
		err,
	)

	require.Len(t, events, 0)
}

// TestReadOffsetOutOfBound assumes ErrOffsetOutOfBound
// to be returned when reading with an offset
// that's >= the length of the log
func TestReadOffsetOutOfBound(t *testing.T) {
	s, teardown := NewSetup(t)
	defer teardown()

	// Append first event
	offsetFirst, _, _, err := s.Client.Append(Payload{"index": "0"})
	require.NoError(t, err)

	offsetDisplaced := incUi64Hex(t, offsetFirst, 1)

	events, err := s.Client.Read(offsetDisplaced, 0)
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, clt.ErrOffsetOutOfBound),
		"unexpected error: %s",
		err,
	)

	check(t, events, expected{})
}

// TestAppendInvalidPayload assumes ErrOffsetOutOfBound
// to be returned when reading with an offset
// that's >= the length of the log
func TestAppendInvalidPayload(t *testing.T) {
	for input, expSuccess := range consts.JSONValidationTest() {
		if expSuccess {
			continue
		}
		t.Run(fmt.Sprintf("%t_%s", expSuccess, input), func(t *testing.T) {
			s, teardown := NewSetup(t)
			defer teardown()

			// Try to append an event
			offset, newVersion, tm, err := s.Client.AppendBytes([]byte(input))

			require.Error(t, err)
			require.True(
				t,
				errors.Is(err, clt.ErrInvalidPayload),
				"unexpected error: %s (%s)",
				err,
			)
			require.Zero(t, offset)
			require.Zero(t, newVersion)
			require.Zero(t, tm)

			events, err := s.Client.Read(offset, 0)
			require.Error(t, err)
			check(t, events, expected{})

		})
	}
}

func decHex(t *testing.T, s string) uint64 {
	n, err := strconv.ParseUint(s, 16, 64)
	require.NoError(t, err)
	return n
}

func hexGreater(t *testing.T, a, b string) {
	ia := decHex(t, a)
	ib := decHex(t, b)
	require.Greater(
		t, ia, ib,
		"a (%q = %d) isn't greater b (%q = %d)",
		a, ia, b, ib,
	)
}

func incUi64Hex(t *testing.T, orig string, delta uint64) string {
	return fmt.Sprintf("%x", decHex(t, orig)+delta)
}

// TestReadBegin assumes the correct offset to be returned for GET /begin
func TestReadBegin(t *testing.T) {
	s, teardown := NewSetup(t)
	defer teardown()

	firstOffset1, err := s.Client.Begin()
	require.NoError(t, err)
	require.NotZero(t, firstOffset1)

	offset, _, _, err := s.Client.Append(Payload{"x": "y"})
	require.NoError(t, err)
	require.Equal(t, firstOffset1, offset)

	firstOffset2, err := s.Client.Begin()
	require.NoError(t, err)
	require.Equal(t, firstOffset1, firstOffset2)
}
