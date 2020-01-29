package eventlog_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/romshark/eventlog/eventlog"
	enginefile "github.com/romshark/eventlog/eventlog/file"
	engineinmem "github.com/romshark/eventlog/eventlog/inmem"
	"github.com/romshark/eventlog/internal/consts"

	"github.com/stretchr/testify/require"
)

const timeTolerance = 1 * time.Second

type expected []map[string]interface{}
type Event struct {
	Offset  uint64
	Time    time.Time
	Payload map[string]interface{}
}

func check(
	t *testing.T,
	actual []Event,
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

// ImplementationTest creates isolated test setups using
// the given event log implementation factory
func ImplementationTest(
	t *testing.T,
	implFactory func(t *testing.T) (
		impl eventlog.EventLog,
		cleanUp func(t *testing.T),
	),
) {
	scan := func(
		l eventlog.EventLog,
		offset,
		n uint64,
	) ([]Event, error) {
		var events []Event
		err := l.Scan(offset, n, func(
			timestamp uint64,
			payload []byte,
			offset uint64,
		) error {
			var data map[string]interface{}
			if err := json.Unmarshal(payload, &data); err != nil {
				return fmt.Errorf("unexpected error: %w", err)
			}

			events = append(events, Event{
				Offset:  offset,
				Time:    time.Unix(int64(timestamp), 0),
				Payload: data,
			})

			return nil
		})
		return events, err
	}

	for tname, test := range map[string]func(*testing.T, eventlog.EventLog){
		"TestAppendRead": func(t *testing.T, l eventlog.EventLog) {
			// Append first event
			offset1, version1, tm1, err := l.Append(
				PayloadJSON(t, Payload{"ix": 1}),
			)
			require.NoError(t, err)
			require.Greater(t, version1, offset1)
			require.WithinDuration(t, time.Now(), tm1, time.Second*1)

			// Append second event
			offset2, version2, tm2, err := l.Append(
				PayloadJSON(t, Payload{"ix": 2}),
			)
			require.NoError(t, err)
			require.Greater(t, version2, offset2)
			require.Greater(t, offset2, offset1)
			require.Greater(t, version2, version1)
			require.GreaterOrEqual(t, tm2.Unix(), tm1.Unix())

			// Append third event
			offset3, version3, tm3, err := l.Append(
				PayloadJSON(t, Payload{"ix": 3}),
			)
			require.NoError(t, err)
			require.Greater(t, version3, offset3)
			require.Greater(t, offset3, offset2)
			require.Greater(t, version3, version2)
			require.GreaterOrEqual(t, tm3.Unix(), tm2.Unix())

			// Read all events
			events, err := scan(l, offset1, 0)
			require.NoError(t, err)

			check(t, events, expected{
				{"ix": float64(1)},
				{"ix": float64(2)},
				{"ix": float64(3)},
			})
		},

		// TestAppendReadUTF8 assumes regular reading and writing
		// events with UTF-8 encoded payloads to succeed
		"TestAppendReadUTF8": func(t *testing.T, l eventlog.EventLog) {
			// Append first event
			newOffset, _, _, err := l.Append(PayloadJSON(t, Payload{
				"ключ":     "значение",
				"გასაღები": "მნიშვნელობა",
			}))
			require.NoError(t, err)

			// Read event
			events, err := scan(l, newOffset, 0)
			require.NoError(t, err)

			check(t, events, expected{
				{
					"ключ":     "значение",
					"გასაღები": "მნიშვნელობა",
				},
			})
		},

		// TestReadN assumes no errors when reading a limited slice
		"TestReadN": func(t *testing.T, l eventlog.EventLog) {
			const numEvents = 10

			offsets := make([]uint64, 0, numEvents)
			for i := 0; i < numEvents; i++ {
				offset, _, _, err := l.Append(PayloadJSON(t, Payload{"index": i}))
				require.NoError(t, err)
				offsets = append(offsets, offset)
			}

			// Read the first half of events
			events, err := scan(l, l.FirstOffset(), 5)
			require.NoError(t, err)

			check(t, events, expected{
				{"index": float64(0)},
				{"index": float64(1)},
				{"index": float64(2)},
				{"index": float64(3)},
				{"index": float64(4)},
			})

			// Read the second half of events
			events, err = scan(l, offsets[5], 5)
			require.NoError(t, err)

			check(t, events, expected{
				{"index": float64(5)},
				{"index": float64(6)},
				{"index": float64(7)},
				{"index": float64(8)},
				{"index": float64(9)},
			})
		},

		// TestReadNGreaterLen assumes no errors when reading n logs where
		// n is greater than the actual log size
		"TestReadNGreaterLen": func(t *testing.T, l eventlog.EventLog) {
			const numEvents = 5

			for i := 0; i < numEvents; i++ {
				_, _, _, err := l.Append(PayloadJSON(t, Payload{"index": i}))
				require.NoError(t, err)
			}

			// Read more events than there actually are
			events, err := scan(l, l.FirstOffset(), numEvents+1)
			require.NoError(t, err)

			check(t, events, expected{
				{"index": float64(0)},
				{"index": float64(1)},
				{"index": float64(2)},
				{"index": float64(3)},
				{"index": float64(4)},
			})
		},

		// TestAppendVersionMismatch assumes an ErrMismatchingVersions
		// to be returned when trying to append with an outdated offset
		"TestAppendVersionMismatch": func(t *testing.T, l eventlog.EventLog) {
			// Append first event
			_, version, _, err := l.Append(PayloadJSON(t, Payload{"index": "0"}))
			require.NoError(t, err)
			require.Greater(t, version, uint64(0))

			// Try to append second event on an invalid version offset
			offset, newVersion, tm, err := l.AppendCheck(
				version+1, // Mismatching version
				PayloadJSON(t, Payload{"index": "1"}),
			)
			require.Error(t, err)
			require.True(
				t,
				errors.Is(err, eventlog.ErrMismatchingVersions),
				"unexpected error: %s",
				err,
			)
			require.Zero(t, newVersion)
			require.Zero(t, offset)
			require.Zero(t, tm)

			events, err := scan(l, l.FirstOffset(), 0)
			require.NoError(t, err)

			check(t, events, expected{
				{"index": "0"},
			})
		},

		// TestReadEmptyLog assumes an ErrOffsetOutOfBound error
		// to be returned when reading at offset 0 on an empty event log
		"TestReadEmptyLog": func(t *testing.T, l eventlog.EventLog) {
			events, err := scan(l, l.FirstOffset(), 0)
			require.Error(t, err)
			require.True(
				t,
				errors.Is(err, eventlog.ErrOffsetOutOfBound),
				"unexpected error: %s",
				err,
			)

			require.Len(t, events, 0)
		},

		// TestReadOffsetOutOfBound assumes ErrOffsetOutOfBound
		// to be returned when reading with an offset
		// that's >= the length of the log
		"TestReadOffsetOutOfBound": func(t *testing.T, l eventlog.EventLog) {
			// Append first event
			_, newVersion, _, err := l.Append(
				PayloadJSON(t, Payload{"index": "0"}),
			)
			require.NoError(t, err)

			events, err := scan(l, newVersion, 0)
			require.Error(t, err)
			require.True(
				t,
				errors.Is(err, eventlog.ErrOffsetOutOfBound),
				"unexpected error: %s",
				err,
			)

			check(t, events, expected{})
		},

		// TestAppendInvalidPayload assumes ErrOffsetOutOfBound
		// to be returned when reading with an offset
		// that's >= the length of the log
		"TestAppendInvalidPayload": func(t *testing.T, l eventlog.EventLog) {
			for input, successExpect := range consts.JSONValidationTest() {
				t.Run(fmt.Sprintf(
					"%t_%s",
					successExpect,
					input,
				), func(t *testing.T) {
					offset, version, tm, err := l.Append([]byte(input))
					if successExpect {
						require.NoError(t, err)
						require.Greater(t, version, uint64(0))
						require.WithinDuration(t, time.Now(), tm, timeTolerance)
					} else {
						require.Error(t, err)
						require.True(
							t,
							errors.Is(err, eventlog.ErrInvalidPayload),
							"unexpected error: %s",
							err,
						)
						require.Zero(t, offset)
						require.Zero(t, version)
						require.Zero(t, tm)
					}
				})
			}
		},
	} {
		t.Run(tname, func(t *testing.T) {
			s, cleanUp := implFactory(t)
			defer cleanUp(t)
			test(t, s)
		})
	}
}

type Payload map[string]interface{}

func PayloadJSON(t *testing.T, p Payload) []byte {
	b, err := json.Marshal(p)
	require.NoError(t, err)
	return b
}

// TestInmem tests the volatile in-memory event log implementation
func TestInmem(t *testing.T) {
	ImplementationTest(t, func(t *testing.T) (
		impl eventlog.EventLog,
		cleanUp func(t *testing.T),
	) {
		l, err := engineinmem.NewInmem()
		require.NoError(t, err)
		require.NotNil(t, l)
		impl = l
		cleanUp = func(t *testing.T) {}
		return
	})
}

// TestFile tests the persistent file-based event log implementation
func TestFile(t *testing.T) {
	ImplementationTest(t, func(t *testing.T) (
		impl eventlog.EventLog,
		cleanUp func(t *testing.T),
	) {
		filePath := fmt.Sprintf(
			"./test_%s_%s",
			strings.ReplaceAll(t.Name(), "/", "_"),
			time.Now().Format(time.RFC3339Nano),
		)

		l, err := enginefile.NewFile(filePath)
		require.NoError(t, err)
		require.NotNil(t, l)

		impl = l
		cleanUp = func(t *testing.T) {
			require.NoError(t, os.Remove(filePath))
		}

		return
	})
}
