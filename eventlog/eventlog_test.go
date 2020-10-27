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
	"github.com/romshark/eventlog/eventlog/inmem"
	"github.com/romshark/eventlog/internal/consts"

	"github.com/stretchr/testify/require"
)

func TestAppendRead(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		// Append first event
		offset1, version1, tm1, err := l.Append(
			MakeEvent(t, "foo", Payload{"ix": 1}),
		)
		require.NoError(t, err)
		require.Greater(t, version1, offset1)
		require.WithinDuration(t, time.Now(), tm1, time.Second*1)

		// Append second event
		offset2, version2, tm2, err := l.Append(
			MakeEvent(t, "foo", Payload{"ix": 2}),
		)
		require.NoError(t, err)
		require.Greater(t, version2, offset2)
		require.Greater(t, offset2, offset1)
		require.Greater(t, version2, version1)
		require.GreaterOrEqual(t, tm2.Unix(), tm1.Unix())

		// Check-append third event
		offset3, version3, tm3, err := l.AppendCheck(
			version2,
			MakeEvent(t, "foo", Payload{"ix": 3}),
		)
		require.NoError(t, err)
		require.Greater(t, version3, offset3)
		require.Greater(t, offset3, offset2)
		require.Greater(t, version3, version2)
		require.GreaterOrEqual(t, tm3.Unix(), tm2.Unix())

		// Append fourth, fifth and sixth event
		offset4, version4, tm4, err := l.AppendMulti(
			MakeEvent(t, "foo", Payload{"ix": 4}),
			MakeEvent(t, "foo", Payload{"ix": 5}),
			MakeEvent(t, "foo", Payload{"ix": 6}),
		)
		require.NoError(t, err)
		require.Greater(t, version4, offset4)
		require.Greater(t, offset4, offset3)
		require.Greater(t, version4, version3)
		require.GreaterOrEqual(t, tm4.Unix(), tm2.Unix())

		// Check-append seventh, eighth and ninth event
		offset5, version5, tm5, err := l.AppendCheckMulti(
			version4,
			MakeEvent(t, "foo", Payload{"ix": 7}),
			MakeEvent(t, "foo", Payload{"ix": 8}),
			MakeEvent(t, "foo", Payload{"ix": 9}),
		)
		require.NoError(t, err)
		require.Greater(t, version5, offset5)
		require.Greater(t, offset5, offset3)
		require.Greater(t, version5, version3)
		require.GreaterOrEqual(t, tm5.Unix(), tm2.Unix())

		// Read all events
		events, nextOffset, err := scan(l, offset1, 0)
		require.NoError(t, err)
		require.Equal(t, l.Version(), nextOffset)

		check(t, events, expected{
			{"ix": float64(1)},
			{"ix": float64(2)},
			{"ix": float64(3)},
			{"ix": float64(4)},
			{"ix": float64(5)},
			{"ix": float64(6)},
			{"ix": float64(7)},
			{"ix": float64(8)},
			{"ix": float64(9)},
		})
	})
}

// TestAppendReadUTF8 assumes regular reading and writing
// events with UTF-8 encoded payloads to succeed
func TestAppendReadUTF8(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		// Append first event
		newOffset, newVersion, _, err := l.Append(
			MakeEvent(t, "i18n", Payload{
				"ключ":     "значение",
				"გასაღები": "მნიშვნელობა",
			}),
		)
		require.NoError(t, err)

		// Read event
		events, nextOffset, err := scan(l, newOffset, 0)
		require.NoError(t, err)
		require.Equal(t, l.Version(), nextOffset)
		require.Equal(t, l.Version(), newVersion)

		check(t, events, expected{
			{
				"ключ":     "значение",
				"გასაღები": "მნიშვნელობა",
			},
		})
	})
}

// TestAppendInvalidPayload assumes ErrInvalidPayload
// to be returned when reading with an offset
// that's >= the length of the log
func TestAppendInvalidPayload(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		for input, successExpect := range validationTestJSON() {
			t.Run(fmt.Sprintf(
				"%t_%s",
				successExpect, input,
			), func(t *testing.T) {
				offset, version, tm, err := l.Append(eventlog.Event{
					Label:       "foo",
					PayloadJSON: []byte(input),
				})
				if successExpect {
					require.NoError(t, err)
					require.Greater(t, version, uint64(0))
					require.WithinDuration(t, time.Now(), tm, timeTolerance)
				} else {
					require.Error(t, err)
					require.True(t,
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
	})
}

// TestAppendInvalidLabel assumes ErrLabelContainsIllegalChar to be returned
func TestAppendInvalidLabel(t *testing.T) {
	validPayload := []byte(`{"x":0}`)
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		for _, cs := range validateTestLabel() {
			offset, version, tm, err := l.Append(eventlog.Event{
				Label:       cs.Label,
				PayloadJSON: validPayload,
			})
			if cs.Legal {
				require.NoError(t, err)
				require.Greater(t, version, uint64(0))
				require.WithinDuration(t, time.Now(), tm, timeTolerance)
			} else {
				require.Error(t, err)
				require.True(t,
					errors.Is(err, eventlog.ErrLabelContainsIllegalChars),
					"unexpected error: %s",
					err,
				)
				require.Zero(t, offset)
				require.Zero(t, version)
				require.Zero(t, tm)
			}
		}
	})
}

// TestReadN assumes no errors when reading a limited slice
func TestReadN(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		const numEvents = 10

		var latestVersion uint64
		offsets := make([]uint64, 0, numEvents)
		for i := 0; i < numEvents; i++ {
			var offset uint64
			var err error
			offset, latestVersion, _, err = l.Append(
				MakeEvent(t, "foo", Payload{"index": i}),
			)
			require.NoError(t, err)
			offsets = append(offsets, offset)
		}

		// Read the first half of events
		events, nextOffset, err := scan(l, l.FirstOffset(), 5)
		require.NoError(t, err)
		require.Equal(t, offsets[5], nextOffset)

		check(t, events, expected{
			{"index": float64(0)},
			{"index": float64(1)},
			{"index": float64(2)},
			{"index": float64(3)},
			{"index": float64(4)},
		})

		// Read the second half of events using the offset
		// of the next item from the previous scan
		events, nextOffset, err = scan(l, nextOffset, 5)
		require.NoError(t, err)
		require.Equal(t, latestVersion, nextOffset)

		check(t, events, expected{
			{"index": float64(5)},
			{"index": float64(6)},
			{"index": float64(7)},
			{"index": float64(8)},
			{"index": float64(9)},
		})
	})
}

// TestReadNGreaterLen assumes no errors when reading n logs where
// n is greater than the actual log size
func TestReadNGreaterLen(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		const numEvents = 5

		var latestVersion uint64
		for i := 0; i < numEvents; i++ {
			var err error
			_, latestVersion, _, err = l.Append(
				MakeEvent(t, "foo", Payload{"index": i}),
			)
			require.NoError(t, err)
		}

		// Read more events than there actually are
		events, nextOffset, err := scan(l, l.FirstOffset(), numEvents+1)
		require.NoError(t, err)
		require.Equal(t, latestVersion, nextOffset)

		check(t, events, expected{
			{"index": float64(0)},
			{"index": float64(1)},
			{"index": float64(2)},
			{"index": float64(3)},
			{"index": float64(4)},
		})
	})
}

// TestAppendVersionMismatch assumes an ErrMismatchingVersions
// to be returned when trying to append with an outdated offset
func TestAppendVersionMismatch(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		// Append first event
		_, version, _, err := l.Append(MakeEvent(t, "foo", Payload{"index": "0"}))
		require.NoError(t, err)
		require.Greater(t, version, uint64(0))

		// Try to append second event on an invalid version offset
		offset, newVersion, tm, err := l.AppendCheck(
			version+1, // Mismatching version
			MakeEvent(t, "foo", Payload{"index": "1"}),
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

		events, nextOffset, err := scan(l, l.FirstOffset(), 0)
		require.NoError(t, err)
		require.Equal(t, version, nextOffset)

		check(t, events, expected{
			{"index": "0"},
		})
	})
}

// TestReadEmptyLog assumes an ErrOffsetOutOfBound error
// to be returned when reading at offset 0 on an empty event log
func TestReadEmptyLog(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		events, nextOffset, err := scan(l, l.FirstOffset(), 0)
		require.Error(t, err)
		require.True(
			t,
			errors.Is(err, eventlog.ErrOffsetOutOfBound),
			"unexpected error: %s",
			err,
		)
		require.Zero(t, nextOffset)

		require.Len(t, events, 0)
	})
}

// TestReadOffsetOutOfBound assumes ErrOffsetOutOfBound
// to be returned when reading with an offset
// that's >= the length of the log
func TestReadOffsetOutOfBound(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		// Append first event
		_, newVersion, _, err := l.Append(
			MakeEvent(t, "foo", Payload{"index": "0"}),
		)
		require.NoError(t, err)

		events, nextOffset, err := scan(l, newVersion, 0)
		require.Error(t, err)
		require.True(
			t,
			errors.Is(err, eventlog.ErrOffsetOutOfBound),
			"unexpected error: %s",
			err,
		)
		require.Zero(t, nextOffset)

		check(t, events, expected{})
	})
}

func TestScanSingle(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		e := make([]struct {
			Time   time.Time
			Offset uint64
			Event  eventlog.Event
		}, 3)

		for i := range e {
			var err error
			e[i].Event = MakeEvent(t, "foo", Payload{"index": i})
			e[i].Offset, _, e[i].Time, err = l.Append(e[i].Event)
			require.NoError(t, err)
		}

		scanOffset := l.FirstOffset()
		for i, v := range e {
			var calls int
			nextOffset, err := l.Scan(scanOffset, 1, func(
				offset uint64,
				timestamp uint64,
				label []byte,
				payloadJSON []byte,
			) error {
				calls++
				require.Equal(t, uint64(v.Time.Unix()), timestamp)
				require.Equal(t, v.Offset, offset)
				require.Equal(t, v.Event.Label, string(label))
				require.Equal(t,
					string(v.Event.PayloadJSON),
					string(payloadJSON),
				)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			if i < len(e)-1 {
				require.Equal(t, e[i+1].Offset, nextOffset)
			} else {
				require.Equal(t, l.Version(), nextOffset)
			}
			scanOffset = nextOffset
		}
	})
}

func TestScanUnlimited(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		e := make([]struct {
			Time   time.Time
			Offset uint64
			Event  eventlog.Event
		}, 3)

		for i := range e {
			var err error
			e[i].Event = MakeEvent(t, "foo", Payload{"index": i})
			e[i].Offset, _, e[i].Time, err = l.Append(e[i].Event)
			require.NoError(t, err)
		}

		var counter int
		nextOffset, err := l.Scan(l.FirstOffset(), 0, func(
			offset uint64,
			timestamp uint64,
			label []byte,
			payloadJSON []byte,
		) error {
			v := e[counter]
			counter++

			require.Equal(t, uint64(v.Time.Unix()), timestamp)
			require.Equal(t, v.Offset, offset)
			require.Equal(t, v.Event.Label, string(label))
			require.Equal(t,
				string(v.Event.PayloadJSON),
				string(payloadJSON),
			)

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, counter)
		require.Equal(t, l.Version(), nextOffset)
	})
}

func TestScanExceedLenght(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		e := make([]struct {
			Time   time.Time
			Offset uint64
			Event  eventlog.Event
		}, 3)

		for i := range e {
			var err error
			e[i].Event = MakeEvent(t, "foo", Payload{"index": i})
			e[i].Offset, _, e[i].Time, err = l.Append(e[i].Event)
			require.NoError(t, err)
		}

		scanFor := uint64(len(e)) * 2
		var counter int
		nextOffset, err := l.Scan(l.FirstOffset(), scanFor, func(
			offset uint64,
			timestamp uint64,
			label []byte,
			payloadJSON []byte,
		) error {
			v := e[counter]
			counter++

			require.Equal(t, uint64(v.Time.Unix()), timestamp)
			require.Equal(t, v.Offset, offset)
			require.Equal(t, v.Event.Label, string(label))
			require.Equal(t,
				string(v.Event.PayloadJSON),
				string(payloadJSON),
			)

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, counter)
		require.Equal(t, l.Version(), nextOffset)
	})
}

func TestScanErrOffsetOutOfBound(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		var counter int
		nextOffset, err := l.Scan(l.FirstOffset(), 0, func(
			uint64, uint64, []byte, []byte,
		) error {
			counter++
			return nil
		})
		require.Error(t, err)
		require.True(t,
			errors.Is(err, eventlog.ErrOffsetOutOfBound),
			"unexpected error: (%T) %s", err, err.Error(),
		)
		require.Zero(t, counter)
		require.Zero(t, nextOffset)

		// Try zero-offset
		nextOffset, err = l.Scan(
			0, 0,
			func(uint64, uint64, []byte, []byte) error {
				counter++
				return nil
			},
		)
		require.Error(t, err)
		require.True(t,
			errors.Is(err, eventlog.ErrOffsetOutOfBound),
			"unexpected error: (%T) %s", err, err.Error(),
		)
		require.Zero(t, counter)
		require.Zero(t, nextOffset)
	})
}

func TestScanReturnErr(t *testing.T) {
	test(t, func(t *testing.T, l *eventlog.EventLog) {
		e := make([]struct {
			Time   time.Time
			Offset uint64
			Event  eventlog.Event
		}, 3)

		for i := range e {
			var err error
			e[i].Event = MakeEvent(t, "foo", Payload{"index": i})
			e[i].Offset, _, e[i].Time, err = l.Append(e[i].Event)
			require.NoError(t, err)
		}

		testErr := errors.New("test error")

		var counter int
		nextOffset, err := l.Scan(l.FirstOffset(), 0, func(
			offset uint64,
			timestamp uint64,
			label []byte,
			payloadJSON []byte,
		) error {
			counter++

			v := e[0]
			require.Equal(t, uint64(v.Time.Unix()), timestamp)
			require.Equal(t, v.Offset, offset)
			require.Equal(t, v.Event.Label, string(label))
			require.Equal(t,
				string(v.Event.PayloadJSON),
				string(payloadJSON),
			)

			return testErr
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, testErr))
		require.Equal(t, 1, counter)
		require.Equal(t, e[1].Offset, nextOffset)
	})
}

func test(t *testing.T, fn func(*testing.T, *eventlog.EventLog)) {
	t.Run("Inmem", func(t *testing.T) {
		l := eventlog.New(inmem.New())
		t.Cleanup(func() {
			if err := l.Close(); err != nil {
				panic(fmt.Errorf("closing eventlog: %s", err))
			}
		})
		fn(t, l)
	})

	t.Run("File", func(t *testing.T) {
		filePath := fmt.Sprintf(
			"./test_%s_%s",
			strings.ReplaceAll(t.Name(), "/", "_"),
			time.Now().Format(time.RFC3339Nano),
		)

		e, err := enginefile.New(filePath)
		require.NoError(t, err)
		require.NotNil(t, e)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(filePath))
		})

		fn(t, eventlog.New(e))
	})
}

func scan(
	l *eventlog.EventLog,
	offset, n uint64,
) ([]Event, uint64, error) {
	var events []Event
	nextOffset, err := l.Scan(offset, n, func(
		offset uint64,
		timestamp uint64,
		label []byte,
		payloadJSON []byte,
	) error {
		var data map[string]interface{}
		if err := json.Unmarshal(payloadJSON, &data); err != nil {
			return fmt.Errorf("unexpected error: %w", err)
		}

		events = append(events, Event{
			Offset:  offset,
			Time:    time.Unix(int64(timestamp), 0),
			Label:   string(label),
			Payload: data,
		})

		return nil
	})
	return events, nextOffset, err
}

const timeTolerance = 1 * time.Second

type expected []map[string]interface{}
type Event struct {
	Offset  uint64
	Time    time.Time
	Label   string
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

type Payload map[string]interface{}

func MakeEvent(t *testing.T, label string, p Payload) eventlog.Event {
	b, err := json.Marshal(p)
	require.NoError(t, err)
	return eventlog.Event{Label: label, PayloadJSON: b}
}

func validationTestJSON() map[string]bool {
	return map[string]bool{
		"":    false,
		"   ": false,
		" \r\n  \n  	 ": false,

		"{}":    false,
		"{   }": false,
		"{ \r\n  \n  	 }": false,

		"[]": false,

		`{"foo":"bar}`: false,
		`{foo:"bar"}`:  false,

		`{"foo":"bar"}`:       true,
		`{"ключ":"значение"}`: true,
	}
}

type LabelTestCase struct {
	Name  string
	Label string
	Legal bool
}

func validateTestLabel() []LabelTestCase {
	legalChars := []byte{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
		'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
		'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		'-', '_', '.', '~', '%',
	}
	isLegal := func(b byte) bool {
		for _, c := range legalChars {
			if c == b {
				return true
			}
		}
		return false
	}

	t := make([]LabelTestCase, 0, 258)
	for i := byte(0); ; i++ {
		v := LabelTestCase{
			Label: string(i),
			Legal: isLegal(i),
		}
		if v.Legal {
			v.Name = "legal byte"
		} else {
			v.Name = "illegal byte"
		}
		t = append(t, v)

		if i == 255 {
			break
		}
	}

	// Max possible label length
	t = append(t, LabelTestCase{
		Name:  "max possible len",
		Legal: true,
		Label: func() string {
			s := make([]byte, consts.MaxLabelLen)
			for i := range s {
				s[i] = 'x'
			}
			return string(s)
		}(),
	})

	// Empty label
	t = append(t, LabelTestCase{
		Name:  "empty",
		Label: "",
		Legal: true,
	})

	return t
}
