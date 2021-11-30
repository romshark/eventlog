package eventlog_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	logfile "github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/inmem"
	"github.com/romshark/eventlog/internal/makestr"

	"github.com/stretchr/testify/require"
)

func TestAppendRead(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		// Append first event
		pv1, v1, tm1, err := l.Append(
			MakeEvent(t, "1", Payload{"ix": 1}),
		)
		require.NoError(t, err)
		require.Zero(t, pv1)
		require.Greater(t, v1, pv1)
		require.WithinDuration(t, time.Now(), tm1, time.Second*1)

		// Append second event
		pv2, v2, tm2, err := l.Append(
			MakeEvent(t, "2", Payload{"ix": 2}),
		)
		require.NoError(t, err)
		require.Equal(t, v1, pv2)
		require.Greater(t, v2, pv2)
		require.GreaterOrEqual(t, tm2.Unix(), tm1.Unix())

		// Check-append third event
		v3, tm3, err := l.AppendCheck(
			v2, MakeEvent(t, "3", Payload{"ix": 3}),
		)
		require.NoError(t, err)
		require.Greater(t, v3, v2)
		require.GreaterOrEqual(t, tm3.Unix(), tm2.Unix())

		// Append fourth, fifth and sixth event
		pv4, fv4, v4, tm4, err := l.AppendMulti(
			MakeEvent(t, "4", Payload{"ix": 4}),
			MakeEvent(t, "5", Payload{"ix": 5}),
			MakeEvent(t, "6", Payload{"ix": 6}),
		)
		require.NoError(t, err)
		require.Equal(t, pv4, v3)
		require.Greater(t, fv4, pv4)
		require.Greater(t, v4, fv4)
		require.GreaterOrEqual(t, tm4.Unix(), tm3.Unix())

		// Check-append seventh, eighth and ninth event
		fv5, v5, tm5, err := l.AppendCheckMulti(
			v4,
			MakeEvent(t, "7", Payload{"ix": 7}),
			MakeEvent(t, "8", Payload{"ix": 8}),
			MakeEvent(t, "9", Payload{"ix": 9}),
		)
		require.NoError(t, err)
		require.Greater(t, fv5, v4)
		require.Greater(t, v5, fv5)
		require.GreaterOrEqual(t, tm5.Unix(), tm4.Unix())

		t.Run("scan", func(t *testing.T) {
			// Read all events
			events, err := scan(l, l.VersionInitial(), false, 0)
			require.NoError(t, err)
			require.Len(t, events, 9)

			for i, e := range events {
				labelInt, err := strconv.Atoi(string(e.Label))
				require.NoError(t, err)

				require.Equal(t, i+1, labelInt)

				require.Equal(t, Payload{"ix": float64(i + 1)}, Payload(e.Payload))
				switch {
				case i == 0:
					// First
					v, n := events[i], events[i+1]
					require.Equal(t, uint64(0), v.VersionPrevious)
					require.Equal(t, l.VersionInitial(), v.Version)
					require.Equal(t, n.Version, v.VersionNext)
					require.GreaterOrEqual(t, n.Time.Unix(), v.Time.Unix())
				case i+1 == len(events):
					// Last
					p, v := events[i-1], events[i]
					require.Equal(t, p.Version, v.VersionPrevious)
					require.Equal(t, p.VersionNext, v.Version)
					require.GreaterOrEqual(t, v.Time.Unix(), p.Time.Unix())
					require.WithinDuration(t, time.Now(), v.Time, time.Second)
				default:
					p, v, n := events[i-1], events[i], events[i+1]
					require.Equal(t, p.Version, v.VersionPrevious)
					require.Equal(t, p.VersionNext, v.Version)
					require.Equal(t, n.VersionPrevious, v.Version)
					require.Equal(t, n.Version, v.VersionNext)
					require.GreaterOrEqual(t, n.Time.Unix(), v.Time.Unix())
					require.GreaterOrEqual(t, v.Time.Unix(), p.Time.Unix())
				}
			}
		})

		t.Run("scan reverse", func(t *testing.T) {
			events, err := scan(l, l.Version(), true, 0)
			require.NoError(t, err)
			require.Len(t, events, 9)

			expectedLabelCounter := 1
			for i := len(events) - 1; i >= 0; i-- {
				e := events[i]

				labelInt, err := strconv.Atoi(string(e.Label))
				require.NoError(t, err)

				require.Equal(t, expectedLabelCounter, labelInt)
				require.Equal(t, Payload{
					"ix": float64(expectedLabelCounter),
				}, Payload(e.Payload))

				expectedLabelCounter++

				switch {
				case i+1 == len(events):
					// First
					v, n := events[i], events[i-1]
					require.Equal(t, uint64(0), v.VersionPrevious)
					require.Equal(t, l.VersionInitial(), v.Version)
					require.Equal(t, n.Version, v.VersionNext)
					require.GreaterOrEqual(t, n.Time.Unix(), v.Time.Unix())
				case i < 1:
					// Last
					p, v := events[i+1], events[i]
					require.Equal(t, p.Version, v.VersionPrevious)
					require.Equal(t, p.VersionNext, v.Version)
					require.GreaterOrEqual(t, v.Time.Unix(), p.Time.Unix())
					require.WithinDuration(t, time.Now(), v.Time, time.Second)
				default:
					p, v, n := events[i+1], events[i], events[i-1]
					require.Equal(t, p.Version, v.VersionPrevious)
					require.Equal(t, p.VersionNext, v.Version)
					require.Equal(t, n.VersionPrevious, v.Version)
					require.Equal(t, n.Version, v.VersionNext)
					require.GreaterOrEqual(t, n.Time.Unix(), v.Time.Unix())
					require.GreaterOrEqual(t, v.Time.Unix(), p.Time.Unix())
				}
			}
		})

		t.Run("scan meta", func(t *testing.T) {
			// Read metadata
			m := make(map[string]string, l.MetadataLen())
			l.ScanMetadata(func(f, v string) bool {
				m[f] = v
				return true
			})
			require.Len(t, m, len(s.Metadata))
			for f, v := range s.Metadata {
				require.Contains(t, m, f)
				require.Equal(t, v, m[f])
			}
		})
	})
}

// TestPayloadUTF8 assumes regular reading and writing
// events with UTF-8 encoded payloads to succeed
func TestPayloadUTF8(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		// Append event
		_, v1, _, err := l.Append(
			MakeEvent(t, "i18n", Payload{
				"ключ":     "значение",
				"გასაღები": "მნიშვნელობა",
			}),
		)
		require.NoError(t, err)

		// Read event
		events, err := scan(l, v1, false, 0)
		require.NoError(t, err)
		require.Equal(t, v1, l.Version())

		require.Len(t, events, 1)
		{
			v := events[0]
			require.Equal(t, "i18n", string(v.Label))
			require.Equal(t, Payload{
				"ключ":     "значение",
				"გასაღები": "მნიშვნელობა",
			}, Payload(v.Payload))
			require.Zero(t, v.VersionPrevious)
			require.Equal(t, l.VersionInitial(), v.Version)
			require.Equal(t, l.Version(), v.Version)
			require.Zero(t, v.VersionNext)
			require.WithinDuration(t, time.Now(), v.Time, time.Second)
		}
	})
}

// TestAppendInvalidPayload assumes ErrInvalidPayload
// to be returned when appending invalid JSON payloads
func TestAppendInvalidPayload(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		for input, successExpect := range validationTestJSON() {
			t.Run(fmt.Sprintf(
				"%t_%s",
				successExpect, input,
			), func(t *testing.T) {
				pv, v, tm, err := l.Append(eventlog.EventData{
					Label:       []byte("foo"),
					PayloadJSON: []byte(input),
				})
				if successExpect {
					require.NoError(t, err)
					require.Greater(t, v, pv)
					require.WithinDuration(t, time.Now(), tm, timeTolerance)
				} else {
					require.Error(t, err)
					require.True(t,
						errors.Is(err, eventlog.ErrInvalidPayload),
						"unexpected error: (%T) %s", err, err.Error(),
					)
					require.Zero(t, pv)
					require.Zero(t, v)
					require.Zero(t, tm)
				}
			})
		}
	})
}

// TestAppendPayloadExceedsLimit assumes ErrInvalidPayload
// to be returned when appending JSON payloads exceeding the size limit.
func TestAppendPayloadExceedsLimit(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		p := makestr.MakeJSON(file.MaxPayloadLen + 1)
		require.True(t, len(p) > file.MaxPayloadLen)

		pv, v, tm, err := l.Append(eventlog.EventData{
			Label:       []byte("foo"),
			PayloadJSON: []byte(p),
		})
		require.Error(t, err)
		require.True(t,
			errors.Is(err, eventlog.ErrPayloadSizeLimitExceeded),
			"unexpected error: (%T) %s", err, err.Error(),
		)
		require.Zero(t, pv)
		require.Zero(t, v)
		require.Zero(t, tm)
	})
}

// TestAppendInvalidLabel assumes ErrLabelContainsIllegalChar to be returned
// when using illegal characters for the label
func TestAppendInvalidLabel(t *testing.T) {
	validPayload := []byte(`{"x":0}`)
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		for _, cs := range validateTestLabel() {
			pv, v, tm, err := l.Append(eventlog.EventData{
				Label:       []byte(cs.Label),
				PayloadJSON: validPayload,
			})
			if cs.Legal {
				require.NoError(t, err)
				require.Greater(t, v, pv)
				require.WithinDuration(t, time.Now(), tm, timeTolerance)
			} else {
				require.Error(t, err)
				require.True(t,
					errors.Is(err, eventlog.ErrLabelContainsIllegalChars),
					"unexpected error: (%T) %s", err, err.Error(),
				)
				require.Zero(t, pv)
				require.Zero(t, v)
				require.Zero(t, tm)
			}
		}
	})
}

// TestAppendVersionMismatch assumes an ErrMismatchingVersions
// to be returned when trying to append with an outdated version
func TestAppendVersionMismatch(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		// Append first event
		_, v, _, err := l.Append(MakeEvent(t, "foo", Payload{"index": "0"}))
		require.NoError(t, err)
		require.Greater(t, v, uint64(0))

		// Try to append second event on an invalid version
		assumed := v + 1
		require.NotEqual(t, l.Version(), assumed)
		v2, tm, err := l.AppendCheck(
			assumed, // Not the current version
			MakeEvent(t, "foo", Payload{"index": "1"}),
		)
		require.Error(t, err)
		require.True(
			t,
			errors.Is(err, eventlog.ErrMismatchingVersions),
			"unexpected error: (%T) %s", err, err.Error(),
		)
		require.Zero(t, v2)
		require.Zero(t, tm)

		events, err := scan(l, l.VersionInitial(), false, 0)
		require.NoError(t, err)

		require.Len(t, events, 1)
	})
}

// TestScanEmptyLog assumes ErrInvalidVersion to be returned
// when reading at version 0 on an empty event log
func TestScanEmptyLog(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		fv := l.VersionInitial()
		require.Zero(t, fv)

		check := func() {
			events, err := scan(l, fv, false, 0)
			require.Error(t, err)
			require.True(
				t,
				errors.Is(err, eventlog.ErrInvalidVersion),
				"unexpected error: (%T) %s", err, err.Error(),
			)
			require.Len(t, events, 0)
		}

		check()

		_, _, _, err := l.Append(
			MakeEvent(t, "foo", Payload{"index": "0"}),
		)
		require.NoError(t, err)

		check()
	})
}

// TestScanVersionOutOfBound assumes ErrInvalidVersion to be returned
// when reading with a version that's >= the length of the log
func TestScanVersionOutOfBound(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		// Append first event
		_, v, _, err := l.Append(
			MakeEvent(t, "foo", Payload{"index": "0"}),
		)
		require.NoError(t, err)

		events, err := scan(l, v+1, false, 0)
		require.Error(t, err)
		require.True(
			t,
			errors.Is(err, eventlog.ErrInvalidVersion),
			"unexpected error: (%T) %s", err, err.Error(),
		)

		require.Len(t, events, 0)
	})
}

func TestScanReturnErr(t *testing.T) {
	test(t, func(t *testing.T, s Setup) {
		l := s.EventLog

		_, _, _, err := l.Append(MakeEvent(t, "foo", Payload{"i": 1}))
		require.NoError(t, err)
		_, _, _, err = l.Append(MakeEvent(t, "bar", Payload{"i": 2}))
		require.NoError(t, err)
		_, _, _, err = l.Append(MakeEvent(t, "baz", Payload{"i": 3}))
		require.NoError(t, err)

		testErr := errors.New("test error")

		var counter int
		err = l.Scan(l.VersionInitial(), false, func(eventlog.Event) error {
			counter++
			return testErr
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, testErr))
		require.Equal(t, 1, counter)
	})
}

type Setup struct {
	// Metadata holds the metadata that was used during log creation
	Metadata map[string]string

	EventLog *eventlog.EventLog
}

// test performes the given test function on all event log implementation
func test(t *testing.T, fn func(*testing.T, Setup)) {
	meta := func() map[string]string {
		return map[string]string{"name": "testlog"}
	}
	t.Run("Inmem", func(t *testing.T) {
		l := eventlog.New(inmem.New(logfile.MaxPayloadLen, meta()))
		t.Cleanup(func() {
			if err := l.Close(); err != nil {
				panic(fmt.Errorf("closing eventlog: %s", err))
			}
		})
		fn(t, Setup{
			Metadata: meta(),
			EventLog: l,
		})
	})

	t.Run("File", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), fmt.Sprintf(
			"test_%s_%s",
			strings.ReplaceAll(t.Name(), "/", "_"),
			time.Now().Format("2006_01_02T15_04_05_999999999Z07_00"),
		))

		err := logfile.Create(filePath, meta(), 0777)
		require.NoError(t, err)

		e, err := logfile.Open(filePath)
		require.NoError(t, err)
		defer e.Close()
		require.NotNil(t, e)

		fn(t, Setup{
			Metadata: meta(),
			EventLog: eventlog.New(e),
		})
	})
}

func scan(
	l *eventlog.EventLog,
	version uint64,
	reverse bool,
	limit int,
) ([]Event, error) {
	var events []Event
	i := 0
	errAbort := errors.New("abort")
	err := l.Scan(version, reverse, func(e eventlog.Event) error {
		var data map[string]interface{}
		if err := json.Unmarshal(e.PayloadJSON, &data); err != nil {
			return fmt.Errorf("unexpected error: %w", err)
		}

		events = append(events, Event{
			Version:         e.Version,
			VersionPrevious: e.VersionPrevious,
			VersionNext:     e.VersionNext,
			Time:            time.Unix(int64(e.Timestamp), 0),
			Label:           string(e.Label),
			Payload:         data,
		})

		i++
		if limit > 0 && i >= limit {
			return errAbort
		}
		return nil
	})

	if err == errAbort {
		err = nil
	}

	return events, err
}

const timeTolerance = 1 * time.Second

type Event struct {
	Version         uint64
	VersionPrevious uint64
	VersionNext     uint64
	Time            time.Time
	Label           string
	Payload         map[string]interface{}
}

type Payload map[string]interface{}

func MakeEvent(t *testing.T, label string, p Payload) eventlog.EventData {
	b, err := json.Marshal(p)
	require.NoError(t, err)
	return eventlog.EventData{Label: []byte(label), PayloadJSON: b}
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
			s := make([]byte, file.MaxLabelLen)
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
