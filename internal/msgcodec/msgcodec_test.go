package msgcodec_test

import (
	"testing"

	"github.com/romshark/eventlog/eventlog"
	bin "github.com/romshark/eventlog/internal/bin"
	"github.com/romshark/eventlog/internal/makestr"
	"github.com/romshark/eventlog/internal/msgcodec"
	"github.com/stretchr/testify/require"
)

func TestCodecBinary(t *testing.T) {
	type Event struct {
		Label       string
		PayloadJSON string
	}

	for _, t1 := range []struct {
		name     string
		expected []Event
	}{
		{"single no label", []Event{
			{"", `{"x":0}`},
		}},
		{"single", []Event{
			{"x", `{"x":0}`},
		}},
		{"single max label", []Event{
			{makestr.Make(65535, 'a'), `{"x":0}`},
		}},
		{"multiple", []Event{
			{"foo", `{"foo":"bar"}`},
			{"", `{"x":0}`},
			{makestr.Make(65535, 'a'), `{"baz":"faz","maz":42}`},
		}},
	} {
		t.Run(t1.name, func(t *testing.T) {
			in := make([]eventlog.Event, len(t1.expected))
			for i, e := range t1.expected {
				in[i] = eventlog.Event{
					Label:       e.Label,
					PayloadJSON: []byte(e.PayloadJSON),
				}
			}

			encoded, err := msgcodec.EncodeBinary(in...)
			require.NoError(t, err)
			require.NotNil(t, encoded)

			actual := []Event{}
			require.NoError(t, msgcodec.ScanBytesBinary(
				encoded,
				func(n int) error {
					require.Equal(t, len(t1.expected), n)
					actual = make([]Event, 0, n)
					return nil
				},
				func(label []byte, payloadJSON []byte) error {
					actual = append(actual, Event{
						Label:       string(label),
						PayloadJSON: string(payloadJSON),
					})
					return nil
				},
			))

			require.Equal(t, t1.expected, actual)
		})
	}
}

func TestScanBytesBinaryErr(t *testing.T) {
	validMinJson := `{"x":0}`
	for _, t1 := range []struct {
		name                  string
		in                    []byte
		expectOnNumInvokation bool
	}{
		{"payload too long", bin.Compose(
			uint16(len("x")),          // Label len
			uint32(len(validMinJson)), // Payload len
			"x",                       // Label
			validMinJson,              // Payload
			byte(0x10),                // Extra byte (illegal)
		), false},
		{"payload too long", bin.Compose(
			uint16(0),                 // Label len
			uint32(len(validMinJson)), // Payload len
			validMinJson,              // Payload
			byte(0x10),                // Extra byte (illegal)
		), false},
		{"payload too short", bin.Compose(
			uint16(1),                 // Label len
			uint32(len(validMinJson)), // Payload len
			"x",                       // Label
			`{"x":0`,                  // Payload (too short)
		), true},
		{"missing label", bin.Compose(
			uint16(1),                 // Label len
			uint32(len(validMinJson)), // Payload len
			/* Missing label */
			validMinJson, // Payload
		), true},
		{"label too short", bin.Compose(
			uint16(2),                 // Label len
			uint32(len(validMinJson)), // Payload len
			"x",                       // Label (too short)
			validMinJson,              // Payload
		), true},
	} {
		t.Run(t1.name, func(t *testing.T) {
			err := msgcodec.ScanBytesBinary(
				t1.in,
				func(int) error {
					if !t1.expectOnNumInvokation {
						panic("unexpected invokation")
					}
					return nil
				},
				func(label []byte, payload []byte) error {
					panic("unexpected invokation")
				},
			)
			require.Error(t, err)
		})
	}
}
