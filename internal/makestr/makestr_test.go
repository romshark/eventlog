package makestr_test

import (
	"testing"

	"github.com/romshark/eventlog/internal/makestr"
	"github.com/stretchr/testify/require"
)

func TestMakeJSON(t *testing.T) {
	for _, tt := range []struct {
		ln  int
		exp string
	}{
		{0, `{"k":""}`},
		{1, `{"k":"x"}`},
		{9, `{"k":"x"}`},
		{10, `{"k":"xx"}`},
		{15, `{"k":"xxxxxxx"}`},
	} {
		t.Run("", func(t *testing.T) {
			a := makestr.MakeJSON(tt.ln)
			require.Equal(t, tt.exp, a)
		})
	}
}
