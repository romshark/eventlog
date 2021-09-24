package jsonminify_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/romshark/eventlog/internal/jsonminify"

	"github.com/stretchr/testify/require"
	tdewolff "github.com/tdewolff/minify"
	tdewolffjson "github.com/tdewolff/minify/json"
)

func Test(t *testing.T) {
	for _, t1 := range []struct {
		impl string
		fn   func([]byte) ([]byte, error)
	}{
		{"jsonminifyMinify", func(in []byte) ([]byte, error) {
			return jsonminify.Minify(in), nil
		}},
		{"tdewolffMinify", func(in []byte) ([]byte, error) {
			m := tdewolff.New()
			m.AddFunc("application/json", tdewolffjson.Minify)
			r := bytes.NewReader(in)
			w := new(bytes.Buffer)
			w.Grow(len(in))
			if err := m.Minify("application/json", w, r); err != nil {
				return nil, err
			}
			return w.Bytes(), nil
		}},
	} {
		t.Run(t1.impl, func(t *testing.T) {
			for _, data := range testdata() {
				t.Run(data.name, func(t *testing.T) {
					result, err := t1.fn([]byte(data.in))
					require.NoError(t, err)
					require.Equal(t, data.expect, string(result))
				})
			}
		})
	}
}

var benchOut []byte

func BenchmarkMinifyJSON(b *testing.B) {
	for _, data := range testdata() {
		in := []byte(data.in)
		b.Run(data.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchOut = jsonminify.Minify(in)
			}
		})
	}
}

func BenchmarkTdewolffMinify(b *testing.B) {
	m := tdewolff.New()
	m.AddFunc("application/json", tdewolffjson.Minify)

	for _, data := range testdata() {
		in := []byte(data.in)
		b.Run(data.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := bytes.NewReader(in)
				w := new(bytes.Buffer)
				w.Grow(len(in))
				if err := m.Minify("application/json", w, r); err != nil {
					panic(err)
				}
				benchOut = w.Bytes()
			}
		})
	}
}

type D struct {
	name   string
	in     string
	expect string
}

func testdata() (testdata []D) {
	testdata = make([]D, 0, 3)
	for _, n := range []string{
		"tiny",
		"small",
		"1mb",
	} {
		fn := "test_" + n + ".json"
		in, err := ioutil.ReadFile(fn)
		if err != nil {
			panic(fmt.Errorf("reading file %q: %w", fn, err))
		}

		if !json.Valid(in) {
			panic(fmt.Errorf("invalid JSON in file %q", fn))
		}

		fn = "test_" + n + "_min.json"
		min, err := ioutil.ReadFile(fn)
		if err != nil {
			panic(fmt.Errorf("reading file %q: %w", fn, err))
		}

		if !json.Valid(min) {
			panic(fmt.Errorf("invalid JSON in file %q", fn))
		}

		testdata = append(testdata, D{
			name:   n,
			in:     string(in),
			expect: string(min),
		})
	}
	return
}
