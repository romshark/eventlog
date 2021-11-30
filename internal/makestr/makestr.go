package makestr

import "bytes"

func Make(length int, c byte) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

// MakeJSON makes a JSON document consisting of a string key-value pair
// where the value is filled with 'x'.
func MakeJSON(length int) string {
	if w := len(`{"k":"`) + len(`"}`); length > w {
		// Take the document wrapping into account
		length -= w
	}

	var b bytes.Buffer
	b.Grow(len(`{"k":"`) + len(`"}`))
	_, _ = b.WriteString(`{"k":"`)
	for i := 0; i < length; i++ {
		_ = b.WriteByte('x')
	}
	_, _ = b.WriteString(`"}`)
	return b.String()
}
