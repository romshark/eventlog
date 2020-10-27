package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Compose composes a binary message out of the given parts.
// All numbers are encoded in little-endian format.
// Supported data types are: []byte, uint16, uint32 and uint64.
//
// WARNING: Panics if either part is of unsupported type.
func Compose(parts ...interface{}) []byte {
	b := new(bytes.Buffer)
	var err error
	for _, p := range parts {
		switch v := p.(type) {
		case uint16:
			buf := make([]byte, 2)
			binary.LittleEndian.PutUint16(buf, v)
			_, err = b.Write(buf)
		case uint32:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, v)
			_, err = b.Write(buf)
		case uint64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, v)
			_, err = b.Write(buf)
		case string:
			_, err = b.WriteString(v)
		case []byte:
			_, err = b.Write(v)
		case byte:
			err = b.WriteByte(v)
		default:
			err = fmt.Errorf("unsupported part type: %T (%#v)", p, p)
		}
		if err != nil {
			panic(err)
		}
	}
	return b.Bytes()
}
