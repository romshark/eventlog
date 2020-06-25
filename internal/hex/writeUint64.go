package hex

import (
	"github.com/valyala/fasthttp"
)

// WriteUint64 writes the given integer as short-lower-hex
// directly to the given request context to avoid indirect calls which
// not only incur overhead but also cause unnecessary allocations
// due to escape analysis
func WriteUint64(ctx *fasthttp.RequestCtx, num uint64) (int, error) {
	if num == 0 {
		return ctx.Write(lookupTable[0:1])
	}
	s := make([]byte, 16)
	x := num
	i := 14
	for ; i >= 0; i -= 2 {
		pos := (x & 0xFF) * 2
		ch := lookupTable[pos]
		s[i] = ch

		ch = lookupTable[pos+1]
		s[i+1] = ch
		x >>= 8
	}
	for i = 0; i < 16; i++ {
		if s[i] != '0' {
			break
		}
	}
	return ctx.Write(s[i:])
}

var lookupTable = []byte(
	"000102030405060708090a0b0c0d0e0f" +
		"101112131415161718191a1b1c1d1e1f" +
		"202122232425262728292a2b2c2d2e2f" +
		"303132333435363738393a3b3c3d3e3f" +
		"404142434445464748494a4b4c4d4e4f" +
		"505152535455565758595a5b5c5d5e5f" +
		"606162636465666768696a6b6c6d6e6f" +
		"707172737475767778797a7b7c7d7e7f" +
		"808182838485868788898a8b8c8d8e8f" +
		"909192939495969798999a9b9c9d9e9f" +
		"a0a1a2a3a4a5a6a7a8a9aaabacadaeaf" +
		"b0b1b2b3b4b5b6b7b8b9babbbcbdbebf" +
		"c0c1c2c3c4c5c6c7c8c9cacbcccdcecf" +
		"d0d1d2d3d4d5d6d7d8d9dadbdcdddedf" +
		"e0e1e2e3e4e5e6e7e8e9eaebecedeeef" +
		"f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
)
