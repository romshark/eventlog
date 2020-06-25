package jsonminify

func Minify(in []byte) []byte {
	i := 0
	inString := false
	for t := 0; t < len(in); t++ {
		switch in[t] {
		case ' ':
			if !inString {
				continue
			}
		case '\n', '\t', '\r':
			continue
		case '"':
			if !inString {
				inString = true
			} else if in[t-1] != '\\' {
				inString = false
			}
		}
		in[i] = in[t]
		i++
	}
	return in[:i]
}
