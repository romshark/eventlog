package makestr

func Make(length int, c byte) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = c
	}
	return string(b)
}
