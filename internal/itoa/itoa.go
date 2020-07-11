package itoa

import (
	"io"
)

// lookup tables
var lut = []byte(
	"00010203040506070809" +
		"10111213141516171819" +
		"20212223242526272829" +
		"30313233343536373839" +
		"40414243444546474849" +
		"50515253545556575859" +
		"60616263646566676869" +
		"70717273747576777879" +
		"80818283848586878889" +
		"90919293949596979899",
)
var lutDigit = []byte("0123456789")

// U32toa writes the given unsigned 32-bit integer to the given writer
func U32toa(s io.Writer, n uint32) error {
	if n == 0 {
		if _, err := s.Write(lut[0:1]); err != nil {
			return err
		}
		return nil
	}
	if n < 10000 {
		var d1 uint32 = (n / 100) << 1
		var d2 uint32 = (n % 100) << 1

		if n >= 1000 {
			if _, err := s.Write(lut[d1 : d1+1]); err != nil {
				return err
			}
		}
		if n >= 100 {
			if _, err := s.Write(lut[d1+1 : d1+2]); err != nil {
				return err
			}
		}
		if n >= 10 {
			if _, err := s.Write(lut[d2 : d2+1]); err != nil {
				return err
			}
		}
		if _, err := s.Write(lut[d2+1 : d2+2]); err != nil {
			return err
		}
	} else if n < 100000000 {
		// n = bbbbcccc
		var b uint32 = n / 10000
		var c uint32 = n % 10000

		var d1 uint32 = (b / 100) << 1
		var d2 uint32 = (b % 100) << 1

		var d3 uint32 = (c / 100) << 1
		var d4 uint32 = (c % 100) << 1

		if n >= 10000000 {
			if _, err := s.Write(lut[d1 : d1+1]); err != nil {
				return err
			}
		}
		if n >= 1000000 {
			if _, err := s.Write(lut[d1+1 : d1+2]); err != nil {
				return err
			}
		}
		if n >= 100000 {
			if _, err := s.Write(lut[d2 : d2+1]); err != nil {
				return err
			}
		}
		if _, err := s.Write(lut[d2+1 : d2+2]); err != nil {
			return err
		}

		if _, err := s.Write(lut[d3 : d3+1]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d3+1 : d3+2]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d4 : d4+1]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d4+1 : d4+2]); err != nil {
			return err
		}
	} else {
		// n = aabbbbcccc in decimal
		var a uint32 = n / 100000000 // 1 to 42
		n %= 100000000

		if a >= 10 {
			var i uint32 = a << 1
			if _, err := s.Write(lut[i : i+1]); err != nil {
				return err
			}
			if _, err := s.Write(lut[i+1 : i+2]); err != nil {
				return err
			}
		} else {
			if _, err := s.Write(lutDigit[a : a+1]); err != nil {
				return err
			}
		}

		var b uint32 = n / 10000 // 0 to 9999
		var c uint32 = n % 10000 // 0 to 9999

		var d1 uint32 = (b / 100) << 1
		var d2 uint32 = (b % 100) << 1

		var d3 uint32 = (c / 100) << 1
		var d4 uint32 = (c % 100) << 1

		if _, err := s.Write(lut[d1 : d1+1]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d1+1 : d1+2]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d2 : d2+1]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d2+1 : d2+2]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d3 : d3+1]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d3+1 : d3+2]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d4 : d4+1]); err != nil {
			return err
		}
		if _, err := s.Write(lut[d4+1 : d4+2]); err != nil {
			return err
		}
	}
	return nil
}
