package eventlog

import "github.com/romshark/eventlog/internal/consts"

func ValidateLabel(label string) error {
	if len(label) > consts.MaxLabelLen {
		return ErrLabelTooLong
	}
	for _, c := range label {
		switch c {
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'_', '-', '.', '~', '%':
		default:
			return ErrLabelContainsIllegalChars
		}
	}
	return nil
}
