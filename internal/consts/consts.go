package consts

// Internal constants
var (
	StatusMsgErrInvalidPayload      = []byte("ErrInvalidPayload")
	StatusMsgErrOffsetOutOfBound    = []byte("ErrOffsetOutOfBound")
	StatusMsgErrInvalidTypeName     = []byte("ErrInvalidTypeName")
	StatusMsgErrMismatchingVersions = []byte("ErrMismatchingVersions")
	StatusMsgErrInvalidOffset       = []byte("ErrInvalidOffset")
	StatusMsgErrInvalidVersion      = []byte("ErrInvalidVersion")
)

// JSONValidationTest returns the JSON validation test setup
func JSONValidationTest() map[string]bool {
	return map[string]bool{
		"":    false,
		"   ": false,
		" \r\n  \n  	 ": false,

		"{}":    false,
		"{   }": false,
		"{ \r\n  \n  	 }": false,

		"[]": false,

		`{"foo":"bar}`: false,
		`{foo:"bar"}`:  false,

		`{"foo":"bar"}`:       true,
		`{"ключ":"значение"}`: true,
	}
}
