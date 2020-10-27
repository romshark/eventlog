package consts

// Internal constants
var (
	MaxLabelLen = 65535

	StatusMsgErrInvalidPayload         = []byte("ErrInvalidPayload")
	StatusMsgErrOffsetOutOfBound       = []byte("ErrOffsetOutOfBound")
	StatusMsgErrInvalidTypeName        = []byte("ErrInvalidTypeName")
	StatusMsgErrMismatchingVersions    = []byte("ErrMismatchingVersions")
	StatusMsgErrInvalidOffset          = []byte("ErrInvalidOffset")
	StatusMsgErrInvalidVersion         = []byte("ErrInvalidVersion")
	StatusMsgErrUnsupportedContentType = []byte("ErrUnsupportedContentType")
)
