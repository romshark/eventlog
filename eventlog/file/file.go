package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/bufpool"
)

const (
	// SupportedProtoVersion defines the supported protocol version
	SupportedProtoVersion = 2

	fileHeaderLen  = 4
	entryHeaderLen = 16

	maxPayloadLen         = 1024 * 1024 // 1 MiB
	minPossiblePayloadLen = 7           // {"x":0}
	minEntryLen           = entryHeaderLen + minPossiblePayloadLen

	entryInitSym = '\n'
)

// Error types
var (
	ErrPayloadExceedLimit   = errors.New("payload exceeded limit")
	ErrInvalidPayloadLength = errors.New("invalid payload length")
)

// Make sure *File implements EventLog
var _ eventlog.EventLog = new(File)

// File is a persistent file-based event log
type File struct {
	filePath   string
	lock       sync.RWMutex
	file       *os.File
	bufPool    *bufpool.Pool
	tailOffset uint64
}

// NewFile returns a new persistent file-based event log instance
func NewFile(filePath string) (*File, error) {
	file, err := os.OpenFile(
		filePath,
		os.O_CREATE|os.O_RDWR|os.O_SYNC,
		0664,
	)
	if err != nil {
		return nil, err
	}

	f := &File{
		filePath:   filePath,
		file:       file,
		tailOffset: fileHeaderLen,
	}
	f.bufPool = bufpool.NewPool(maxPayloadLen)

	b := f.bufPool.Get()
	defer b.Release()
	buf := b.Bytes()

	if err := f.checkVersion(buf); errors.Is(err, io.EOF) {
		// New file, write header
		bufUint32 := buf[:4]
		binary.LittleEndian.PutUint32(bufUint32, SupportedProtoVersion)
		if _, err := f.file.Write(bufUint32); err != nil {
			return nil, fmt.Errorf(
				"writing header version: %w",
				err,
			)
		}
	} else if err != nil {
		return nil, err
	}

	return f, nil
}

func (f *File) checkVersion(buf []byte) error {
	bufUint32 := buf[:4]
	if _, err := f.file.ReadAt(bufUint32, 0); err != nil {
		return err
	}

	actual := binary.LittleEndian.Uint32(buf)
	if SupportedProtoVersion != actual {
		return fmt.Errorf(
			"unsupported file version (%d)",
			actual,
		)
	}

	return nil
}

// read returns io.EOF when there's nothing more to read
//
// WARNING: pl []byte references the given buffer
func (f *File) read(
	buf []byte,
	offset int64,
) (
	bytesRead int64,
	tm uint64,
	pl []byte,
	err error,
) {
	var n int

	buf8 := buf[:8]
	buf4 := buf[:4]
	buf1 := buf[:1]

	// Read entry initiation symbol
	if _, err = f.file.ReadAt(buf1, offset); errors.Is(err, io.EOF) {
		return
	} else if err != nil {
		err = fmt.Errorf(
			"reading entry initiation symbol at offset %d: %w",
			offset,
			err,
		)
		return
	}
	if buf1[0] != entryInitSym {
		err = fmt.Errorf(
			"expected entry initiation symbol at offset %d, got %v",
			offset,
			buf1[0],
		)
		return
	}
	bytesRead++

	// Read timestamp
	n, err = f.file.ReadAt(buf8, offset+1)
	if err != nil {
		err = fmt.Errorf(
			"reading timestamp at offset %d: %w",
			offset+1,
			err,
		)
		return
	}
	if n != 8 {
		err = fmt.Errorf(
			"reading timestamp (expected: 8; read: %d)",
			n,
		)
		return
	}
	bytesRead += 8

	tm = binary.LittleEndian.Uint64(buf8)

	// Read payload len
	n, err = f.file.ReadAt(buf4, offset+9)
	if err != nil {
		err = fmt.Errorf(
			"reading payload length at offset %d: %w",
			offset+9,
			err,
		)
		return
	}
	if n != 4 {
		err = fmt.Errorf(
			"reading payload length (expected: 4; read: %d)",
			n,
		)
		return
	}
	bytesRead += 4

	// Check payload length
	payloadLen := binary.LittleEndian.Uint32(buf4)
	switch {
	case payloadLen < minPossiblePayloadLen:
		err = ErrInvalidPayloadLength
		return
	case payloadLen > maxPayloadLen:
		err = ErrPayloadExceedLimit
		return
	}

	// Read & check terminator (if not last entry)
	buf2 := buf[:2]
	termSeqOffset := offset + bytesRead + int64(payloadLen) - 1
	if uint64(termSeqOffset)+1 < f.tailOffset {
		// Not last entry
		n, err = f.file.ReadAt(buf2, termSeqOffset)
		if err != nil {
			err = fmt.Errorf(
				"reading next payload terminator (-1) at offset %d: %w",
				offset+int64(payloadLen)-1,
				err,
			)
			return
		}
		if n != 2 {
			err = fmt.Errorf(
				"reading next payload terminator (expected: 2; read: %d)",
				n,
			)
			return
		}
		if buf2[0] != '}' || buf2[1] != entryInitSym {
			err = fmt.Errorf(
				"unexpected termination sequence: %s",
				buf2,
			)
			return
		}
	}

	// Read payload
	pl = buf[:payloadLen]
	n, err = f.file.ReadAt(pl, offset+13)
	if err != nil {
		err = fmt.Errorf(
			"reading payload at offset %d: %w",
			offset+13,
			err,
		)
		return
	}
	if n != len(pl) {
		err = fmt.Errorf(
			"reading payload length (expected: %d; read: %d)",
			len(pl),
			n,
		)
		return
	}
	bytesRead += int64(payloadLen)

	return
}

// Close closes the file
func (f *File) Close() error {
	return f.file.Close()
}

// Version implements EventLog.Version
func (f *File) Version() uint64 {
	f.lock.RLock()
	v := f.tailOffset
	f.lock.RUnlock()
	return uint64(v)
}

// FirstOffset implements EventLog.FirstOffset
func (f *File) FirstOffset() uint64 { return fileHeaderLen }

// Scan reads a maximum of n events starting at the given offset.
// If offset+n exceeds the length of the log then a smaller number
// of events is returned. If n is 0 then all events starting at the
// given offset are returned
func (f *File) Scan(
	offset uint64,
	n uint64,
	fn eventlog.ScanFn,
) (
	nextOffset uint64,
	err error,
) {
	b := f.bufPool.Get()
	defer b.Release()
	buf := b.Bytes()

	f.lock.RLock()
	defer f.lock.RUnlock()

	if offset >= f.tailOffset || f.tailOffset-offset < minEntryLen {
		return 0, eventlog.ErrOffsetOutOfBound
	}

	var i int64
	if n > 0 {
		// Limited scan
		if offset+n > f.tailOffset {
			n -= offset + n - f.tailOffset
		}

		var r uint64
		for i, r = int64(offset), uint64(0); r < n; r++ {
			n, tm, pl, err := f.read(buf, i)
			if err == io.EOF {
				break
			} else if err != nil {
				return 0, err
			}
			i += int64(n)
			if err := fn(tm, pl, uint64(i)); err != nil {
				return 0, err
			}
		}
	} else {
		// Unlimited scan
		for i = int64(offset); ; {
			if offset >= f.tailOffset {
				break
			}
			n, tm, pl, err := f.read(buf, i)
			if err == io.EOF {
				break
			} else if err != nil {
				return 0, err
			}
			i += int64(n)
			if err := fn(tm, pl, uint64(i)); err != nil {
				return 0, err
			}
		}
	}

	if uint64(i) < f.tailOffset {
		nextOffset = uint64(i)
	}
	return
}

// Append appends a new entry onto the event log
func (f *File) Append(payload []byte) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	if err = eventlog.VerifyPayload(payload); err != nil {
		return
	}

	tm = time.Now().UTC()
	timestamp := uint64(tm.Unix())

	buf := f.bufPool.Get()
	defer buf.Release()

	f.lock.Lock()
	defer f.lock.Unlock()

	offset = f.tailOffset
	_, err = f.writeLog(buf.Bytes(), timestamp, payload)
	newVersion = f.tailOffset
	return
}

// AppendCheck implements EventLog.AppendCheck
func (f *File) AppendCheck(
	assumedVersion uint64,
	payload []byte,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	if err = eventlog.VerifyPayload(payload); err != nil {
		return
	}

	tm = time.Now().UTC()
	timestamp := uint64(tm.Unix())

	buf := f.bufPool.Get()
	defer buf.Release()

	f.lock.Lock()
	defer f.lock.Unlock()

	if assumedVersion != f.tailOffset {
		tm = time.Time{}
		err = eventlog.ErrMismatchingVersions
		return
	}

	offset = f.tailOffset
	_, err = f.writeLog(buf.Bytes(), timestamp, payload)
	newVersion = f.tailOffset
	return
}

func (f *File) writeLog(
	buf []byte,
	timestamp uint64,
	payload []byte,
) (int64, error) {
	offset := int64(f.tailOffset)
	buf1 := buf[:1]

	// Write entry initiation symbol
	buf1[0] = entryInitSym
	if _, err := f.file.WriteAt(buf1, offset); err != nil {
		return 0, err
	}
	offset++

	buf8 := buf[:8]
	buf4 := buf[:4]

	// Write timestamp (8 bytes)
	binary.LittleEndian.PutUint64(buf8, timestamp)
	n, err := f.file.WriteAt(buf8, offset)
	if err != nil {
		return 1, err
	}
	if n != 8 {
		return 1, fmt.Errorf(
			"writing timestamp, unexpectedly wrote: %d",
			n,
		)
	}
	offset += 8

	// Write payload length (4 bytes)
	pldLen := uint32(len(payload))

	binary.LittleEndian.PutUint32(buf4, pldLen)
	n, err = f.file.WriteAt(buf4, offset)
	if err != nil {
		return 9, err
	}
	if n != 4 {
		return 9, fmt.Errorf(
			"writing payload length, unexpectedly wrote: %d",
			n,
		)
	}
	offset += 4

	// Write payload
	n, err = f.file.WriteAt(payload, offset)
	if err != nil {
		return 13, err
	}
	if n != len(payload) {
		return 13, fmt.Errorf(
			"writing payload, unexpectedly wrote: %d",
			n,
		)
	}
	offset += int64(len(payload))

	if err := f.file.Sync(); err != nil {
		return 13, err
	}

	wrote := offset - int64(f.tailOffset)
	f.tailOffset = uint64(offset)

	return wrote, nil
}
