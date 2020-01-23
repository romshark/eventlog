package file

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"
)

const (
	// SupportedProtoVersion defines the supported protocol version
	SupportedProtoVersion = 1

	fileHeaderLen  = 12
	entryHeaderLen = 16
)

// Make sure *File implements EventLog
var _ eventlog.EventLog = new(File)

// File is a persistent file-based event log
type File struct {
	filePath   string
	lock       sync.RWMutex
	file       *os.File
	buf        []byte
	index      []uint64
	tailOffset uint64
}

// NewFile returns a new persistent file-based event log instance
func NewFile(filePath string) (*File, error) {
	newFile := false

	info, err := os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) ||
		err == nil && !info.IsDir() && info.Size() < 1 {
		newFile = true
	} else if err != nil {
		return nil, err
	}

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
		buf:        make([]byte, 1024),
	}

	if err := f.init(newFile); err != nil {
		return nil, err
	}

	return f, nil
}

func (f *File) init(newFile bool) error {
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var (
		previousTime int64
		payloadLen   uint64

		uint64Buf = f.buf[:8]
		uint32Buf = f.buf[:4]
	)

	if newFile {
		// Initialize new file

		// Write protocol version (4 bytes)
		binary.LittleEndian.PutUint32(uint32Buf, SupportedProtoVersion)
		if _, err := f.file.WriteAt(uint32Buf, 0); err != nil {
			return err
		}

		// Write log length (8 bytes)
		binary.LittleEndian.PutUint64(uint64Buf, 0)
		if _, err := f.file.WriteAt(uint64Buf, 4); err != nil {
			return err
		}

		if err := f.file.Sync(); err != nil {
			return err
		}
		if _, err := f.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}

	rd := bufio.NewReader(f.file)

	// Read proto version
	if _, err := io.ReadFull(rd, uint32Buf); err != nil {
		return errors.New(
			"malformed file, expected version (4) at offset 0",
		)
	}

	fileVersion := binary.LittleEndian.Uint32(uint32Buf)

	// Check version
	if SupportedProtoVersion != fileVersion {
		return fmt.Errorf(
			"unsupported proto version: %d (supported: %d)",
			fileVersion,
			SupportedProtoVersion,
		)
	}

	// Read log length
	if _, err := io.ReadFull(rd, uint64Buf); err != nil {
		return errors.New(
			"malformed file, expected log length (8) at offset 4",
		)
	}
	expectedLength := binary.LittleEndian.Uint64(uint64Buf)

	// Preallocate index table
	f.index = make([]uint64, 0, expectedLength)

	// Read file
READ_LOOP:
	for {
		// Read timestamp (8 bytes)
		_, err := io.ReadFull(rd, uint64Buf)
		switch {
		case errors.Is(err, io.EOF):
			break READ_LOOP
		case errors.Is(err, io.ErrUnexpectedEOF):
			return fmt.Errorf(
				"malformed file, expected timestamp (8) at offset %d",
				f.tailOffset,
			)
		}
		timestamp := int64(binary.LittleEndian.Uint64(uint64Buf))

		// Validate and set time
		if f.tailOffset > fileHeaderLen && timestamp < previousTime {
			return fmt.Errorf(
				"malformed file, invalid order, "+
					"time (%s) smaller than previous event (%s) on offset %d",
				time.Unix(timestamp, 0),
				time.Unix(previousTime, 0),
				f.tailOffset+8,
			)
		}
		previousTime = timestamp

		// Read payload len (8 bytes)
		if _, err := io.ReadFull(rd, uint64Buf); err != nil {
			return fmt.Errorf(
				"malformed file, expected payload length (8) at offset %d",
				f.tailOffset+8,
			)
		}
		payloadLen = binary.LittleEndian.Uint64(uint64Buf)

		// Validate payload length
		if payloadLen < 1 {
			return fmt.Errorf(
				"malformed file, invalid payload length (%d) at offset %d",
				payloadLen,
				f.tailOffset+8,
			)
		}

		// Adjust read buffer, grow if necessary
		if payloadLen > uint64(len(f.buf)) {
			f.buf = append(
				f.buf,
				make([]byte, payloadLen-uint64(len(f.buf)))...,
			)
			uint64Buf = f.buf[:8]
			uint32Buf = f.buf[:4]
		}

		// Read payload
		pldBuf := f.buf[:payloadLen]
		if _, err = io.ReadFull(rd, pldBuf); err != nil {
			return fmt.Errorf(
				"malformed file, expected payload (%d) at offset %d",
				payloadLen,
				f.tailOffset+entryHeaderLen,
			)
		}

		// Validate payload JSON
		var payload map[string]interface{}
		if err := json.Unmarshal(pldBuf, &payload); err != nil {
			return fmt.Errorf(
				"malformed file, malformed payload: %w at offset %d",
				err,
				f.tailOffset+entryHeaderLen,
			)
		}

		f.index = append(f.index, f.tailOffset)
		f.tailOffset += entryHeaderLen + payloadLen
	}

	actualLen := uint64(len(f.index))
	if actualLen != expectedLength {
		// Fix corrupted log length in header
		if err := f.setHeadLen(actualLen); err != nil {
			return fmt.Errorf("fixing corrupted length header: %w", err)
		}
		return f.file.Sync()
	}

	return nil
}

// setHeadLen updates the length header in the file to the given value
func (f *File) setHeadLen(length uint64) error {
	if _, err := f.file.Seek(4, io.SeekStart); err != nil {
		return err
	}
	uint64Buf := f.buf[:8]
	binary.LittleEndian.PutUint64(uint64Buf, length)
	if n, err := f.file.WriteAt(uint64Buf, 4); err != nil || n != 8 {
		return err
	}
	return nil
}

// read returns io.EOF when there's nothing more to read
//
// WARNING: pl []byte references the read buffer f.buf
func (f *File) read(offsetByte uint64) (tm uint64, pl []byte, err error) {
	var n int

	if _, err = f.file.Seek(int64(offsetByte), 0); err != nil {
		err = fmt.Errorf(
			"seeking file at offset %d: %w",
			offsetByte,
			err,
		)
		return
	}

	uint64Buf := f.buf[:8]

	// Read timestamp
	n, err = f.file.Read(uint64Buf)
	if errors.Is(err, io.EOF) {
		return
	} else if err != nil || n != 8 {
		err = fmt.Errorf(
			"reading timestamp at offset %d: %w",
			offsetByte,
			err,
		)
		return
	}

	tm = binary.LittleEndian.Uint64(uint64Buf)

	// Read payload len
	if n, err = f.file.Read(uint64Buf); err != nil || n != 8 {
		err = fmt.Errorf(
			"reading payload length at offset %d: %w",
			offsetByte+8,
			err,
		)
		return
	}

	// Adjust read buffer, grow if necessary
	payloadLen := binary.LittleEndian.Uint64(uint64Buf)
	if payloadLen > uint64(len(f.buf)) {
		growBy := payloadLen - uint64(len(f.buf))
		f.buf = append(f.buf, make([]byte, growBy)...)
		uint64Buf = f.buf[:8]
	}

	// Read payload
	pl = f.buf[:payloadLen]
	if n, err = f.file.Read(pl); err != nil || n != len(pl) {
		err = fmt.Errorf(
			"reading payload at offset %d: %w",
			offsetByte+8,
			err,
		)
		return
	}
	return
}

// Close closes the file
func (f *File) Close() error {
	return f.file.Close()
}

// Version implements EventLog.Version
func (f *File) Version() uint64 {
	f.lock.RLock()
	v := len(f.index)
	f.lock.RUnlock()
	return uint64(v)
}

// Scan reads a maximum of n events starting at the given offset.
// If offset+n exceeds the length of the log then a smaller number
// of events is returned. If n is 0 then all events starting at the
// given offset are returned
func (f *File) Scan(offsetIndex uint64, n uint64, fn eventlog.ScanFn) error {
	// TODO: Replace sync.Mutex with sync.RWMutex to improve read performance
	// by allowing multiple readers to read simultaneously
	defer f.lock.Unlock()
	f.lock.Lock()

	ln := uint64(len(f.index))

	if offsetIndex >= ln {
		return eventlog.ErrOffsetOutOfBound
	}

	if n > 0 {
		// Limited scan
		if offsetIndex+n > ln {
			n -= offsetIndex + n - ln
		}

		for i := uint64(0); i < n; i++ {
			tm, pl, err := f.read(f.index[offsetIndex])
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			fn(tm, pl)
			offsetIndex++
		}
		return nil
	}

	// Unlimited scan
	for {
		if offsetIndex >= uint64(len(f.index)) {
			break
		}
		tm, pl, err := f.read(f.index[offsetIndex])
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		fn(tm, pl)
		offsetIndex++
	}
	return nil
}

// Append appends a new entry onto the event log
func (f *File) Append(payload []byte) error {
	if err := eventlog.VerifyPayload(payload); err != nil {
		return err
	}

	timestamp := uint64(time.Now().Unix())

	defer f.lock.Unlock()
	f.lock.Lock()

	return f.writeLog(timestamp, payload)
}

// AppendCheck appends a new entry onto the event log
// expecting offset to be the offset of the last entry plus 1
func (f *File) AppendCheck(
	offset uint64,
	payload []byte,
) error {
	if err := eventlog.VerifyPayload(payload); err != nil {
		return err
	}

	timestamp := uint64(time.Now().Unix())

	defer f.lock.Unlock()
	f.lock.Lock()

	if offset != uint64(len(f.index)) {
		return eventlog.ErrMismatchingVersions
	}

	return f.writeLog(timestamp, payload)
}

func (f *File) writeLog(timestamp uint64, payload []byte) error {
	uint64Buf := f.buf[:8]

	// Write timestamp (8 bytes)
	binary.LittleEndian.PutUint64(uint64Buf, timestamp)
	if n, err := f.file.WriteAt(
		uint64Buf,
		int64(f.tailOffset),
	); err != nil || n != 8 {
		return err
	}

	// Write payload length (8 bytes)
	pldLen := uint64(len(payload))

	binary.LittleEndian.PutUint64(uint64Buf, pldLen)
	if n, err := f.file.WriteAt(
		uint64Buf,
		int64(f.tailOffset+8),
	); err != nil || n != 8 {
		return err
	}

	// Write payload
	if n, err := f.file.WriteAt(
		payload,
		int64(f.tailOffset+entryHeaderLen),
	); err != nil || n != len(payload) {
		return err
	}

	// Update index
	f.index = append(f.index, f.tailOffset)
	f.tailOffset += entryHeaderLen + pldLen

	if err := f.file.Sync(); err != nil {
		return err
	}

	// Update length header
	_ = f.setHeadLen(uint64(len(f.index)))

	return nil
}
