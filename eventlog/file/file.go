package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/bufpool"
)

const (
	// SupportedProtoVersion defines the supported protocol version
	SupportedProtoVersion = 2

	fileHeaderLen  = 4
	entryHeaderLen = 16

	MaxPayloadLen = 1024 * 1024 // 1 MiB
	MinPayloadLen = 7           // {"x":0}
	minEntryLen   = entryHeaderLen + MinPayloadLen
)

// Make sure *File implements eventlog.Implementer
var _ eventlog.Implementer = new(File)

// File is a persistent file-based event log
type File struct {
	filePath   string
	lock       sync.RWMutex
	file       *os.File
	bufPool    *bufpool.Pool
	tailOffset uint64
}

// New returns a new persistent file-based event log instance
func New(filePath string) (*File, error) {
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
	f.bufPool = bufpool.NewPool(MaxPayloadLen)

	b := f.bufPool.Get()
	defer b.Release()
	buf := b.Bytes()

	switch err := readHeader(f.file, buf); err {
	case io.EOF:
		if err := f.writeFileHeader(buf); err != nil {
			return nil, err
		}
	case nil:
	default:
		return nil, err
	}

	return f, nil
}

func (f *File) writeFileHeader(buf []byte) error {
	// New file, write header
	bufUint32 := buf[:4]
	binary.LittleEndian.PutUint32(bufUint32, SupportedProtoVersion)
	if _, err := f.file.Write(bufUint32); err != nil {
		return fmt.Errorf("writing header version: %w", err)
	}
	return nil
}

// read returns io.EOF when there's nothing more to read
//
// WARNING: pl []byte references the given buffer
func readEntry(
	reader reader,
	buf []byte,
	offset int64,
) (
	bytesRead int64,
	tm uint64,
	pl []byte,
	err error,
) {
	var n int
	var payloadHash uint64

	buf8 := buf[:8]
	buf4 := buf[:4]

	// Read timestamp (8 bytes)
	n, err = reader.ReadAt(buf8, offset)
	switch {
	case errors.Is(err, io.EOF):
		return
	case err != nil:
		err = fmt.Errorf(
			"reading timestamp at offset %d: %w",
			offset,
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

	// Read payload hash (8 bytes, xxhash64)
	n, err = reader.ReadAt(buf8, offset+8)
	if err != nil {
		err = fmt.Errorf(
			"reading payload hash at offset %d: %w",
			offset+8,
			err,
		)
		return
	}
	if n != 8 {
		err = fmt.Errorf(
			"reading payload hash (expected: 8; read: %d)",
			n,
		)
		return
	}
	bytesRead += 8
	payloadHash = binary.LittleEndian.Uint64(buf8)

	// Read payload len
	n, err = reader.ReadAt(buf4, offset+16)
	if err != nil {
		err = fmt.Errorf(
			"reading payload length at offset %d: %w",
			offset+16,
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
	if payloadLen < MinPayloadLen || payloadLen > MaxPayloadLen {
		// Invalid payload length indicates wrong offset
		err = eventlog.ErrInvalidOffset
		return
	}

	// Read payload
	pl = buf[:payloadLen]
	n, err = reader.ReadAt(pl, offset+20)
	if err != nil {
		err = fmt.Errorf(
			"reading payload at offset %d: %w",
			offset+20,
			err,
		)
		return
	}
	if n != len(pl) {
		err = fmt.Errorf(
			"reading payload (expected: %d; read: %d)",
			len(pl),
			n,
		)
		return
	}
	bytesRead += int64(payloadLen)

	// Verify payload integrity
	if payloadHash != xxhash.Sum64(pl) {
		err = eventlog.ErrInvalidOffset
		return
	}

	return
}

// Close closes the file
func (f *File) Close() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if err := f.file.Close(); err != nil {
		return err
	}
	f.file = nil
	return nil
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
			n, tm, pl, err := readEntry(f.file, buf, i)
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
			n, tm, pl, err := readEntry(f.file, buf, i)
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

	nextOffset = uint64(i)
	return
}

func (f *File) Append(payloadJSON []byte) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now().UTC()
	timestamp := uint64(tm.Unix())

	buf := f.bufPool.Get()
	defer buf.Release()

	f.lock.Lock()
	defer f.lock.Unlock()

	offset = f.tailOffset
	_, err = f.writeLog(buf.Bytes(), timestamp, payloadJSON)
	newVersion = f.tailOffset
	return
}

func (f *File) AppendMulti(payloadsJSON ...[]byte) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now().UTC()
	timestamp := uint64(tm.Unix())

	buf := f.bufPool.Get()
	defer buf.Release()

	f.lock.Lock()
	defer f.lock.Unlock()

	for _, p := range payloadsJSON {
		offset = f.tailOffset
		_, err = f.writeLog(buf.Bytes(), timestamp, p)
		newVersion = f.tailOffset
	}
	return
}

func (f *File) AppendCheck(
	assumedVersion uint64,
	payloadJSON []byte,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
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
	_, err = f.writeLog(buf.Bytes(), timestamp, payloadJSON)
	newVersion = f.tailOffset
	return
}

func (f *File) AppendCheckMulti(
	assumedVersion uint64,
	payloadsJSON ...[]byte,
) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
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

	for _, p := range payloadsJSON {
		offset = f.tailOffset
		_, err = f.writeLog(buf.Bytes(), timestamp, p)
		newVersion = f.tailOffset
	}
	return
}

func (f *File) writeLog(
	buf []byte,
	timestamp uint64,
	payload []byte,
) (int64, error) {
	offset := int64(f.tailOffset)

	buf8 := buf[:8]
	buf4 := buf[:4]

	// Write timestamp (8 bytes)
	binary.LittleEndian.PutUint64(buf8, timestamp)
	n, err := f.file.WriteAt(buf8, offset)
	if err != nil {
		return 0, err
	}
	if n != 8 {
		return 0, fmt.Errorf(
			"writing timestamp, unexpectedly wrote: %d",
			n,
		)
	}

	// Write payload hash (8 bytes, xxhash64)
	checksum := xxhash.Sum64(payload)
	binary.LittleEndian.PutUint64(buf8, checksum)
	if n, err = f.file.WriteAt(buf8, offset+8); err != nil {
		return 8, err
	}
	if n != 8 {
		return 8, fmt.Errorf(
			"writing payload hash, unexpectedly wrote: %d",
			n,
		)
	}

	// Write payload length (4 bytes)
	pldLen := uint32(len(payload))
	binary.LittleEndian.PutUint32(buf4, pldLen)
	n, err = f.file.WriteAt(buf4, offset+16)
	if err != nil {
		return 16, err
	}
	if n != 4 {
		return 16, fmt.Errorf(
			"writing payload length, unexpectedly wrote: %d",
			n,
		)
	}

	// Write payload
	n, err = f.file.WriteAt(payload, offset+20)
	if err != nil {
		return 20, err
	}
	if n != len(payload) {
		return 20, fmt.Errorf(
			"writing payload, unexpectedly wrote: %d",
			n,
		)
	}

	if err := f.file.Sync(); err != nil {
		return 20, err
	}

	wrote := offset - int64(f.tailOffset)
	f.tailOffset += uint64(20) + uint64(len(payload))

	return wrote, nil
}

func readHeader(
	reader reader,
	buf []byte,
) error {
	buf4 := buf[:4]
	n, err := reader.ReadAt(buf4, 0)
	if err != nil {
		return err
	}
	if n != 4 {
		return fmt.Errorf(
			"reading version header (expected: 4, actual: %d)", n,
		)
	}
	return checkVersion(binary.LittleEndian.Uint32(buf4))
}

func checkVersion(version uint32) error {
	if version != SupportedProtoVersion {
		return fmt.Errorf("unsupported file version (%d)", version)
	}
	return nil
}

type reader interface {
	ReadAt(buf []byte, offset int64) (read int, err error)
}
