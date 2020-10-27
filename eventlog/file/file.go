package file

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file/internal"
	"github.com/romshark/eventlog/internal/bufpool"
	"github.com/romshark/eventlog/internal/consts"
)

const (
	// SupportedProtoVersion defines the supported protocol version
	SupportedProtoVersion = 3

	// FileHeaderLen defines the file header length in bytes
	FileHeaderLen = 4

	// EntryHeaderLen defines the entry header length in bytes
	EntryHeaderLen = 22

	// MaxPayloadLen defines the maximum possible payload length in bytes
	MaxPayloadLen = 1024 * 1024 // 1 MiB

	// MinPayloadLen defines the minimum possible payload length in bytes
	MinPayloadLen = 7 // {"x":0}

	// MinEntryLen defines the minimum possible
	MinEntryLen = EntryHeaderLen + MinPayloadLen

	// MaxLabelLen defines the maximum possible label length in bytes
	MaxLabelLen = 256
)

// Make sure *File implements eventlog.Implementer
var _ eventlog.Implementer = new(File)

var readConfig = internal.ReaderConf{
	MaxPayloadLen: MaxPayloadLen,
	MinPayloadLen: MinPayloadLen,
}

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
		tailOffset: FileHeaderLen,
	}
	f.bufPool = bufpool.NewPool(MaxPayloadLen + consts.MaxLabelLen)

	b := f.bufPool.Get()
	defer b.Release()
	buf := b.Bytes()

	switch err := internal.ReadHeader(buf, f.file, checkVersion); err {
	case io.EOF:
		if err := internal.WriteFileHeader(
			buf,
			f.file,
			SupportedProtoVersion,
		); err != nil {
			return nil, err
		}
	case nil:
	default:
		return nil, err
	}

	return f, nil
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
func (f *File) FirstOffset() uint64 { return FileHeaderLen }

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
	if offset < FileHeaderLen {
		return 0, eventlog.ErrOffsetOutOfBound
	}

	b := f.bufPool.Get()
	defer b.Release()
	buf := b.Bytes()
	buf = buf[:cap(buf)]

	f.lock.RLock()
	defer f.lock.RUnlock()

	if offset >= f.tailOffset {
		return 0, eventlog.ErrOffsetOutOfBound
	}
	if f.tailOffset-offset < MinEntryLen {
		return 0, eventlog.ErrInvalidOffset
	}

	var (
		read int64  // bytes read
		tm   uint64 // timestamp
		lb   []byte // label
		pl   []byte // payload
	)

	var i int64
	if n > 0 {
		// Limited scan
		if offset+n > f.tailOffset {
			n -= offset + n - f.tailOffset
		}

		var r uint64
		for i, r = int64(offset), uint64(0); r < n; r++ {
			_, tm, lb, pl, read, err = internal.ReadEvent(
				buf, f.file, xxhash.New(), i, readConfig,
			)
			if err == io.EOF {
				err = nil
				return
			} else if err != nil {
				return
			}
			nextOffset = uint64(i)
			if err = fn(uint64(i), tm, lb, pl); err != nil {
				nextOffset += uint64(read)
				return
			}
			i += int64(read)
			nextOffset = uint64(i)
		}
	} else {
		// Unlimited scan
		for i = int64(offset); ; {
			if offset >= f.tailOffset {
				return
			}
			_, tm, lb, pl, read, err = internal.ReadEvent(
				buf, f.file, xxhash.New(), i, readConfig,
			)
			if err == io.EOF {
				err = nil
				return
			} else if err != nil {
				return
			}
			nextOffset = uint64(i)
			if err = fn(uint64(i), tm, lb, pl); err != nil {
				nextOffset += uint64(read)
				return
			}
			i += int64(read)
			nextOffset = uint64(i)
		}
	}
	return
}

func (f *File) write(
	buffer []byte,
	checksum uint64,
	timestamp uint64,
	event eventlog.Event,
) (
	offset uint64,
	newVersion uint64,
	err error,
) {
	offset = f.tailOffset
	written, err := internal.WriteEvent(
		f.file,
		buffer,
		checksum,
		int64(f.tailOffset),
		timestamp,
		event,
	)
	if err != nil {
		f.tailOffset = offset
		offset = 0
		return
	}
	f.tailOffset += uint64(written)
	newVersion = f.tailOffset
	return
}

func (f *File) writeMulti(
	buffer []byte,
	timestamp uint64,
	checksums []uint64,
	events []eventlog.Event,
) (
	offset uint64,
	newVersion uint64,
	err error,
) {
	offset = f.tailOffset
	var written int
	for i, e := range events {
		if written, err = internal.WriteEvent(
			f.file,
			buffer,
			checksums[i],
			int64(f.tailOffset),
			timestamp,
			e,
		); err != nil {
			f.tailOffset = offset
			offset = 0
			newVersion = 0
			return
		}
		f.tailOffset += uint64(written)
		newVersion = f.tailOffset
	}
	return
}

func (f *File) Append(event eventlog.Event) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now().UTC()
	timestamp := uint64(tm.Unix())

	buf := f.bufPool.Get()
	defer buf.Release()

	var checksum uint64
	if checksum, err = internal.Checksum(
		buf.Bytes(),
		xxhash.New(),
		timestamp,
		internal.UnsafeS2B(event.Label),
		event.PayloadJSON,
	); err != nil {
		err = fmt.Errorf("computing checksum: %w", err)
		return
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	offset, newVersion, err = f.write(
		buf.Bytes(),
		checksum,
		timestamp,
		event,
	)
	return
}

func (f *File) AppendMulti(events ...eventlog.Event) (
	offset uint64,
	newVersion uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now().UTC()
	timestamp := uint64(tm.Unix())

	buf := f.bufPool.Get()
	defer buf.Release()

	checksums := make([]uint64, len(events))
	for i, e := range events {
		var checksum uint64
		if checksum, err = internal.Checksum(
			buf.Bytes(),
			xxhash.New(),
			timestamp,
			internal.UnsafeS2B(e.Label),
			e.PayloadJSON,
		); err != nil {
			err = fmt.Errorf("computing checksum [%d]: %w", i, err)
			return
		}
		checksums[i] = checksum
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	offset, newVersion, err = f.writeMulti(
		buf.Bytes(),
		timestamp,
		checksums,
		events,
	)
	return
}

func (f *File) AppendCheck(
	assumedVersion uint64,
	event eventlog.Event,
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

	var checksum uint64
	if checksum, err = internal.Checksum(
		buf.Bytes(),
		xxhash.New(),
		timestamp,
		internal.UnsafeS2B(event.Label),
		event.PayloadJSON,
	); err != nil {
		err = fmt.Errorf("computing checksum: %w", err)
		return
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if assumedVersion != f.tailOffset {
		tm = time.Time{}
		err = eventlog.ErrMismatchingVersions
		return
	}

	offset, newVersion, err = f.write(
		buf.Bytes(),
		checksum,
		timestamp,
		event,
	)
	return
}

func (f *File) AppendCheckMulti(
	assumedVersion uint64,
	events ...eventlog.Event,
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

	checksums := make([]uint64, len(events))
	for i, e := range events {
		var checksum uint64
		if checksum, err = internal.Checksum(
			buf.Bytes(),
			xxhash.New(),
			timestamp,
			internal.UnsafeS2B(e.Label),
			e.PayloadJSON,
		); err != nil {
			err = fmt.Errorf("computing checksum [%d]: %w", i, err)
			return
		}
		checksums[i] = checksum
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if assumedVersion != f.tailOffset {
		tm = time.Time{}
		err = eventlog.ErrMismatchingVersions
		return
	}

	offset, newVersion, err = f.writeMulti(
		buf.Bytes(),
		timestamp,
		checksums,
		events,
	)
	return
}

func checkVersion(version uint32) error {
	if version != SupportedProtoVersion {
		return fmt.Errorf("unsupported file version (%d)", version)
	}
	return nil
}
