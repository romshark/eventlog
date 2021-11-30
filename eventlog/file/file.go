package file

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file/internal"
	"github.com/romshark/eventlog/internal/bufpool"

	xxhash "github.com/cespare/xxhash/v2"
)

const (
	// SupportedProtoVersion defines the supported protocol version
	SupportedProtoVersion = internal.SupportedProtoVersion

	// EntryHeaderLen defines the entry header length in bytes
	EntryHeaderLen = 8 + // Checksum
		8 + // Previous version
		8 + // Timestamp
		2 + // Label length
		4 // Payload length

	// MaxPayloadLen defines the maximum possible payload length in bytes
	MaxPayloadLen = 1024 * 1024 // 1 MiB

	// MinPayloadLen defines the minimum possible payload length in bytes
	MinPayloadLen = 7 // {"x":0}

	// MinEntryLen defines the minimum possible
	MinEntryLen = EntryHeaderLen + MinPayloadLen

	// MaxLabelLen defines the maximum possible label length in bytes
	MaxLabelLen = 256
)

// Make sure *File implements EventLogger
var _ eventlog.EventLogger = new(File)

var config = internal.Config{
	MaxPayloadLen: MaxPayloadLen,
	MinPayloadLen: MinPayloadLen,
}

// File is a persistent file-based event log
type File struct {
	metadata      map[string]string
	filePath      string
	lock          sync.RWMutex
	hasher        internal.Hasher
	file          *os.File
	bufPool       *bufpool.Pool
	headerLength  uint64
	latestVersion uint64
	writerOffset  int64
}

// Open loads an event log from a file
func Open(filePath string) (*File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_SYNC, 0664)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("reading file stat: %w", err)
	}

	f := &File{
		filePath:     filePath,
		file:         file,
		hasher:       xxhash.New(),
		writerOffset: fi.Size(),
	}
	f.bufPool = bufpool.NewPool(MaxPayloadLen + MaxLabelLen + 8)

	b := f.bufPool.Get()
	defer b.Release()
	buf := b.Bytes()
	buf = buf[:cap(buf)]

	f.metadata = make(map[string]string)
	headerLen, err := internal.ReadHeader(
		buf,
		f.file,
		f.hasher,
		config,
		func(field, value string) error {
			f.metadata[field] = value
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}
	f.headerLength = uint64(headerLen)

	if f.writerOffset > headerLen {
		// The log isn't empty
		// Read version of the last event
		buf8 := buf[:8]
		if _, err := file.ReadAt(buf8, f.writerOffset-8); err != nil {
			return nil, fmt.Errorf("reading the last event version: %w", err)
		}
		f.latestVersion = binary.LittleEndian.Uint64(buf8)

		// Check the integrity of the last event
		if _, _, _, err := internal.ReadEvent(
			buf, f.file, f.hasher, int64(f.latestVersion), config,
		); err != nil {
			return nil, fmt.Errorf("checking integrity of last event: %w", err)
		}
	}

	return f, nil
}

// Create creates a new persistent file-based event log
func Create(
	filePath string,
	metadata map[string]string,
	fsFileMode fs.FileMode,
) error {
	if _, err := os.Stat(filePath); err == nil {
		return ErrExists
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("reading file stats: %w", err)
	}

	f, err := os.OpenFile(
		filePath,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		fsFileMode,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encoding metadata JSON: %w", err)
	}

	if _, err := internal.WriteFileHeader(
		f, xxhash.New(), time.Now(), metaJSON, config,
	); err != nil {
		return fmt.Errorf("writing file header: %w", err)
	}

	return nil
}

var ErrExists = errors.New("can't override existing file")

func (f *File) MetadataLen() int {
	return len(f.metadata)
}

func (f *File) ScanMetadata(fn func(field, value string) bool) {
	for f, v := range f.metadata {
		if !fn(f, v) {
			return
		}
	}
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
	v := f.latestVersion
	f.lock.RUnlock()
	return uint64(v)
}

// VersionInitial implements EventLogger.VersionInitial
func (f *File) VersionInitial() uint64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if f.latestVersion == 0 {
		return 0
	}
	return f.headerLength
}

// Scan implements EventLogger.Scan
func (f *File) Scan(
	version uint64,
	reverse bool,
	fn eventlog.ScanFn,
) (err error) {
	if version < f.headerLength {
		return eventlog.ErrInvalidVersion
	}

	b := f.bufPool.Get()
	defer b.Release()
	buf := b.Bytes()
	buf = buf[:cap(buf)]

	f.lock.RLock()
	defer f.lock.RUnlock()

	if version > f.latestVersion {
		return eventlog.ErrInvalidVersion
	} else if version < f.headerLength {
		return eventlog.ErrInvalidVersion
	}

	if reverse {
		nextVersion := uint64(0)
		if version != f.latestVersion {
			// Read and ignore the event which has a subsequent one
			// to determine the version of the latter
			_, _, n, err := internal.ReadEvent(
				buf, f.file, f.hasher, int64(version), config,
			)
			if err == io.EOF {
				return fmt.Errorf(
					"error at offset %d: "+
						"reading entry to determine next version: %w",
					version, err,
				)
			}

			// Determine next version
			_, _, n, err = internal.ReadEvent(
				buf, f.file, f.hasher, int64(version+uint64(n)), config,
			)
			if err == io.EOF {
				panic("there must have been a subsequent event!")
			} else if err != nil {
				return fmt.Errorf(
					"error at offset %d: "+
						"reading entry to determine next version: %w",
					version, err,
				)
			}
			nextVersion = uint64(n)
		}

		for i := int64(version); i >= int64(f.headerLength); {
			_, event, n, err :=
				internal.ReadEvent(buf, f.file, f.hasher, i, config)
			if err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf(
					"error at offset %d: reading entry: %w", i, err,
				)
			}

			event.VersionNext = nextVersion
			nextVersion = uint64(i)
			i -= int64(n)

			if err := fn(event); err != nil {
				return err
			}
		}
	} else {
		for i := int64(version); ; {
			_, event, n, err :=
				internal.ReadEvent(buf, f.file, f.hasher, i, config)
			if err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf(
					"error at offset %d: reading entry: %w", i, err,
				)
			}

			if uint64(i) < f.latestVersion {
				i += int64(n)
				event.VersionNext = uint64(i)
			} else {
				i += int64(n)
			}

			if err := fn(event); err != nil {
				return err
			}
		}
	}
	return
}

func (f *File) write(
	buffer []byte,
	checksum uint64,
	timestamp uint64,
	event eventlog.EventData,
) error {
	written, err := internal.WriteEvent(
		f.file,
		buffer,
		checksum,
		int64(f.writerOffset),
		eventlog.Event{
			VersionPrevious: f.latestVersion,
			EventData:       event,
			Timestamp:       timestamp,
		},
		config,
	)
	if err != nil {
		return err
	}

	f.latestVersion = uint64(f.writerOffset)
	f.writerOffset += int64(written)

	return nil
}

func (f *File) writeMulti(
	buffer []byte,
	timestamp uint64,
	checksums []uint64,
	events []eventlog.EventData,
) (versionFirst uint64, err error) {
	var written int
	initialWriterOffset := f.writerOffset
	initialLatestVersion := f.latestVersion
	for i, e := range events {
		if written, err = internal.WriteEvent(
			f.file,
			buffer,
			checksums[i],
			int64(f.writerOffset),
			eventlog.Event{
				VersionPrevious: f.latestVersion,
				EventData:       e,
				Timestamp:       timestamp,
			},
			config,
		); err != nil {
			f.writerOffset = initialWriterOffset
			f.latestVersion = initialLatestVersion
			return
		}
		f.latestVersion = uint64(f.writerOffset)
		f.writerOffset += int64(written)
		if i < 1 {
			versionFirst = f.latestVersion
		}
	}
	return
}

func (f *File) Append(event eventlog.EventData) (
	versionPrevious uint64,
	version uint64,
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
		event.Label,
		event.PayloadJSON,
		f.latestVersion,
	); err != nil {
		err = fmt.Errorf("computing checksum: %w", err)
		return
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	versionPrevious = f.latestVersion

	if err = f.write(buf.Bytes(), checksum, timestamp, event); err != nil {
		versionPrevious = 0
		tm = time.Time{}
		return
	}

	version = f.latestVersion

	return
}

func (f *File) AppendMulti(events ...eventlog.EventData) (
	versionPrevious uint64,
	versionFirst uint64,
	version uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now().UTC()
	timestamp := uint64(tm.Unix())

	buf := f.bufPool.Get()
	defer buf.Release()

	checksums := make([]uint64, len(events))
	latestVersion := f.latestVersion
	for i, e := range events {
		var checksum uint64

		if checksum, err = internal.Checksum(
			buf.Bytes(),
			xxhash.New(),
			timestamp,
			e.Label,
			e.PayloadJSON,
			latestVersion,
		); err != nil {
			err = fmt.Errorf("computing checksum [%d]: %w", i, err)
			return
		}
		checksums[i] = checksum
		if latestVersion == 0 {
			latestVersion = f.headerLength
		} else {
			latestVersion += EventLen(e)
		}
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	versionPrevious = f.latestVersion

	versionFirst, err = f.writeMulti(buf.Bytes(), timestamp, checksums, events)
	if err != nil {
		tm = time.Time{}
		versionPrevious = 0
		versionFirst = 0
		return
	}
	version = f.latestVersion
	return
}

func (f *File) AppendCheck(
	assumedVersion uint64,
	event eventlog.EventData,
) (
	version uint64,
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
		event.Label,
		event.PayloadJSON,
		f.latestVersion,
	); err != nil {
		err = fmt.Errorf("computing checksum: %w", err)
		return
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if assumedVersion != f.latestVersion {
		tm = time.Time{}
		err = eventlog.ErrMismatchingVersions
		return
	}

	if err = f.write(buf.Bytes(), checksum, timestamp, event); err != nil {
		tm = time.Time{}
		return
	}
	version = f.latestVersion
	return
}

func (f *File) AppendCheckMulti(
	assumedVersion uint64,
	events ...eventlog.EventData,
) (
	versionFirst uint64,
	version uint64,
	tm time.Time,
	err error,
) {
	tm = time.Now().UTC()
	timestamp := uint64(tm.Unix())

	buf := f.bufPool.Get()
	defer buf.Release()

	checksums := make([]uint64, len(events))
	latestVersion := f.latestVersion
	for i, e := range events {
		var checksum uint64
		if checksum, err = internal.Checksum(
			buf.Bytes(),
			xxhash.New(),
			timestamp,
			e.Label,
			e.PayloadJSON,
			latestVersion,
		); err != nil {
			err = fmt.Errorf("computing checksum [%d]: %w", i, err)
			return
		}
		checksums[i] = checksum
		if latestVersion == 0 {
			latestVersion = f.headerLength
		} else {
			latestVersion += EventLen(e)
		}
	}
	f.lock.Lock()
	defer f.lock.Unlock()

	if assumedVersion != f.latestVersion {
		tm = time.Time{}
		err = eventlog.ErrMismatchingVersions
		return
	}

	versionFirst, err = f.writeMulti(
		buf.Bytes(),
		timestamp,
		checksums,
		events,
	)
	if err != nil {
		versionFirst = 0
		tm = time.Time{}
		return
	}
	version = f.latestVersion
	return
}

func EventLen(e eventlog.EventData) uint64 {
	return uint64(8 + // Checksum
		8 + // Timestamp
		2 + // Label length
		4 + // Payload length
		len(e.Label) + // Label
		len(e.PayloadJSON) + // Payload
		8) // Previous version
}
