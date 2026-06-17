package eventlog

import (
	"encoding/binary"
	"io"

	v1 "github.com/supunhg/kairos/api/v1"
	"google.golang.org/protobuf/proto"
)

const (
	magicBytes = 0x4B414952 // "KAIROS" in hex
	version    = 1
)

type Decoder struct {
	r            io.Reader
	lastOffset   int64
	currentStart int64
	bytesRead    uint64
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

func (d *Decoder) Decode() (*v1.Event, error) {
	d.currentStart = d.lastOffset

	var header [14]byte
	n, err := io.ReadFull(d.r, header[:])
	d.bytesRead += uint64(n) //nolint:gosec // safe: n is bounded by header size
	d.lastOffset += int64(n)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}

	magic := binary.BigEndian.Uint32(header[:4])
	if magic != magicBytes {
		return nil, ErrInvalidMagic
	}

	ver := binary.BigEndian.Uint16(header[4:6])
	if ver != version {
		return nil, ErrUnsupportedVersion
	}

	length := binary.BigEndian.Uint64(header[6:14])
	if length > 1<<30 { // 1GB max event
		return nil, ErrEventTooLarge
	}

	data := make([]byte, length)
	n, err = io.ReadFull(d.r, data)
	d.bytesRead += uint64(n) //nolint:gosec // safe: n is bounded by data length
	d.lastOffset += int64(n)
	if err != nil {
		return nil, err
	}

	var ev v1.Event
	if err := proto.Unmarshal(data, &ev); err != nil {
		return nil, err
	}

	return &ev, nil
}

func (d *Decoder) LastOffset() int64 {
	return d.currentStart
}

func (d *Decoder) BytesRead() uint64 {
	return d.bytesRead
}

func MarshalEvent(ev *v1.Event) ([]byte, error) {
	data, err := proto.Marshal(ev)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 14+len(data))
	binary.BigEndian.PutUint32(buf[:4], magicBytes)
	binary.BigEndian.PutUint16(buf[4:6], version)
	binary.BigEndian.PutUint64(buf[6:14], uint64(len(data)))
	copy(buf[14:], data)
	return buf, nil
}

func UnmarshalEvent(data []byte) (*v1.Event, error) {
	if len(data) < 14 {
		return nil, ErrInvalidFormat
	}
	magic := binary.BigEndian.Uint32(data[:4])
	if magic != magicBytes {
		return nil, ErrInvalidMagic
	}
	ver := binary.BigEndian.Uint16(data[4:6])
	if ver != version {
		return nil, ErrUnsupportedVersion
	}
	length := binary.BigEndian.Uint64(data[6:14])
	if uint64(len(data)) < 14+length {
		return nil, ErrInvalidFormat
	}
	var ev v1.Event
	if err := proto.Unmarshal(data[14:14+length], &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

var (
	ErrInvalidMagic       = NewError("invalid magic bytes")
	ErrUnsupportedVersion = NewError("unsupported version")
	ErrEventTooLarge      = NewError("event too large")
	ErrInvalidFormat      = NewError("invalid format")
	ErrEventNotFound      = NewError("event not found")
	ErrStoreClosed        = NewError("store closed")
	ErrMissingPayloadType = NewError("missing payload type")
)
