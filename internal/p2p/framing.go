package p2p

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

// MaxFrameBytes bounds a single length-prefixed frame. It is a hard guard
// against a peer that announces an absurd length.
const MaxFrameBytes = 8 << 20

// writeFrame emits a uvarint length prefix followed by the payload.
func writeFrame(w io.Writer, payload []byte) error {
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(payload)))
	if _, err := w.Write(hdr[:n]); err != nil {
		return fmt.Errorf("p2p: write frame header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("p2p: write frame body: %w", err)
	}
	return nil
}

// readFrame reads one length-prefixed payload, capped at limit bytes.
func readFrame(r io.Reader, limit int) ([]byte, error) {
	size, err := binary.ReadUvarint(&byteReader{r})
	if errors.Is(err, io.EOF) {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("p2p: read frame header: %w", err)
	}
	if size == 0 {
		return nil, nil
	}
	if size > uint64(MaxFrameBytes) {
		return nil, fmt.Errorf("p2p: frame of %d bytes exceeds protocol cap", size)
	}
	if limit > 0 && size > uint64(limit) {
		return nil, fmt.Errorf("p2p: frame of %d bytes exceeds configured limit %d", size, limit)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("p2p: read frame body: %w", err)
	}
	return buf, nil
}

// writeMsg marshals and frames a protobuf message.
func writeMsg(w io.Writer, m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("p2p: marshal: %w", err)
	}
	return writeFrame(w, b)
}

// readMsg reads one frame and unmarshals it into m.
func readMsg(r io.Reader, limit int, m proto.Message) error {
	b, err := readFrame(r, limit)
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return errors.New("p2p: empty message frame")
	}
	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("p2p: unmarshal: %w", err)
	}
	return nil
}

// byteReader adapts an io.Reader to the io.ByteReader that binary.ReadUvarint
// requires without buffering the whole stream.
type byteReader struct{ r io.Reader }

func (b *byteReader) ReadByte() (byte, error) {
	var tmp [1]byte
	if _, err := io.ReadFull(b.r, tmp[:]); err != nil {
		return 0, err
	}
	return tmp[0], nil
}
