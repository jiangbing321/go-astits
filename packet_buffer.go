package astits

import (
	"fmt"
	"io"

	"math"
	"sync"

	"github.com/pkg/errors"
)

// packetBuffer represents a packet buffer
type packetBuffer struct {
	b          []byte
	items      []*packetBufferItem
	packetSize int
	r          io.Reader
}

// packetBufferItem represents a packet buffer item
type packetBufferItem struct {
	err error
	p   *Packet
	wg  sync.WaitGroup
}

// newPacketBuffer creates a new packet buffer
func newPacketBuffer(r io.Reader, packetSize int) (pb *packetBuffer, err error) {
	// Init
	pb = &packetBuffer{
		packetSize: packetSize,
		r:          r,
	}

	// Packet size is not set
	if pb.packetSize == 0 {
		// Auto detect packet size
		if pb.packetSize, err = autoDetectPacketSize(r); err != nil {
			err = errors.Wrap(err, "astits: auto detecting packet size failed")
			return
		}
	}
	pb.b = make([]byte, 10000*pb.packetSize)
	return
}

// autoDetectPacketSize updates the packet size based on the first bytes
// Minimum packet size is 188 and is bounded by 2 sync bytes
// Assumption is made that the first byte of the reader is a sync byte
func autoDetectPacketSize(r io.Reader) (packetSize int, err error) {
	// Read first bytes
	const l = 193
	var b = make([]byte, l)
	if _, err = r.Read(b); err != nil {
		err = errors.Wrapf(err, "astits: reading first %d bytes failed", l)
		return
	}

	// Packet must start with a sync byte
	if b[0] != syncByte {
		err = ErrPacketMustStartWithASyncByte
		return
	}

	// Look for sync bytes
	for idx, b := range b {
		if b == syncByte && idx >= 188 {
			// Update packet size
			packetSize = idx

			// Rewind or sync reader
			var n int64
			if n, err = rewind(r); err != nil {
				err = errors.Wrap(err, "astits: rewinding failed")
				return
			} else if n == -1 {
				var ls = packetSize - (l - packetSize)
				if _, err = r.Read(make([]byte, ls)); err != nil {
					err = errors.Wrapf(err, "astits: reading %d bytes to sync reader failed", ls)
					return
				}
			}
			return
		}
	}
	err = fmt.Errorf("astits: only one sync byte detected in first %d bytes", l)
	return
}

// rewind rewinds the reader if possible, otherwise n = -1
func rewind(r io.Reader) (n int64, err error) {
	if s, ok := r.(io.Seeker); ok {
		if n, err = s.Seek(0, 0); err != nil {
			err = errors.Wrap(err, "astits: seeking to 0 failed")
			return
		}
		return
	}
	n = -1
	return
}

// next fetches the next packet from the buffer
func (pb *packetBuffer) next() (p *Packet, err error) {
	// Check items
	if p, err = pb.first(); p != nil || err != nil {
		return
	}

	// Read
	var n int
	if n, err = io.ReadFull(pb.r, pb.b); err != nil && err != io.ErrUnexpectedEOF {
		if err == io.EOF {
			err = ErrNoMorePackets
		} else {
			err = errors.Wrapf(err, "astits: reading %d bytes failed", len(pb.b))
		}
		return
	}

	// Loop through packets
	for i := 0; i < int(math.Ceil(float64(n)/float64(pb.packetSize))); i++ {
		var item = &packetBufferItem{}
		item.wg.Add(1)
		pb.items = append(pb.items, item)
		go func(i int) {
			defer item.wg.Done()
			var b = make([]byte, pb.packetSize)
			copy(b, pb.b[i*pb.packetSize:(i+1)*pb.packetSize])
			if item.p, item.err = parsePacket(b); err != nil {
				item.err = errors.Wrap(item.err, "astits: building packet failed")
				return
			}
		}(i)
	}

	// Get first packet
	p, err = pb.first()
	return
}

// first returns the first packet of the buffer
func (pb *packetBuffer) first() (p *Packet, err error) {
	if len(pb.items) > 0 {
		pb.items[0].wg.Wait()
		p, err = pb.items[0].p, pb.items[0].err
		pb.items = pb.items[1:]
	}
	return
}
