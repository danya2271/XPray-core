package splithttp

// upload_queue is a specialized priorityqueue + channel to reorder generic
// packets by a sequence number

import (
	"io"
	"sync/atomic"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/signal/done"
)

type Packet struct {
	Reader  *httpServerConn
	Payload []byte
	Seq     uint64
}

type uploadQueue struct {
	reader         atomic.Pointer[httpServerConn]
	pushedPackets  chan Packet
	heap           uploadHeap
	nextSeq        uint64
	maxPackets     int
	closed         *done.Instance
	pendingPayload []byte
}

func NewUploadQueue(maxPackets int) *uploadQueue {
	return &uploadQueue{
		pushedPackets: make(chan Packet, maxPackets),
		heap:          uploadHeap{},
		nextSeq:       0,
		closed:        done.New(),
		maxPackets:    maxPackets,
	}
}

func (h *uploadQueue) Push(p Packet) error {
	if h.reader.Load() != nil || (p.Reader != nil && !h.reader.CompareAndSwap(nil, p.Reader)) {
		return errors.New("h.reader already exists")
	}
	select {
	case h.pushedPackets <- p: // no panic
		if h.closed.Done() {
			return errors.New("packet queue closed")
		}
		return nil
	case <-h.closed.Wait():
		return errors.New("packet queue closed")
	}
}

func (h *uploadQueue) Close() error {
	h.closed.Close()
	if reader := h.reader.Load(); reader != nil {
		return reader.Close()
	}
	return nil
}

func (h *uploadQueue) Read(b []byte) (int, error) {
	if reader := h.reader.Load(); reader != nil {
		return reader.Read(b)
	}

	if h.closed.Done() {
		return 0, io.EOF
	}

	if len(h.pendingPayload) > 0 {
		pending := h.pendingPayload
		n := copy(b, pending)
		if n == len(pending) {
			h.pendingPayload = nil
		} else {
			h.pendingPayload = pending[n:]
		}
		return n, nil
	}

	if len(h.heap) == 0 {
		select {
		case p := <-h.pushedPackets:
			if p.Reader != nil {
				return p.Reader.Read(b)
			}
			h.heap.push(p)
		case <-h.closed.Wait():
			return 0, io.EOF
		}
	}

	for len(h.heap) > 0 {
		packet := h.heap.pop()
		n := 0

		if packet.Seq == h.nextSeq {
			n = copy(b, packet.Payload)

			if n < len(packet.Payload) {
				h.pendingPayload = packet.Payload[n:]
			}
			h.nextSeq = packet.Seq + 1

			return n, nil
		}

		// misordered packet
		if packet.Seq > h.nextSeq {
			if len(h.heap) > h.maxPackets {
				// the "reassembly buffer" is too large, and we want to
				// constrain memory usage somehow. let's tear down the
				// connection, and hope the application retries.
				return 0, errors.New("packet queue is too large")
			}
			h.heap.push(packet)
			select {
			case p := <-h.pushedPackets:
				h.heap.push(p)
			case <-h.closed.Wait():
				return 0, io.EOF
			}
		}
	}

	return 0, nil
}

type uploadHeap []Packet

func (h *uploadHeap) push(packet Packet) {
	*h = append(*h, packet)
	for index := len(*h) - 1; index > 0; {
		parent := (index - 1) / 2
		if (*h)[parent].Seq <= (*h)[index].Seq {
			break
		}
		(*h)[parent], (*h)[index] = (*h)[index], (*h)[parent]
		index = parent
	}
}

func (h *uploadHeap) pop() Packet {
	old := *h
	last := len(old) - 1
	packet := old[0]
	if last == 0 {
		old[0] = Packet{}
		*h = old[:0]
		return packet
	}

	old[0] = old[last]
	old[last] = Packet{}
	*h = old[:last]

	for index := 0; ; {
		left := index*2 + 1
		if left >= len(*h) {
			break
		}
		right := left + 1
		smaller := left
		if right < len(*h) && (*h)[right].Seq < (*h)[left].Seq {
			smaller = right
		}
		if (*h)[index].Seq <= (*h)[smaller].Seq {
			break
		}
		(*h)[index], (*h)[smaller] = (*h)[smaller], (*h)[index]
		index = smaller
	}

	return packet
}
