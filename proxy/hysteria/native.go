package hysteria

import (
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport/internet/stat"
)

type nativeConnection interface {
	buf.Reader
	buf.Writer
	NativeHysteriaTarget() net.Destination
	CloseWrite() error
	Close() error
}

func nativeLink(conn stat.Connection) (nativeConnection, buf.Reader, buf.Writer, bool) {
	native, ok := stat.TryUnwrapStatsConn(conn).(nativeConnection)
	if !ok {
		return nil, nil, nil, false
	}
	reader := nativeReader{Reader: native, connection: native}
	writer := nativeWriter{Writer: native, connection: native}
	if counted, ok := conn.(*stat.CounterConnection); ok {
		reader.counter = counted.ReadCounter
		writer.counter = counted.WriteCounter
	}
	return native, reader, writer, true
}

type nativeReader struct {
	buf.Reader
	counter    stats.Counter
	connection nativeConnection
}

func (r nativeReader) Interrupt() { r.connection.Close() }

func (r nativeReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.Reader.ReadMultiBuffer()
	if r.counter != nil {
		r.counter.Add(int64(mb.Len()))
	}
	return mb, err
}

type nativeWriter struct {
	buf.Writer
	counter    stats.Counter
	connection nativeConnection
}

func (w nativeWriter) Close() error { return w.connection.CloseWrite() }
func (w nativeWriter) Interrupt()   { w.connection.Close() }

func (w nativeWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if w.counter == nil {
		return w.Writer.WriteMultiBuffer(mb)
	}
	n := mb.Len()
	err := w.Writer.WriteMultiBuffer(mb)
	if err == nil {
		w.counter.Add(int64(n))
	}
	return err
}
