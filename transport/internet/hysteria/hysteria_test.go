package hysteria

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
)

func TestDatagram(t *testing.T) {
	run := func() (addr net.Addr, recv chan int64, cancel func()) {
		cert, _ := cert.MustGenerate(nil)
		Certificate := [][]byte{cert.Certificate}
		PrivateKey := common.Must2(x509.ParsePKCS8PrivateKey(cert.PrivateKey))

		tlsConf := &tls.Config{
			Certificates: []tls.Certificate{
				{
					Certificate: Certificate,
					PrivateKey:  PrivateKey,
				},
			},
			NextProtos: []string{"h3"},
		}

		quicConf := &quic.Config{
			InitialStreamReceiveWindow:     8388608,
			MaxStreamReceiveWindow:         8388608,
			InitialConnectionReceiveWindow: 8388608 * 5 / 2,
			MaxConnectionReceiveWindow:     8388608 * 5 / 2,
			MaxIdleTimeout:                 30 * time.Second,
			MaxIncomingStreams:             1024,
			DisablePathMTUDiscovery:        runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin",
			EnableDatagrams:                true,
			MaxDatagramFrameSize:           MaxDatagramFrameSize,
			AssumePeerMaxDatagramFrameSize: MaxDatagramFrameSize,
			DisablePathManager:             true,
		}

		pktConn := common.Must2(net.ListenPacket("udp", "127.0.0.1:0"))
		tr := &quic.Transport{Conn: pktConn}
		l := common.Must2(tr.Listen(tlsConf, quicConf))

		recv = make(chan int64)
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			defer pktConn.Close()
			defer tr.Close()
			defer l.Close()
			defer close(recv)

			var buf [1500]byte
			for {
				conn, err := l.Accept(ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						t.Error(err)
					}
					break
				}
				err = conn.SendDatagram(buf[:])
				var qErr *quic.DatagramTooLargeError
				if !errors.As(err, &qErr) {
					t.Error(err)
				}
				recv <- qErr.MaxDatagramPayloadSize
				defer conn.CloseWithError(0, "")
			}
		}()

		return l.Addr(), recv, cancel
	}

	addr, recv, cancel := run()

	t.Run("With ChromeParrot", func(t *testing.T) {
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
		}

		quicConf := &quic.Config{
			InitialStreamReceiveWindow:     8388608,
			MaxStreamReceiveWindow:         8388608,
			InitialConnectionReceiveWindow: 8388608 * 5 / 2,
			MaxConnectionReceiveWindow:     8388608 * 5 / 2,
			MaxIdleTimeout:                 30 * time.Second,
			KeepAlivePeriod:                10 * time.Second,
			DisablePathMTUDiscovery:        runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin",
			ChromeParrot:                   true,
			EnableDatagrams:                true,
			MaxDatagramFrameSize:           MaxDatagramFrameSize,
			OmitMaxDatagramFrameSize:       true,
			DisablePathManager:             true,
		}

		pktConn := common.Must2(net.ListenPacket("udp", "127.0.0.1:0"))
		tr := &quic.Transport{Conn: pktConn, ConnectionIDGenerator: quic.ZeroLengthConnectionIDGenerator{}}
		conn := common.Must2(tr.DialEarly(context.Background(), addr, tlsConf, quicConf))

		defer pktConn.Close()
		defer tr.Close()
		defer conn.CloseWithError(0, "")

		var buf [1500]byte
		err := conn.SendDatagram(buf[:])
		var qErr *quic.DatagramTooLargeError
		if !errors.As(err, &qErr) || qErr.MaxDatagramPayloadSize != 1197 {
			t.Error(err)
		}
		if server := <-recv; server != 1243 {
			t.Error(server)
		}
	})

	t.Run("Without ChromeParrot", func(t *testing.T) {
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"h3"},
		}

		quicConf := &quic.Config{
			InitialStreamReceiveWindow:     8388608,
			MaxStreamReceiveWindow:         8388608,
			InitialConnectionReceiveWindow: 8388608 * 5 / 2,
			MaxConnectionReceiveWindow:     8388608 * 5 / 2,
			MaxIdleTimeout:                 30 * time.Second,
			KeepAlivePeriod:                10 * time.Second,
			DisablePathMTUDiscovery:        runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin",
			ChromeParrot:                   false,
			EnableDatagrams:                true,
			MaxDatagramFrameSize:           MaxDatagramFrameSize,
			OmitMaxDatagramFrameSize:       true,
			DisablePathManager:             true,
		}

		pktConn := common.Must2(net.ListenPacket("udp", "127.0.0.1:0"))
		tr := &quic.Transport{Conn: pktConn}
		conn := common.Must2(tr.DialEarly(context.Background(), addr, tlsConf, quicConf))

		defer pktConn.Close()
		defer tr.Close()
		defer conn.CloseWithError(0, "")

		var buf [1500]byte
		err := conn.SendDatagram(buf[:])
		var qErr *quic.DatagramTooLargeError
		if !errors.As(err, &qErr) || qErr.MaxDatagramPayloadSize != 1197 {
			t.Error(err)
		}
		if server := <-recv; server != 1197 {
			t.Error(server)
		}
	})

	cancel()
}

func TestRetryIfInactiveRedialsOnce(t *testing.T) {
	connectionStatus := StatusInactive
	openCalls := 0
	redialCalls := 0

	got, err := retryIfInactive(
		context.Background(),
		func() status { return connectionStatus },
		func(context.Context) error {
			redialCalls++
			connectionStatus = StatusActive
			return nil
		},
		func() (int, error) {
			openCalls++
			if openCalls == 1 {
				return 0, errors.New("stale connection")
			}
			return 42, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("open result = %d, want 42", got)
	}
	if openCalls != 2 || redialCalls != 1 {
		t.Fatalf("open calls = %d, redial calls = %d; want 2 and 1", openCalls, redialCalls)
	}
}

func TestRetryIfInactiveDoesNotRedialLiveConnection(t *testing.T) {
	streamLimitErr := &quic.StreamLimitReachedError{}
	openCalls := 0
	redialCalls := 0

	_, err := retryIfInactive(
		context.Background(),
		func() status { return StatusActive },
		func(context.Context) error {
			redialCalls++
			return nil
		},
		func() (int, error) {
			openCalls++
			return 0, streamLimitErr
		},
	)
	if err != streamLimitErr {
		t.Fatalf("open error = %v, want original stream-limit error", err)
	}
	if openCalls != 1 || redialCalls != 0 {
		t.Fatalf("open calls = %d, redial calls = %d; want 1 and 0", openCalls, redialCalls)
	}
}

func TestRetryIfInactiveReturnsRedialError(t *testing.T) {
	dialErr := errors.New("redial failed")
	openCalls := 0

	_, err := retryIfInactive(
		context.Background(),
		func() status { return StatusInactive },
		func(context.Context) error { return dialErr },
		func() (int, error) {
			openCalls++
			return 0, errors.New("stale connection")
		},
	)
	if err != dialErr {
		t.Fatalf("retry error = %v, want redial error", err)
	}
	if openCalls != 1 {
		t.Fatalf("open calls = %d, want 1", openCalls)
	}
}
