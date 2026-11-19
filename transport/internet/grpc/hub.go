package grpc

import (
	"context"
	"time"

	goreality "github.com/xtls/reality"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/grpc/encoding"
	"github.com/xtls/xray-core/transport/internet/handoff"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/tls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type Listener struct {
	encoding.UnimplementedGRPCServiceServer
	ctx                  context.Context
	handler              internet.ConnHandler
	local                net.Addr
	trustedXForwardedFor []string

	s *grpc.Server
}

func (l Listener) Tun(server encoding.GRPCService_TunServer) error {
	tunCtx, cancel := context.WithCancel(l.ctx)
	l.handler(encoding.NewHunkConn(server, cancel, l.trustedXForwardedFor))
	<-tunCtx.Done()
	return nil
}

func (l Listener) TunMulti(server encoding.GRPCService_TunMultiServer) error {
	tunCtx, cancel := context.WithCancel(l.ctx)
	l.handler(encoding.NewMultiHunkConn(server, cancel, l.trustedXForwardedFor))
	<-tunCtx.Done()
	return nil
}

func (l Listener) Close() error {
	l.s.Stop()
	return nil
}

// Drain sends GOAWAY and waits for existing RPCs to end on their own. Only
// call it after the replacement can serve new connections. Close remains the
// immediate shutdown path, e.g. if systemd is stopping the entire service.
func (l *Listener) Drain() {
	l.s.GracefulStop()
}

func (l Listener) Addr() net.Addr {
	return l.local
}

func Listen(ctx context.Context, address net.Address, port net.Port, settings *internet.MemoryStreamConfig, handler internet.ConnHandler) (internet.Listener, error) {
	if listener := handoff.TCPListenerFromContext(ctx); listener != nil {
		return ServeListener(ctx, listener, settings, handler), nil
	}
	var addr net.Addr
	if port == 0 {
		addr = &net.UnixAddr{Name: address.Domain(), Net: "unix"}
	} else {
		addr = &net.TCPAddr{IP: address.IP(), Port: int(port)}
	}
	var streamListener net.Listener
	var err error
	if settings.FinalMask != nil {
		streamListener, err = settings.FinalMask.Listen(ctx, addr)
	} else {
		streamListener, err = internet.ListenSystem(ctx, addr, settings.SocketSettings)
	}
	if err != nil {
		return nil, errors.New("failed to listen on ", addr).Base(err)
	}
	return ServeListener(ctx, streamListener, settings, handler), nil
}

// ServeListener attaches the gRPC transport to an already prepared listener.
// Ownership of the listener is transferred to the gRPC server. The caller may
// supply a paused handoff listener here to serve from a shared accept queue.
func ServeListener(ctx context.Context, streamListener net.Listener, settings *internet.MemoryStreamConfig, handler internet.ConnHandler) *Listener {
	grpcSettings := settings.ProtocolSettings.(*Config)
	listener := &Listener{ctx: ctx, handler: handler, local: streamListener.Addr()}
	if settings.SocketSettings != nil {
		listener.trustedXForwardedFor = settings.SocketSettings.TrustedXForwardedFor
		if settings.SocketSettings.AcceptProxyProtocol {
			errors.LogWarning(ctx, "accepting PROXY protocol")
		}
	}

	config := tls.ConfigFromStreamSettings(settings)

	var options []grpc.ServerOption
	if config != nil {
		// gRPC server may silently ignore TLS errors
		options = append(options, grpc.Creds(credentials.NewTLS(config.GetTLSConfig(tls.WithNextProto("h2")))))
	}
	if grpcSettings.IdleTimeout > 0 || grpcSettings.HealthCheckTimeout > 0 {
		options = append(options, grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    time.Second * time.Duration(grpcSettings.IdleTimeout),
			Timeout: time.Second * time.Duration(grpcSettings.HealthCheckTimeout),
		}))
	}

	s := grpc.NewServer(options...)
	listener.s = s
	errors.LogDebug(ctx, "gRPC listen for service name `"+grpcSettings.getServiceName()+"` tun `"+grpcSettings.getTunStreamName()+"` multi tun `"+grpcSettings.getTunMultiStreamName()+"`")
	encoding.RegisterGRPCServiceServerX(s, listener, grpcSettings.getServiceName(), grpcSettings.getTunStreamName(), grpcSettings.getTunMultiStreamName())
	if config := reality.ConfigFromStreamSettings(settings); config != nil {
		streamListener = goreality.NewListener(streamListener, config.GetREALITYConfig())
	}

	go func() {
		if err := s.Serve(streamListener); err != nil {
			errors.LogInfoInner(ctx, err, "Listener for gRPC ended")
		}
	}()

	return listener
}

func init() {
	common.Must(internet.RegisterTransportListener(protocolName, Listen))
}
