package main

import (
	"context"
	"errors"
	"testing"

	handler "github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/common/protocol"
	"google.golang.org/grpc"
)

type probeAPIStub struct {
	handler.HandlerServiceClient
	calls               int
	addErr, cleanupErr  error
	cleanupContextValid bool
}

func (s *probeAPIStub) AlterInbound(ctx context.Context, _ *handler.AlterInboundRequest, _ ...grpc.CallOption) (*handler.AlterInboundResponse, error) {
	s.calls++
	if s.calls == 1 {
		return &handler.AlterInboundResponse{}, s.addErr
	}
	_, bounded := ctx.Deadline()
	s.cleanupContextValid = ctx.Err() == nil && bounded
	return &handler.AlterInboundResponse{}, s.cleanupErr
}

func TestCandidateProbeRequiresCredentialCleanup(t *testing.T) {
	cleanupErr := errors.New("injected credential cleanup failure")
	verifyErr := errors.New("injected public tunnel failure")
	for _, failTunnel := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful-tunnel", true: "failed-tunnel"}[failTunnel], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			api := &probeAPIStub{cleanupErr: cleanupErr}
			err := withCandidateUser(ctx, api, "hy", &protocol.User{Email: "probe@invalid"}, func() error {
				cancel()
				if failTunnel {
					return verifyErr
				}
				return nil
			})
			if !errors.Is(err, cleanupErr) || (failTunnel && !errors.Is(err, verifyErr)) {
				t.Fatalf("lost cleanup/tunnel failure: %v", err)
			}
			if api.calls != 2 || !api.cleanupContextValid {
				t.Fatal("cleanup did not use an independent bounded context")
			}
		})
	}
}

func TestCandidateProbeDoesNotVerifyWithoutCredential(t *testing.T) {
	addErr := errors.New("injected credential failure")
	api := &probeAPIStub{addErr: addErr}
	err := withCandidateUser(t.Context(), api, "hy", &protocol.User{Email: "probe@invalid"}, func() error {
		t.Fatal("verified without a candidate-only credential")
		return nil
	})
	if !errors.Is(err, addErr) || api.calls != 1 {
		t.Fatalf("credential failure: calls=%d err=%v", api.calls, err)
	}
}
