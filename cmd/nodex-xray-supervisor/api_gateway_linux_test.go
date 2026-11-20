//go:build linux && !android

package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"

	handler "github.com/xtls/xray-core/app/proxyman/command"
	stats "github.com/xtls/xray-core/app/stats/command"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type generationAPIStub struct {
	handler.UnimplementedHandlerServiceServer
	stats.UnimplementedStatsServiceServer
	mu      sync.Mutex
	users   map[string]*protocol.User
	value   int64
	fail    bool
	resets  int
	queries []string
}

func (s *generationAPIStub) GetInboundUsers(_ context.Context, request *handler.GetInboundUserRequest) (*handler.GetInboundUserResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, request.Email)
	if request.Email != "" {
		// Match the upstream API, including its nil user for an absent email.
		return &handler.GetInboundUserResponse{Users: []*protocol.User{s.users[request.Email]}}, nil
	}
	response := &handler.GetInboundUserResponse{}
	for _, user := range s.users {
		response.Users = append(response.Users, user)
	}
	return response, nil
}
func (s *generationAPIStub) AlterInbound(_ context.Context, request *handler.AlterInboundRequest) (*handler.AlterInboundResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return nil, grpcstatus.Error(codes.Unavailable, "injected API failure")
	}
	operation, err := request.Operation.GetInstance()
	if err != nil {
		return nil, err
	}
	switch operation := operation.(type) {
	case *handler.AddUserOperation:
		s.users[operation.User.Email] = operation.User
	case *handler.RemoveUserOperation:
		delete(s.users, operation.Email)
	}
	return &handler.AlterInboundResponse{}, nil
}
func (s *generationAPIStub) QueryStats(_ context.Context, request *stats.QueryStatsRequest) (*stats.QueryStatsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.Reset_ {
		s.resets++
		return nil, grpcstatus.Error(codes.FailedPrecondition, "worker counters must never reset")
	}
	return &stats.QueryStatsResponse{Stat: []*stats.Stat{{Name: "user>>>test>>>traffic>>>uplink", Value: s.value}}}, nil
}

func attachGenerationAPI(t *testing.T, g *apiGateway, id string) *generationAPIStub {
	t.Helper()
	path := filepath.Join(g.root, id+".sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	stub := &generationAPIStub{users: make(map[string]*protocol.User)}
	server := grpc.NewServer()
	handler.RegisterHandlerServiceServer(server, stub)
	stats.RegisterStatsServiceServer(server, stub)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	if err := g.connect(id, path); err != nil {
		t.Fatal(err)
	}
	return stub
}

func TestGatewayReplayUsesTargetedUserLookups(t *testing.T) {
	g, err := newAPIGateway(socketRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()
	stub := attachGenerationAPI(t, g, "g1")
	const userCount = 128
	for i := range userCount {
		user := &protocol.User{Email: fmt.Sprintf("user-%d@example.invalid", i), Level: 1}
		data, err := proto.Marshal(user)
		if err != nil {
			t.Fatal(err)
		}
		g.journal.Users["hy\x00"+user.Email] = desiredUser{Tag: "hy", Email: user.Email, User: data}
		if i%2 == 0 {
			stub.users[user.Email] = user
		}
	}
	stub.users["removed@example.invalid"] = &protocol.User{Email: "removed@example.invalid"}
	g.journal.Users["hy\x00removed@example.invalid"] = desiredUser{Tag: "hy", Email: "removed@example.invalid"}
	if err := g.replay(t.Context(), g.clients["g1"]); err != nil {
		t.Fatal(err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.users) != userCount || len(stub.queries) != userCount+1 {
		t.Fatalf("replay: users=%d lookups=%d", len(stub.users), len(stub.queries))
	}
	for _, email := range stub.queries {
		if email == "" {
			t.Fatal("replay fetched the entire user list for an individual mutation")
		}
	}
	if stub.users["removed@example.invalid"] != nil {
		t.Fatal("targeted lookup failed to apply removal")
	}
}

func TestGatewayCountersSurviveFinalizationResetAndRecovery(t *testing.T) {
	g, err := newAPIGateway(socketRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()
	a, b := attachGenerationAPI(t, g, "g1"), attachGenerationAPI(t, g, "g2")
	a.value, b.value = 100, 20
	check := func(want int64) {
		t.Helper()
		response, err := g.QueryStats(t.Context(), &stats.QueryStatsRequest{Pattern: "user>>>"})
		if err != nil || len(response.Stat) != 1 || response.Stat[0].Value != want {
			t.Fatalf("counter %v, %v; want %d", response, err, want)
		}
	}
	check(120)
	if err := g.finalize(t.Context(), "g1"); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.value = 30
	b.mu.Unlock()
	check(130)
	response, err := g.GetStats(t.Context(), &stats.GetStatsRequest{Name: "user>>>test>>>traffic>>>uplink", Reset_: true})
	if err != nil || response.Stat.Value != 130 {
		t.Fatalf("reset: %v, %v", response, err)
	}
	check(0)
	b.mu.Lock()
	b.value = 35
	b.mu.Unlock()
	check(5)
	recovered, err := newAPIGateway(g.root)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.close()
	if err := recovered.connect("g2", filepath.Join(g.root, "g2.sock")); err != nil {
		t.Fatal(err)
	}
	got, err := recovered.QueryStats(t.Context(), &stats.QueryStatsRequest{})
	if err != nil || len(got.Stat) != 1 || got.Stat[0].Value != 5 {
		t.Fatalf("recovered accounting: %v, %v", got, err)
	}
	if err := recovered.connect("g1", filepath.Join(g.root, "g1.sock")); err == nil {
		t.Fatal("reattached finalized counter source")
	}
	if a.resets != 0 || b.resets != 0 {
		t.Fatal("reset generation counters")
	}
}

func TestGatewayReplaysJournaledUserAfterPartialFailure(t *testing.T) {
	g, err := newAPIGateway(socketRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()
	a, b := attachGenerationAPI(t, g, "g1"), attachGenerationAPI(t, g, "g2")
	b.fail = true
	user := &protocol.User{Email: "test@example.invalid", Level: 1}
	request := &handler.AlterInboundRequest{Tag: "hy", Operation: serial.ToTypedMessage(&handler.AddUserOperation{User: user})}
	if _, err := g.AlterInbound(t.Context(), request); err == nil {
		t.Fatal("reported partially applied user as synchronized")
	}
	a.mu.Lock()
	added := a.users[user.Email] != nil
	a.mu.Unlock()
	if !added {
		t.Fatal("healthy generation was not updated")
	}
	recovered, err := newAPIGateway(g.root)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.close()
	b.mu.Lock()
	b.fail = false
	b.mu.Unlock()
	if err := recovered.connect("g2", filepath.Join(g.root, "g2.sock")); err != nil {
		t.Fatal(err)
	}
	if err := recovered.replay(t.Context(), recovered.clients["g2"]); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	added = b.users[user.Email] != nil
	b.mu.Unlock()
	if !added {
		t.Fatal("durable user was not replayed")
	}
	if _, err := g.AlterInbound(t.Context(), &handler.AlterInboundRequest{Tag: "hy", Operation: serial.ToTypedMessage(&handler.RemoveUserOperation{Email: user.Email})}); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	remainingA := len(a.users)
	a.mu.Unlock()
	b.mu.Lock()
	remainingB := len(b.users)
	b.mu.Unlock()
	if remainingA+remainingB != 0 {
		t.Fatal("user removal did not reach all live generations")
	}
}
