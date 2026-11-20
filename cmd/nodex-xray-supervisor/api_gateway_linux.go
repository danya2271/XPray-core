//go:build linux && !android

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	logger "github.com/xtls/xray-core/app/log/command"
	handler "github.com/xtls/xray-core/app/proxyman/command"
	stats "github.com/xtls/xray-core/app/stats/command"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type desiredUser struct {
	Tag   string `json:"tag"`
	Email string `json:"email"`
	User  []byte `json:"user,omitempty"` // nil is a durable removal tombstone.
}

type gatewayJournal struct {
	Users     map[string]desiredUser      `json:"users"`
	Counters  map[string]map[string]int64 `json:"counters"`
	Baselines map[string]int64            `json:"baselines"`
	Finalized map[string]bool             `json:"finalized"`
}

type apiGateway struct {
	handler.UnimplementedHandlerServiceServer
	stats.UnimplementedStatsServiceServer
	logger.UnimplementedLoggerServiceServer
	mu      sync.Mutex
	root    string
	journal gatewayJournal
	clients map[string]*grpc.ClientConn
	active  string
	server  *grpc.Server
}

func newAPIGateway(root string) (*apiGateway, error) {
	g := &apiGateway{root: root, clients: make(map[string]*grpc.ClientConn), journal: gatewayJournal{Users: make(map[string]desiredUser), Counters: make(map[string]map[string]int64), Baselines: make(map[string]int64), Finalized: make(map[string]bool)}}
	data, err := os.ReadFile(filepath.Join(root, "management.json"))
	if err == nil {
		if err := json.Unmarshal(data, &g.journal); err != nil {
			return nil, err
		}
		if g.journal.Users == nil || g.journal.Counters == nil || g.journal.Baselines == nil || g.journal.Finalized == nil {
			return nil, errors.New("incomplete management journal")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return g, nil
}

func (g *apiGateway) persist() error {
	data, err := json.Marshal(g.journal)
	if err != nil {
		return err
	}
	path := filepath.Join(g.root, ".management.new")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	syncErr, closeErr := f.Sync(), f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(path, filepath.Join(g.root, "management.json")); err != nil {
		return err
	}
	dir, err := os.Open(g.root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (g *apiGateway) serve(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	g.server = grpc.NewServer()
	handler.RegisterHandlerServiceServer(g.server, g)
	stats.RegisterStatsServiceServer(g.server, g)
	logger.RegisterLoggerServiceServer(g.server, g)
	go g.server.Serve(listener)
	return nil
}

func (g *apiGateway) connect(id, path string) error {
	if g.journal.Finalized[id] {
		return errors.New("cannot reattach finalized generation API")
	}
	if g.clients[id] != nil {
		return nil
	}
	connection, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	g.clients[id] = connection
	return nil
}

func (g *apiGateway) close() {
	if g.server != nil {
		g.server.Stop()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, client := range g.clients {
		client.Close()
	}
}

func applyDesiredUser(ctx context.Context, connection *grpc.ClientConn, desired desiredUser) error {
	client := handler.NewHandlerServiceClient(connection)
	current, err := client.GetInboundUsers(ctx, &handler.GetInboundUserRequest{Tag: desired.Tag, Email: desired.Email})
	if err != nil {
		return err
	}
	var matching []*protocol.User
	for _, user := range current.Users {
		if user != nil && user.Email == desired.Email {
			matching = append(matching, user)
		}
	}
	current.Users = matching
	if len(desired.User) == 0 {
		if len(current.Users) == 0 {
			return nil
		}
		_, err = client.AlterInbound(ctx, &handler.AlterInboundRequest{Tag: desired.Tag, Operation: serial.ToTypedMessage(&handler.RemoveUserOperation{Email: desired.Email})})
		return err
	}
	var user protocol.User
	if err := proto.Unmarshal(desired.User, &user); err != nil {
		return err
	}
	if len(current.Users) == 1 && proto.Equal(current.Users[0], &user) {
		return nil
	}
	if len(current.Users) != 0 {
		if _, err := client.AlterInbound(ctx, &handler.AlterInboundRequest{Tag: desired.Tag, Operation: serial.ToTypedMessage(&handler.RemoveUserOperation{Email: desired.Email})}); err != nil {
			return err
		}
	}
	_, err = client.AlterInbound(ctx, &handler.AlterInboundRequest{Tag: desired.Tag, Operation: serial.ToTypedMessage(&handler.AddUserOperation{User: &user})})
	return err
}

func (g *apiGateway) replay(ctx context.Context, connection *grpc.ClientConn) error {
	keys := make([]string, 0, len(g.journal.Users))
	for key := range g.journal.Users {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := applyDesiredUser(ctx, connection, g.journal.Users[key]); err != nil {
			return err
		}
	}
	return nil
}

func (g *apiGateway) AlterInbound(ctx context.Context, request *handler.AlterInboundRequest) (*handler.AlterInboundResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if request.Operation == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "missing user operation")
	}
	operation, err := request.Operation.GetInstance()
	if err != nil {
		return nil, err
	}
	desired := desiredUser{Tag: request.Tag}
	switch operation := operation.(type) {
	case *handler.AddUserOperation:
		if operation.User == nil || operation.User.Email == "" {
			return nil, grpcstatus.Error(codes.InvalidArgument, "managed users need an email identity")
		}
		desired.Email = operation.User.Email
		desired.User, err = proto.Marshal(operation.User)
	case *handler.RemoveUserOperation:
		desired.Email = operation.Email
	default:
		return nil, grpcstatus.Error(codes.Unimplemented, "managed API only accepts user mutations")
	}
	if err != nil {
		return nil, err
	}
	if desired.Tag == "" || desired.Email == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "missing user identity")
	}
	key := desired.Tag + "\x00" + desired.Email
	previous, existed := g.journal.Users[key]
	g.journal.Users[key] = desired
	if err := g.persist(); err != nil {
		if existed {
			g.journal.Users[key] = previous
		} else {
			delete(g.journal.Users, key)
		}
		return nil, err
	}
	if len(g.clients) == 0 {
		return nil, grpcstatus.Error(codes.Unavailable, "no live generation API; mutation retained for replay")
	}
	var failures []error
	for id, connection := range g.clients {
		if err := applyDesiredUser(ctx, connection, desired); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", id, err))
		}
	}
	if err := errors.Join(failures...); err != nil {
		return nil, grpcstatus.Error(codes.Unavailable, "mutation journaled; generations pending replay: "+err.Error())
	}
	return &handler.AlterInboundResponse{}, nil
}

func (g *apiGateway) current() (*grpc.ClientConn, error) {
	connection := g.clients[g.active]
	if connection == nil {
		return nil, grpcstatus.Error(codes.Unavailable, "active generation API unavailable")
	}
	return connection, nil
}

func (g *apiGateway) GetInboundUsers(ctx context.Context, request *handler.GetInboundUserRequest) (*handler.GetInboundUserResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	client, err := g.current()
	if err != nil {
		return nil, err
	}
	return handler.NewHandlerServiceClient(client).GetInboundUsers(ctx, request)
}
func (g *apiGateway) GetInboundUsersCount(ctx context.Context, request *handler.GetInboundUserRequest) (*handler.GetInboundUsersCountResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	client, err := g.current()
	if err != nil {
		return nil, err
	}
	return handler.NewHandlerServiceClient(client).GetInboundUsersCount(ctx, request)
}
func (g *apiGateway) ListInbounds(ctx context.Context, request *handler.ListInboundsRequest) (*handler.ListInboundsResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	client, err := g.current()
	if err != nil {
		return nil, err
	}
	return handler.NewHandlerServiceClient(client).ListInbounds(ctx, request)
}
func (g *apiGateway) RestartLogger(ctx context.Context, request *logger.RestartLoggerRequest) (*logger.RestartLoggerResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, client := range g.clients {
		if _, err := logger.NewLoggerServiceClient(client).RestartLogger(ctx, request); err != nil {
			return nil, err
		}
	}
	return &logger.RestartLoggerResponse{}, nil
}

func addCounter(sum, value int64) (int64, error) {
	if value < 0 || sum > math.MaxInt64-value {
		return 0, errors.New("invalid or overflowing generation counter")
	}
	return sum + value, nil
}

func (g *apiGateway) collect(ctx context.Context) error {
	for id, connection := range g.clients {
		response, err := stats.NewStatsServiceClient(connection).QueryStats(ctx, &stats.QueryStatsRequest{})
		if err != nil {
			return fmt.Errorf("%s counters: %w", id, err)
		}
		previous := g.journal.Counters[id]
		if previous == nil {
			previous = make(map[string]int64)
		}
		for _, counter := range response.Stat {
			if counter.Value < previous[counter.Name] {
				return errors.New("generation counter regressed; refusing to lose accounting")
			}
			previous[counter.Name] = counter.Value
		}
		g.journal.Counters[id] = previous
	}
	return g.persist()
}

func (g *apiGateway) totals() (map[string]int64, error) {
	totals := make(map[string]int64)
	for _, counters := range g.journal.Counters {
		for name, value := range counters {
			sum, err := addCounter(totals[name], value)
			if err != nil {
				return nil, err
			}
			totals[name] = sum
		}
	}
	return totals, nil
}

func (g *apiGateway) QueryStats(ctx context.Context, request *stats.QueryStatsRequest) (*stats.QueryStatsResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.collect(ctx); err != nil {
		return nil, err
	}
	totals, err := g.totals()
	if err != nil {
		return nil, err
	}
	response := &stats.QueryStatsResponse{}
	previousBaselines := make(map[string]int64, len(g.journal.Baselines))
	for name, value := range g.journal.Baselines {
		previousBaselines[name] = value
	}
	for name, total := range totals {
		if strings.Contains(name, request.Pattern) {
			response.Stat = append(response.Stat, &stats.Stat{Name: name, Value: total - g.journal.Baselines[name]})
			if request.Reset_ {
				g.journal.Baselines[name] = total
			}
		}
	}
	sort.Slice(response.Stat, func(i, j int) bool { return response.Stat[i].Name < response.Stat[j].Name })
	if request.Reset_ {
		if err := g.persist(); err != nil {
			g.journal.Baselines = previousBaselines
			return nil, err
		}
	}
	return response, nil
}

func (g *apiGateway) GetStats(ctx context.Context, request *stats.GetStatsRequest) (*stats.GetStatsResponse, error) {
	// GetStats must match exactly; never reset similarly named counters.
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.collect(ctx); err != nil {
		return nil, err
	}
	totals, err := g.totals()
	if err != nil {
		return nil, err
	}
	total, exists := totals[request.Name]
	if !exists {
		return nil, grpcstatus.Error(codes.NotFound, "counter not found")
	}
	response := &stats.GetStatsResponse{Stat: &stats.Stat{Name: request.Name, Value: total - g.journal.Baselines[request.Name]}}
	if request.Reset_ {
		previous, existed := g.journal.Baselines[request.Name]
		g.journal.Baselines[request.Name] = total
		if err := g.persist(); err != nil {
			if existed {
				g.journal.Baselines[request.Name] = previous
			} else {
				delete(g.journal.Baselines, request.Name)
			}
			return nil, err
		}
	}
	return response, nil
}

func (g *apiGateway) finalize(ctx context.Context, id string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.journal.Finalized[id] {
		return nil
	}
	if g.clients[id] == nil {
		return errors.New("retiring generation API is unavailable for final accounting")
	}
	if err := g.collect(ctx); err != nil {
		return err
	}
	g.journal.Finalized[id] = true
	if err := g.persist(); err != nil {
		delete(g.journal.Finalized, id)
		return err
	}
	g.clients[id].Close()
	delete(g.clients, id)
	return nil
}

func (g *apiGateway) bootstrap(ctx context.Context, candidate *generation, tags []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.connect(candidate.id, filepath.Join(candidate.dir, "api.sock")); err != nil {
		return err
	}
	client := g.clients[candidate.id]
	if g.active != "" {
		active, err := g.current()
		if err != nil {
			return err
		}
		for _, tag := range tags {
			response, err := handler.NewHandlerServiceClient(active).GetInboundUsers(ctx, &handler.GetInboundUserRequest{Tag: tag})
			if err != nil {
				return err
			}
			for _, user := range response.Users {
				if user == nil || user.Email == "" {
					return errors.New("managed user has no stable email")
				}
				key := tag + "\x00" + user.Email
				if _, exists := g.journal.Users[key]; exists {
					continue
				}
				data, err := proto.Marshal(user)
				if err != nil {
					return err
				}
				desired := desiredUser{Tag: tag, Email: user.Email, User: data}
				g.journal.Users[key] = desired
			}
			// A stale candidate config is not authoritative. Remove users
			// absent from the live active generation unless an unapplied
			// journal entry explicitly requires their presence.
			candidateUsers, err := handler.NewHandlerServiceClient(client).GetInboundUsers(ctx, &handler.GetInboundUserRequest{Tag: tag})
			if err != nil {
				return err
			}
			present := make(map[string]bool)
			for _, user := range response.Users {
				if user != nil {
					present[user.Email] = true
				}
			}
			for _, user := range candidateUsers.Users {
				if user == nil || user.Email == "" {
					return errors.New("candidate user has no stable email")
				}
				key := tag + "\x00" + user.Email
				if !present[user.Email] {
					if _, exists := g.journal.Users[key]; !exists {
						g.journal.Users[key] = desiredUser{Tag: tag, Email: user.Email}
					}
				}
			}
		}
		if err := g.persist(); err != nil {
			return err
		}
	}
	return g.replay(ctx, client)
}

func apiContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 5*time.Second)
}
