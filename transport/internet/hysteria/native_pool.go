//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"context"
	"errors"
	stdnet "net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

const nativePoolIdle = 30 * time.Second
const nativeSessionFlows = 64
const nativeGlobalSessions = 16

var nativeSessionSlots atomic.Int32

type nativePoolKey struct {
	instance *core.Instance
	settings *internet.MemoryStreamConfig
	dest     net.Destination
	limit    int
	bridge   string
}

type nativeSession struct {
	engine *native.Engine
	users  int
	idle   time.Time
}

type nativePool struct {
	mu       sync.Mutex
	key      nativePoolKey
	sessions []*nativeSession
	closed   bool
	used     time.Time
	changed  chan struct{}
	creating int
}

type nativePools struct {
	sync.Mutex
	entries map[nativePoolKey]*nativePool
	once    sync.Once
}

var pooledNative = nativePools{entries: make(map[nativePoolKey]*nativePool)}

func nativePoolLimit(config native.Config) (int, error) {
	limit := 2
	if value := os.Getenv("XRAY_HYSTERIA_POOL_SIZE"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || n > 8 {
			return 0, errors.New("XRAY_HYSTERIA_POOL_SIZE must be 0..8")
		}
		limit = n
	}
	// Fixed negotiated rates belong to one QUIC connection. Do not multiply a
	// user's Brutal bandwidth allowance by the number of pool shards.
	// An advertised RX rate can enable server-side Brutal even with local BBR.
	fixedTX := (config.Congestion == "" || config.Congestion == "brutal" || config.Congestion == "force-brutal") && config.TxBPS != 0
	if limit > 1 && (fixedTX || config.RxBPS != 0) {
		limit = 1
	}
	return limit, nil
}

func (m *nativePools) get(key nativePoolKey) *nativePool {
	m.once.Do(func() { go m.clean() })
	m.Lock()
	defer m.Unlock()
	if pool := m.entries[key]; pool != nil {
		return pool
	}
	pool := &nativePool{key: key, used: time.Now(), changed: make(chan struct{})}
	m.entries[key] = pool
	return pool
}

func (m *nativePools) clean() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for now := range ticker.C {
		var closeEngines []*native.Engine
		m.Lock()
		for key, pool := range m.entries {
			pool.mu.Lock()
			stopped := key.instance != nil && !key.instance.IsRunning()
			kept := pool.sessions[:0]
			for _, session := range pool.sessions {
				dead := false
				select {
				case <-session.engine.Done():
					dead = true
				default:
				}
				if stopped || dead || session.users == 0 && now.Sub(session.idle) >= nativePoolIdle {
					closeEngines = append(closeEngines, session.engine)
				} else {
					kept = append(kept, session)
				}
			}
			clear(pool.sessions[len(kept):])
			pool.sessions = kept
			close(pool.changed)
			pool.changed = make(chan struct{})
			if stopped || len(kept) == 0 && pool.creating == 0 && now.Sub(pool.used) >= nativePoolIdle {
				pool.closed = true
				delete(m.entries, key)
			}
			pool.mu.Unlock()
		}
		m.Unlock()
		for _, engine := range closeEngines {
			engine.Close()
		}
	}
}

func (p *nativePool) open(ctx context.Context, target net.Destination, create func(context.Context) (*native.Engine, error)) (*pooledNativeConn, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
retry:
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, stdnet.ErrClosed
	}
	p.used = time.Now()
	kept := p.sessions[:0]
	for _, session := range p.sessions {
		select {
		case <-session.engine.Done():
			go session.engine.Close()
		default:
			kept = append(kept, session)
		}
	}
	clear(p.sessions[len(kept):])
	p.sessions = kept
	var selected *nativeSession
	for _, session := range p.sessions {
		select {
		case <-session.engine.Done():
			continue
		default:
		}
		if session.users < nativeSessionFlows && (selected == nil || session.users < selected.users) {
			selected = session
		}
	}
	grow := (selected == nil || selected.users != 0) && len(p.sessions)+p.creating < p.key.limit
	if grow && nativeSessionSlots.Add(1) > nativeGlobalSessions {
		nativeSessionSlots.Add(-1)
		if selected == nil {
			p.mu.Unlock()
			return nil, errors.New("native global session budget reached (16 sessions)")
		}
		// The global budget limits new sessions, not streams on existing ones.
		grow = false
	}
	if grow {
		p.creating++
		p.mu.Unlock()
		engine, err := create(ctx)
		p.mu.Lock()
		p.creating--
		close(p.changed)
		p.changed = make(chan struct{})
		if err != nil {
			nativeSessionSlots.Add(-1)
			p.mu.Unlock()
			return nil, err
		}
		if p.closed || p.key.instance != nil && !p.key.instance.IsRunning() {
			p.mu.Unlock()
			engine.Close()
			nativeSessionSlots.Add(-1)
			return nil, stdnet.ErrClosed
		}
		selected = &nativeSession{engine: engine}
		p.sessions = append(p.sessions, selected)
		// A closed bridge must be joined even if no more dials occur.
		go func() { <-engine.Done(); engine.Close(); nativeSessionSlots.Add(-1) }()
	}
	if selected == nil {
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
			goto retry
		}
	}
	selected.users++
	p.mu.Unlock()
	release := func() {
		p.mu.Lock()
		selected.users--
		selected.idle = time.Now()
		p.used = selected.idle
		close(p.changed)
		p.changed = make(chan struct{})
		p.mu.Unlock()
	}
	conn, err := selected.engine.Open(ctx, target)
	if err != nil {
		release()
		return nil, err
	}
	return &pooledNativeConn{Conn: conn, release: release}, nil
}

type pooledNativeConn struct {
	*native.Conn
	once    sync.Once
	release func()
	err     error
}

func (c *pooledNativeConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); c.release() })
	return c.err
}
