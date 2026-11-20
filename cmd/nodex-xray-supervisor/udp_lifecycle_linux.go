//go:build linux && !android

package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

func (s *supervisor) udpToken(purpose string, generation *generation, tag string) [16]byte {
	input := fmt.Sprintf("%x\x00%s\x00%s\x00%s", s.broker.nonce, purpose, generation.id, tag)
	sum := sha256.Sum256([]byte(input))
	return [16]byte(sum[:16])
}

func (s *supervisor) adoptUDPReceive(ctx context.Context, owner *generation) error {
	for _, b := range s.bindings {
		if b.transport != "hysteria" || !b.native {
			continue
		}
		state, _, err := s.broker.command(ctx, brokerRequest{Operation: "status", Tag: b.tag})
		if err != nil {
			return err
		}
		if !state.Direct {
			continue
		}
		response, err := owner.control.Command(ctx, fmt.Sprintf("release-receive %s:%d", b.tag, b.port))
		if err != nil {
			return err
		}
		if len(response.Receive) == 0 {
			return errors.New("missing public UDP ownership snapshot")
		}
		if _, _, err := s.broker.command(ctx, brokerRequest{Operation: "adopt", Tag: b.tag, Snapshot: response.Receive}); err != nil {
			return err
		}
	}
	return nil
}

func (s *supervisor) activateUDP(ctx context.Context, active *generation) error {
	for _, b := range s.bindings {
		if b.transport != "hysteria" {
			continue
		}
		if b.native {
			state, _, err := s.broker.command(ctx, brokerRequest{Operation: "status", Tag: b.tag})
			if err != nil {
				return err
			}
			if state.Direct && state.Current == active.slot {
				continue
			}
		}
		if _, _, err := s.broker.command(ctx, brokerRequest{Operation: "activate", Tag: b.tag, Slot: active.slot}); err != nil {
			return err
		}
	}
	return nil
}

func (s *supervisor) drainUDP(active, old *generation) error {
	state, err := old.control.Command(s.ctx, "status")
	if err != nil {
		return err
	}
	if state.State == "draining" || state.State == "drained" {
		return nil
	}
	for _, b := range s.bindings {
		if b.transport != "hysteria" {
			continue
		}
		token := s.udpToken("drain", old, b.tag)
		for {
			if !active.running() || !old.running() {
				return errors.New("generation exited during UDP drain")
			}
			response, _, err := s.broker.command(s.ctx, brokerRequest{Operation: "fence", Tag: b.tag, Slot: old.slot, Token: token})
			if err != nil {
				return err
			}
			if response.Ready {
				drain, err := old.control.Command(s.ctx, fmt.Sprintf("drain-udp %s:%d %x", b.tag, b.port, token))
				if err != nil {
					return err
				}
				if drain.State == "udp-drained" {
					break
				}
				if drain.State != "udp-waiting" {
					return fmt.Errorf("unexpected UDP drain state %q", drain.State)
				}
			}
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}

func (s *supervisor) returnUDPDirect(active, old *generation) error {
	for _, b := range s.bindings {
		if b.transport != "hysteria" {
			continue
		}
		drain := s.udpToken("drain", old, b.tag)
		for {
			retired, _, err := s.broker.command(s.ctx, brokerRequest{Operation: "retire", Tag: b.tag, Slot: old.slot, Token: drain})
			if err != nil {
				return err
			}
			if retired.Ready {
				break
			}
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
		if !b.native {
			continue
		}
		token := s.udpToken("acquire-after-"+old.id, active, b.tag)
		for {
			response, _, err := s.broker.command(s.ctx, brokerRequest{Operation: "release", Tag: b.tag, Slot: active.slot, Token: token})
			if err != nil {
				return err
			}
			if response.Ready {
				break
			}
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		}
		for {
			response, err := active.control.Command(s.ctx, fmt.Sprintf("acquire-receive %s:%d %x", b.tag, b.port, token))
			if err != nil {
				return err
			}
			if response.State == "receive-direct" {
				break
			}
			if response.State != "receive-waiting" {
				return fmt.Errorf("unexpected UDP receive state %q", response.State)
			}
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	return nil
}
