package handoff

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"time"
)

type Control interface {
	Command(ctx context.Context, command string) (Response, error)
}

type ControlSocket string

func (s ControlSocket) Command(ctx context.Context, command string) (Response, error) {
	var response Response
	transport := net.Dialer{Timeout: 2 * time.Second}
	conn, err := transport.DialContext(ctx, "unix", string(s))
	if err != nil {
		return response, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline := time.Now().Add(3 * time.Second)
	if earlier, exists := ctx.Deadline(); exists && earlier.Before(deadline) {
		deadline = earlier
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return response, err
	}
	if _, err := fmt.Fprintln(conn, command); err != nil {
		return response, err
	}
	if err := json.NewDecoder(io.LimitReader(conn, 4096)).Decode(&response); err != nil {
		return response, err
	}
	if response.Protocol != ProtocolVersion {
		return response, fmt.Errorf("unsupported generation control protocol %d", response.Protocol)
	}
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}

type SwitchResult struct {
	// Active is "candidate", "previous", or "unknown" (retain both).
	Active     string
	RolledBack bool
}

// LinkXHTTP prepares both directions without changing either admission gate.
// A second call with the same paths is harmless, including after a lost ACK.
func LinkXHTTP(ctx context.Context, previous, candidate ControlSocket) error {
	if err := expect(ctx, candidate, "link "+filepath.Dir(string(previous)), "paused"); err != nil {
		return fmt.Errorf("link candidate to previous XHTTP generation: %w", err)
	}
	return expect(ctx, previous, "link "+filepath.Dir(string(candidate)), "active")
}

// Switch verifies both readiness states, pauses A, resumes B, and probes the
// public data path. The probe must create new work and prove it reached B.
// No process or existing stream is stopped, even on rollback. Drain is a
// separate, irreversible operation to initiate after the caller confirms
// the switch and records the new active generation.
func Switch(ctx context.Context, previous, candidate Control, verify func(context.Context) error) (SwitchResult, error) {
	return switchAdmission(ctx, previous, candidate, nil, verify)
}

// SwitchWithIngress also changes the supervisor-owned UDP admission selector.
// The selector is committed before probing public traffic. On rollback it is
// restored without deleting either generation's established CID routes.
// apply must be idempotent; an error does not prove admission was unchanged.
func SwitchWithIngress(ctx context.Context, previous, candidate Control, apply func(candidate bool) error, verify func(context.Context) error) (SwitchResult, error) {
	if apply == nil {
		return SwitchResult{Active: "unknown"}, errors.New("UDP switch requires an ingress selector")
	}
	return switchAdmission(ctx, previous, candidate, apply, verify)
}

func switchAdmission(ctx context.Context, previous, candidate Control, apply func(candidate bool) error, verify func(context.Context) error) (SwitchResult, error) {
	unknown := SwitchResult{Active: "unknown"}
	if verify == nil {
		return unknown, errors.New("switch requires a candidate data-path probe")
	}
	oldStatus, err := commandState(ctx, previous, "status", "active")
	if err != nil {
		return unknown, fmt.Errorf("old generation is not active: %w", err)
	}
	newStatus, err := commandState(ctx, candidate, "status", "paused")
	if err != nil {
		return unknown, fmt.Errorf("candidate is not paused and ready: %w", err)
	}
	if oldStatus.UDPManaged != newStatus.UDPManaged || oldStatus.UDPManaged != (apply != nil) {
		return unknown, errors.New("UDP admission selector must match both generation manifests")
	}
	if err := move(ctx, previous, "pause", "paused"); err != nil {
		rollback, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		// B has not been activated. Restoring admission on A cannot race a
		// live B, even if the pause reply was lost.
		if restore := move(rollback, previous, "resume", "active"); restore != nil {
			return unknown, errors.Join(err, fmt.Errorf("could not restore admission on previous generation: %w", restore))
		}
		return SwitchResult{Active: "previous"}, err
	}
	err = move(ctx, candidate, "resume", "active")
	if err == nil && apply != nil {
		err = apply(true)
	}
	if err == nil {
		err = verify(ctx)
	}
	if err == nil && ctx.Err() == nil {
		return SwitchResult{Active: "candidate"}, nil
	}
	if err == nil {
		err = ctx.Err()
	}
	rollback, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if pauseErr := move(rollback, candidate, "pause", "paused"); pauseErr != nil {
		return unknown, errors.Join(err, fmt.Errorf("cannot prove candidate is paused; retained both generations: %w", pauseErr))
	}
	if resumeErr := move(rollback, previous, "resume", "active"); resumeErr != nil {
		return unknown, errors.Join(err, fmt.Errorf("could not restore previous generation: %w", resumeErr))
	}
	if apply != nil {
		if restore := apply(false); restore != nil {
			return unknown, errors.Join(err, fmt.Errorf("could not restore UDP ingress: %w", restore))
		}
	}
	return SwitchResult{Active: "previous", RolledBack: true}, err
}

func expect(ctx context.Context, endpoint Control, command, expected string) error {
	_, err := commandState(ctx, endpoint, command, expected)
	return err
}

func commandState(ctx context.Context, endpoint Control, command, expected string) (Response, error) {
	response, err := endpoint.Command(ctx, command)
	if err != nil {
		return response, fmt.Errorf("%s: %w", command, err)
	}
	if response.Protocol != ProtocolVersion || response.Error != "" || response.State != expected {
		return response, fmt.Errorf("%s: expected %q, got state %q (protocol %d, %s)", command, expected, response.State, response.Protocol, response.Error)
	}
	return response, nil
}

func move(ctx context.Context, endpoint Control, command, expected string) error {
	err := expect(ctx, endpoint, command, expected)
	if err == nil {
		return nil
	}
	// The command may have applied even if the connection failed before the
	// ACK; never infer a generation's state from an I/O failure alone.
	if statusErr := expect(ctx, endpoint, "status", expected); statusErr != nil {
		return errors.Join(err, statusErr)
	}
	return nil
}
