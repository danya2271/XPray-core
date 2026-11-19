package handoff

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type fakeControl struct {
	state    string
	commands []string
	drop     map[string]bool
	lost     bool
}

func (f *fakeControl) Command(ctx context.Context, command string) (Response, error) {
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	f.commands = append(f.commands, command)
	if f.lost {
		return Response{}, errors.New("unreachable")
	}
	switch command {
	case "pause":
		f.state = "paused"
	case "resume":
		f.state = "active"
	case "status":
	default:
		return Response{}, errors.New("unexpected command")
	}
	if f.drop[command] {
		return Response{}, errors.New("reply lost")
	}
	return Response{Protocol: ProtocolVersion, State: f.state}, nil
}

func TestSwitchAdmission(t *testing.T) {
	t.Run("waits for paused candidate", func(t *testing.T) {
		a := &fakeControl{state: "active"}
		b := &fakeControl{state: "starting"}
		_, err := Switch(t.Context(), a, b, func(context.Context) error { t.Fatal("probe called"); return nil })
		if err == nil || !reflect.DeepEqual(a.commands, []string{"status"}) {
			t.Fatalf("active generation was changed before B was ready: %v, %v", a.commands, err)
		}
	})
	t.Run("lost acknowledgments", func(t *testing.T) {
		a := &fakeControl{state: "active", drop: map[string]bool{"pause": true}}
		b := &fakeControl{state: "paused", drop: map[string]bool{"resume": true}}
		result, err := Switch(t.Context(), a, b, func(context.Context) error {
			if a.state != "paused" || b.state != "active" {
				t.Fatalf("invalid admission during probe: %q, %q", a.state, b.state)
			}
			return nil
		})
		if err != nil || result.Active != "candidate" || !reflect.DeepEqual(a.commands, []string{"status", "pause", "status"}) {
			t.Fatalf("lost ACK was not reconciled: %+v, %v, %v", result, err, a.commands)
		}
	})
	t.Run("cancelled probe rolls back", func(t *testing.T) {
		a, b := &fakeControl{state: "active"}, &fakeControl{state: "paused"}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result, err := Switch(ctx, a, b, func(context.Context) error { cancel(); return nil })
		if !errors.Is(err, context.Canceled) || !result.RolledBack || a.state != "active" || b.state != "paused" {
			t.Fatalf("cancel failed to restore admission: %+v, %v, %s, %s", result, err, a.state, b.state)
		}
	})
	t.Run("unknown candidate is not false rollback", func(t *testing.T) {
		a, b := &fakeControl{state: "active"}, &fakeControl{state: "paused"}
		result, err := Switch(t.Context(), a, b, func(context.Context) error {
			b.lost = true
			return errors.New("probe failed")
		})
		if err == nil || result.RolledBack || result.Active != "unknown" || a.state != "paused" || !reflect.DeepEqual(a.commands, []string{"status", "pause"}) {
			t.Fatalf("unknown candidate allowed conflicting admission: %+v, %v, %v", result, err, a.commands)
		}
	})
}
