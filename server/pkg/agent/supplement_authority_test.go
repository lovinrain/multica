package agent

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSupplementAuthorityDefaultAndCancellation(t *testing.T) {
	if err := CheckSupplementAuthority(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := CheckSupplementAuthority(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSupplementAuthorityDenialDistinguishesPrewriteFromTransport(t *testing.T) {
	denied := errors.New("generation revoked")
	ctx := WithSupplementAuthority(t.Context(), func(context.Context) error { return denied })
	if err := CheckSupplementAuthority(ctx); !errors.Is(err, ErrSupplementAuthority) || !errors.Is(err, denied) {
		t.Fatalf("denial: %v", err)
	}
	ctx, cancel := context.WithCancel(WithSupplementAuthority(t.Context(), func(context.Context) error { t.Fatal("canceled gate called"); return nil }))
	cancel()
	if err := CheckSupplementAuthority(ctx); !errors.Is(err, ErrSupplementAuthority) || !errors.Is(err, context.Canceled) {
		t.Fatalf("coordinator cancellation: %v", err)
	}
	human, cancelHuman := context.WithCancel(t.Context())
	cancelHuman()
	if err := CheckSupplementAuthority(human); errors.Is(err, ErrSupplementAuthority) || !errors.Is(err, context.Canceled) {
		t.Fatalf("human cancellation: %v", err)
	}
}

func TestCodexSupplementAuthorityRejectsBeforeSteerWrite(t *testing.T) {
	c, stdin, _ := newTestCodexClient(t)
	c.threadID = "thread-current"
	c.setActiveTurnID("turn-current")
	denied := errors.New("coordinator generation revoked")
	checked := false
	ctx := WithSupplementAuthority(t.Context(), func(context.Context) error { checked = true; return denied })
	if err := supplementCodexTurn(ctx, c, "private coordinator input"); !errors.Is(err, denied) {
		t.Fatal(err)
	}
	if !checked || len(stdin.Lines()) != 0 || len(c.pending) != 0 {
		t.Fatal("unauthorized steer wrote input or retained pending RPC")
	}
}

func TestGrokSupplementAuthorityRejectsBeforeInterjectWrite(t *testing.T) {
	stdin := &fakeStdin{}
	c := &hermesClient{stdin: stdin, pending: make(map[int]*pendingRPC)}
	denied := errors.New("coordinator lease expired")
	ctx := WithSupplementAuthority(t.Context(), func(context.Context) error { return denied })
	_, err := c.request(ctx, "_x.ai/interject", map[string]any{"text": "private coordinator input"})
	if !errors.Is(err, denied) || len(stdin.Lines()) != 0 || len(c.pending) != 0 {
		t.Fatalf("err=%v lines=%v pending=%v", err, stdin.Lines(), c.pending)
	}
}

func TestClaudeHeldHookRechecksCoordinatorAuthorityAtWrite(t *testing.T) {
	for _, event := range []string{"PreToolUse", "Stop"} {
		for _, reason := range []string{"generation revoked", "lease expired"} {
			t.Run(event+"/"+reason, func(t *testing.T) {
				s := activeClaudeSupplementSession(t)
				denied := errors.New(reason)
				revoked := false
				checks := 0
				ctx := WithSupplementAuthority(t.Context(), func(context.Context) error {
					checks++
					if revoked {
						return denied
					}
					return nil
				})
				done := queueClaudeSupplement(t, s, ctx, "private coordinator input", 1)
				reply, handled := s.prepareHook(claudeSupplementHook(event, ""))
				if !handled || checks != 0 {
					t.Fatal("authority gate ran before held hook reached writer")
				}
				revoked = true
				var wire bytes.Buffer
				if err := reply(&wire); err != nil {
					t.Fatal(err)
				}
				if err := <-done; !errors.Is(err, denied) {
					t.Fatalf("supplement result: %v", err)
				}
				if checks != 1 || strings.Contains(wire.String(), "private coordinator input") || strings.Contains(wire.String(), "block") {
					t.Fatalf("rejected input reached hook: %s", wire.String())
				}
				if event == "Stop" && s.ready() {
					t.Fatal("rejected Stop input held admission open")
				}
			})
		}
	}
}
