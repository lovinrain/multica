package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestRunTranscriptScrubberCollectsOnlyCredentialValues(t *testing.T) {
	scrubber := newRunTranscriptScrubber([]string{"INHERITED_TOKEN=old-credential", "PATH=/normal/path", "MODE=1"}, map[string]string{"FAKE_API_KEY": "fixture-api-credential", "MULTICA_TOKEN": "task-credential", "TOKEN_FILE": "/token/file", "MUXPILOT_GENERATION": "1"})
	got := scrubber.Text("old-credential fixture-api-credential task-credential /normal/path /token/file generation1")
	if strings.Contains(got, "old-credential") || strings.Contains(got, "fixture-api-credential") || strings.Contains(got, "task-credential") {
		t.Fatalf("credential retained: %q", got)
	}
	if !strings.Contains(got, "/normal/path /token/file generation1") {
		t.Fatalf("noncredential environment altered: %q", got)
	}
	source := errors.New("fixture-api-credential failure")
	safe := scrubTranscriptError(scrubber, source)
	if strings.Contains(safe.Error(), "fixture-api-credential") || !errors.Is(safe, source) {
		t.Fatalf("error redaction or identity failed: %v", safe)
	}
}

type splitCredentialBackend struct {
	echo    string
	seen    chan string
	advance chan struct{}
}

func (b splitCredentialBackend) Execute(_ context.Context, prompt string, _ agent.ExecOptions) (*agent.Session, error) {
	b.seen <- prompt
	messages := make(chan agent.Message)
	results := make(chan agent.Result, 1)
	go func() {
		messages <- agent.Message{Type: agent.MessageText, Content: "before fixture-api-"}
		// Cross both the first-visible flush and a periodic 500ms flush.
		<-b.advance
		messages <- agent.Message{Type: agent.MessageText, Content: "credential after"}
		messages <- agent.Message{Type: agent.MessageToolUse, Tool: "patch_apply", Input: map[string]any{"changes": []any{map[string]any{"content": b.echo, "path": ".env"}}}}
		messages <- agent.Message{Type: agent.MessageToolResult, Tool: "patch_apply", Output: b.echo}
		messages <- agent.Message{Type: agent.MessageError, Content: b.echo}
		close(messages)
		results <- agent.Result{Status: "failed", Output: b.echo, Error: b.echo}
		close(results)
	}()
	return &agent.Session{Messages: messages, Result: results}, nil
}

func TestExecuteAndDrainRedactsCredentialsAcrossFlushesAndTerminalResult(t *testing.T) {
	t.Parallel()
	const secret = "fixture-api-credential"
	native := "mat_" + strings.Repeat("a", 40) + " mxpc_" + strings.Repeat("b", 64)
	echo := secret + " " + native
	ctx := context.WithValue(context.Background(), transcriptScrubberKey{}, newRunTranscriptScrubber(nil, map[string]string{"FAKE_API_KEY": secret}))
	d, rec := newTranscriptRecorder(t)
	backend := splitCredentialBackend{echo: echo, seen: make(chan string, 1), advance: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(backend.advance) })
	done := make(chan struct {
		result agent.Result
		err    error
	}, 1)
	go func() {
		result, _, err := d.executeAndDrain(ctx, backend, echo, agent.ExecOptions{}, slog.Default(), "task-redaction-flush", "", new(atomic.Int32))
		done <- struct {
			result agent.Result
			err    error
		}{result, err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(rec.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := rec.snapshot(); len(got) == 0 || got[0].Content != "before " {
		t.Fatalf("first-visible flush exposed candidate credential: %+v", got)
	}
	// Keep the provider blocked beyond the periodic tick and inspect storage
	// again before allowing the remaining credential bytes to arrive.
	time.Sleep(550 * time.Millisecond)
	if got := rec.snapshot(); len(got) != 1 || got[0].Content != "before " {
		t.Fatalf("periodic flush exposed candidate credential: %+v", got)
	}
	release.Do(func() { close(backend.advance) })
	completed := <-done
	result, err := completed.result, completed.err
	if err != nil {
		t.Fatal(err)
	}
	if got := <-backend.seen; got != echo {
		t.Fatal("provider prompt was changed")
	}
	messages := rec.snapshot()
	blob, _ := json.Marshal(messages)
	for _, credential := range []string{secret, "mat_" + strings.Repeat("a", 40), "mxpc_" + strings.Repeat("b", 64)} {
		if strings.Contains(string(blob), credential) || strings.Contains(result.Output, credential) || strings.Contains(result.Error, credential) {
			t.Fatalf("credential persisted or returned: %s", blob)
		}
	}
	var text strings.Builder
	for _, message := range messages {
		if message.Type == "text" {
			text.WriteString(message.Content)
		}
	}
	if got := text.String(); got != "before [REDACTED CREDENTIAL] after" {
		t.Fatalf("assembled transcript = %q", got)
	}
	if !strings.Contains(string(blob), ".env") {
		t.Fatal("tool metadata lost")
	}
}
