// Package redact provides functions for detecting and masking secrets
// in agent output before it reaches the database or WebSocket broadcast.
package redact

import (
	"regexp"
	"sort"
	"strings"
)

// secretPattern pairs a compiled regex with its replacement text.
type secretPattern struct {
	re          *regexp.Regexp
	replacement string
}

// Patterns are checked in order; first match wins per position.
var patterns = []secretPattern{
	// Multica task and scoped external coordinator capabilities can appear bare.
	{regexp.MustCompile(`(?:mat|mxpc)_[A-Za-z0-9_-]{32,}`), "[REDACTED MULTICA TOKEN]"},

	// AWS access key IDs (always start with AKIA)
	{regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`), "[REDACTED AWS KEY]"},

	// AWS secret access keys (40 char base64-ish, preceded by a common separator)
	{regexp.MustCompile(`(?i)(?:aws_secret_access_key|secret_?access_?key)\s*[=:]\s*[A-Za-z0-9/+=]{40}`), "[REDACTED AWS SECRET]"},

	// PEM private keys (multi-line)
	{regexp.MustCompile(`(?s)-----BEGIN[A-Z\s]*PRIVATE KEY-----.*?-----END[A-Z\s]*PRIVATE KEY-----`), "[REDACTED PRIVATE KEY]"},

	// GitHub tokens (classic PAT, OAuth, user-to-server, server-to-server, refresh)
	{regexp.MustCompile(`\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{36,255}\b`), "[REDACTED GITHUB TOKEN]"},

	// GitHub fine-grained personal access tokens use the github_pat_ prefix,
	// which the classic ghp_/gho_/... pattern above does not cover. Without
	// this line a fine-grained PAT emitted in agent output leaks unredacted
	// to the database and WebSocket broadcast.
	{regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,255}\b`), "[REDACTED GITHUB TOKEN]"},

	// OpenAI / Anthropic API keys
	{regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}\b`), "[REDACTED API KEY]"},

	// Slack bot/user/legacy tokens. The char class includes 'e' so the
	// newer xoxe- config/refresh tokens are covered alongside xoxb/p/o/r/a/s.
	{regexp.MustCompile(`\bxox[bporase]-[A-Za-z0-9\-]{10,}\b`), "[REDACTED SLACK TOKEN]"},

	// Slack app-level tokens use the xapp- prefix, which the xox*- rule above
	// does not match. Without this an app-level token echoed in agent output
	// leaks unredacted to the DB / WebSocket broadcast.
	{regexp.MustCompile(`\bxapp-[A-Za-z0-9-]{10,}\b`), "[REDACTED SLACK TOKEN]"},

	// GitLab personal access tokens
	{regexp.MustCompile(`\bglpat-[A-Za-z0-9_-]{20,}\b`), "[REDACTED GITLAB TOKEN]"},

	// Google API keys always start with the AIza prefix and are 39 chars total
	// (AIza + 35). Capture and restore the trailing delimiter so keys ending in
	// a non-word character such as '-' are still redacted.
	{regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{35}([^0-9A-Za-z_-]|$)`), "[REDACTED GOOGLE API KEY]$1"},

	// Stripe secret / restricted live keys (sk_live_ / rk_live_). The sk-
	// rule above only matches the hyphen form used by OpenAI/Anthropic; Stripe
	// uses an underscore, so live keys are not covered without this. Publishable
	// keys (pk_live_) are intentionally excluded — they are not secret.
	{regexp.MustCompile(`\b(?:sk|rk)_live_[0-9A-Za-z]{16,}\b`), "[REDACTED STRIPE KEY]"},

	// JWT tokens (three base64url segments)
	{regexp.MustCompile(`\bey[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`), "[REDACTED JWT]"},

	// Generic "Bearer <token>" in output
	{regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9\-._~+/]+=*\b`), "Bearer [REDACTED]"},

	// Connection strings with embedded passwords
	{regexp.MustCompile(`(?i)(?:postgres|mysql|mongodb|redis|amqp)(?:ql)?://[^:\s]+:[^@\s]+@`), "[REDACTED CONNECTION STRING]@"},

	// Generic key=value patterns for common secret env var names
	{regexp.MustCompile(`(?i)(?:API_KEY|API_SECRET|SECRET_KEY|SECRET|ACCESS_TOKEN|AUTH_TOKEN|PRIVATE_KEY|DATABASE_URL|DB_PASSWORD|DB_URL|REDIS_URL|PASSWORD|TOKEN)\s*[=:]\s*\S+`), "[REDACTED CREDENTIAL]"},
}

// maxRedactDepth bounds the walk in redactValue. Tool inputs are decoded from
// daemon-supplied JSON, so nesting depth is attacker-influenced; without a
// bound a pathologically nested payload would recurse until the stack blows and
// take the process down. Real tool inputs nest a handful of levels at most, so
// this only ever trips on abuse.
const maxRedactDepth = 32

// depthLimitPlaceholder replaces anything below maxRedactDepth. Returning the
// raw value there would hand back an unscrubbed string, which is exactly what
// this package exists to prevent, so the fail-safe direction is to drop it.
const depthLimitPlaceholder = "[REDACTED DEPTH LIMIT]"

// InputMap returns a copy of m with every string value passed through Text,
// including strings nested inside maps and slices.
//
// The nested walk is load-bearing, not defensive tidying: providers record
// structured tool inputs, and Codex records a file edit as
// changes[]{path, diff, content}. A top-level-only pass leaves a credential
// inside a patch body — or the full contents of a deleted .env — untouched on
// its way to the database and the WebSocket broadcast.
func InputMap(m map[string]any) map[string]any {
	return redactMap(m, 0, Text)
}

func redactMap(m map[string]any, depth int, text func(string) string) map[string]any {
	if m == nil {
		return nil
	}
	if depth >= maxRedactDepth {
		return map[string]any{"_": depthLimitPlaceholder}
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = redactValue(v, depth+1, text)
	}
	return out
}

// redactValue scrubs a single decoded JSON value, recursing through the
// composite shapes json.Unmarshal produces plus []string, which providers use
// for argv-style inputs.
//
// Composites are copied rather than scrubbed in place: the caller still holds
// the original map and keeps using it after redaction (the daemon handler logs
// and re-reads it), so mutating through the shared reference would be a
// surprise at a distance.
func redactValue(v any, depth int, text func(string) string) any {
	if depth >= maxRedactDepth {
		return depthLimitPlaceholder
	}
	switch t := v.(type) {
	case string:
		return text(t)
	case map[string]any:
		return redactMap(t, depth, text)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = redactValue(e, depth+1, text)
		}
		return out
	case []string:
		out := make([]string, len(t))
		for i, e := range t {
			out[i] = text(e)
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(t))
		for k, e := range t {
			out[k] = text(e)
		}
		return out
	default:
		return v
	}
}

// Text scans the input string for known secret patterns and replaces
// matches with safe placeholders.
//
// It deliberately does NOT mask the local home directory. That masking used to
// live here, but it was never a boundary: anyone who can read a transcript
// already sees repository paths, file contents, commands and diffs, so hiding
// one path segment protected nothing while making paths unusable for copy-paste
// and debugging. It was also incoherent about whose home directory it hid —
// Content/Output are redacted in the server's ingest handler, so a hosted
// deployment matched them against the *server's* home, never the machine
// running the agent. Transcript visibility is an authorization concern and is
// handled at that layer, not by string replacement here.
func Text(s string) string {
	for _, p := range patterns {
		s = p.re.ReplaceAllString(s, p.replacement)
	}
	return s
}

// Scrubber adds exact, run-owned credential values to the general token rules.
// It is immutable and safe to share with transcript and terminal reporters.
type Scrubber struct {
	values   []string
	replacer *strings.Replacer
}

func New(values ...string) *Scrubber {
	unique := make(map[string]bool)
	for _, value := range values {
		if value != "" {
			unique[value] = true
		}
	}
	sorted := make([]string, 0, len(unique))
	for value := range unique {
		sorted = append(sorted, value)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if len(sorted[i]) == len(sorted[j]) {
			return sorted[i] < sorted[j]
		}
		return len(sorted[i]) > len(sorted[j])
	})
	pairs := make([]string, 0, len(sorted)*2)
	for _, value := range sorted {
		pairs = append(pairs, value, "[REDACTED CREDENTIAL]")
	}
	return &Scrubber{values: sorted, replacer: strings.NewReplacer(pairs...)}
}

func (s *Scrubber) Text(value string) string                     { return Text(s.replacer.Replace(value)) }
func (s *Scrubber) InputMap(value map[string]any) map[string]any { return redactMap(value, 0, s.Text) }

// Split emits only text that cannot be the start of a known credential. Keep
// its second return value across periodic transcript flushes and append the
// next delta before calling Split again. At a semantic frame boundary, Text
// settles the retained suffix. This prevents a tick between provider deltas
// from persisting the two halves of a credential in separate rows.
func (s *Scrubber) Split(value string) (safe, pending string) {
	var output strings.Builder
	for offset := 0; offset < len(value); {
		remaining := value[offset:]
		// Prefer a potential longer match to a complete shorter credential.
		// Waiting here is bounded by the longest registered value, even when
		// complete matches overlap with themselves or with one another.
		for _, secret := range s.values {
			if len(remaining) < len(secret) && strings.HasPrefix(secret, remaining) {
				return s.Text(output.String()), remaining
			}
		}
		matched := false
		for _, secret := range s.values {
			if strings.HasPrefix(remaining, secret) {
				output.WriteString("[REDACTED CREDENTIAL]")
				offset += len(secret)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		for _, prefix := range []string{"mat_", "mxpc_"} {
			if len(remaining) < len(prefix) && strings.HasPrefix(prefix, remaining) {
				return s.Text(output.String()), remaining
			}
			if !strings.HasPrefix(remaining, prefix) {
				continue
			}
			end := len(prefix)
			for end < len(remaining) && nativeTokenByte(remaining[end]) {
				end++
			}
			if end == len(remaining) && end <= 256 {
				return s.Text(output.String()), remaining
			}
			// Actual native capabilities fit below this cap. A malicious unbounded
			// token-shaped stream must not make the transcript retain unbounded data.
			if end > 256 {
				output.WriteString("[REDACTED MULTICA TOKEN]")
				offset += end
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		output.WriteByte(value[offset])
		offset++
	}
	return s.Text(output.String()), ""
}

func nativeTokenByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '-'
}
