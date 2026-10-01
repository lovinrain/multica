package daemon

import (
	"context"
	"regexp"
	"strings"

	"github.com/multica-ai/multica/server/pkg/redact"
)

type transcriptScrubberKey struct{}

var credentialEnvKey = regexp.MustCompile(`(?i)(?:^|_)(?:API_KEY|API_SECRET|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIALS?|PRIVATE_KEY|ACCESS_KEY|DATABASE_URL|DB_URL|REDIS_URL)(?:_|$)`)

func newRunTranscriptScrubber(inherited []string, effective map[string]string) *redact.Scrubber {
	var values []string
	collect := func(key, value string) {
		upper := strings.ToUpper(key)
		// A credential filename is an address, not the credential it points at.
		if credentialEnvKey.MatchString(key) && !strings.HasSuffix(upper, "_FILE") && !strings.HasSuffix(upper, "_PATH") && value != "" {
			values = append(values, value)
		}
	}
	for _, entry := range inherited {
		if key, value, ok := strings.Cut(entry, "="); ok {
			collect(key, value)
		}
	}
	for key, value := range effective {
		collect(key, value)
	}
	return redact.New(values...)
}

func transcriptScrubberFromContext(ctx context.Context) *redact.Scrubber {
	if value, ok := ctx.Value(transcriptScrubberKey{}).(*redact.Scrubber); ok && value != nil {
		return value
	}
	return redact.New()
}

type redactedTranscriptError struct {
	source error
	safe   string
}

func (e redactedTranscriptError) Error() string { return e.safe }
func (e redactedTranscriptError) Unwrap() error { return e.source }
func scrubTranscriptError(scrubber *redact.Scrubber, err error) error {
	if err == nil {
		return nil
	}
	safe := scrubber.Text(err.Error())
	if safe == err.Error() {
		return err
	}
	return redactedTranscriptError{source: err, safe: safe}
}
