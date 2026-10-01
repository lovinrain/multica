//go:build agentintegration

package agent

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMuxpilotCodexPairedSmoke delegates the natural coordinator workflow to a
// private, explicitly supplied release driver. Default tests never inspect an
// agent executable, login, account or this driver's configuration.
func TestMuxpilotCodexPairedSmoke(t *testing.T) {
	if os.Getenv("MULTICA_RUN_REAL_AGENT_SMOKE") != "1" {
		t.Skip("explicit real-agent smoke authorization is required")
	}
	script := os.Getenv("MULTICA_MUXPILOT_SMOKE_SCRIPT")
	reportPath := os.Getenv("MULTICA_MUXPILOT_SMOKE_REPORT")
	if !filepath.IsAbs(script) || !filepath.IsAbs(reportPath) {
		t.Fatal("absolute private smoke driver and report paths are required")
	}
	info, err := os.Stat(script)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 {
		t.Fatal("smoke driver must exist and not be writable by another account")
	}
	// This lookup is deliberately below the authorization gate.
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required by the paired driver")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, script)
	output, runErr := cmd.CombinedOutput()
	logPath := reportPath + ".driver.log"
	if err := os.WriteFile(logPath, output, 0600); err != nil {
		t.Fatal("cannot retain private smoke driver output")
	}
	_ = os.Chmod(logPath, 0600)
	t.Logf("Private driver log: %s", logPath)
	if runErr != nil {
		t.Fatalf("paired driver did not complete: %v; inspect its private report/log", runErr)
	}
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal("paired driver did not produce its report")
	}
	var report struct {
		RunID             string  `json:"run_id"`
		SchemaVersion     int     `json:"schema_version"`
		Passed            bool    `json:"passed"`
		Provider          string  `json:"provider"`
		Model             string  `json:"model"`
		WorkerCount       int     `json:"worker_count"`
		DurationSeconds   float64 `json:"duration_seconds"`
		RepositoryPrivate bool    `json:"repository_private"`
		DraftPRURL        string  `json:"draft_pr_url"`
		RepoRoot          string  `json:"repo_root"`
		Checks            []struct {
			Name   string `json:"name"`
			Passed bool   `json:"passed"`
		} `json:"checks"`
		EvidenceFiles []string `json:"evidence_files"`
		HelperPolicy  struct {
			MultiAgent *bool  `json:"multi_agent"`
			Verified   bool   `json:"verified"`
			Version    string `json:"version"`
		} `json:"helper_policy"`
	}
	if json.Unmarshal(raw, &report) != nil {
		t.Fatal("paired smoke report is not valid JSON")
	}
	if report.SchemaVersion != 1 || !report.Passed || report.Provider != "codex" || report.Model != "gpt-6.1-sol" || report.WorkerCount < 1 || report.WorkerCount > 3 || report.DurationSeconds <= 0 || report.DurationSeconds > 600 || !report.RepositoryPrivate || !filepath.IsAbs(report.RepoRoot) {
		t.Fatal("paired smoke report does not satisfy the authorized provider, model, worker, time and private-repository limits")
	}
	pr, err := url.Parse(report.DraftPRURL)
	if err != nil || pr.Scheme != "https" || pr.Host != "github.com" || !strings.Contains(pr.Path, "/pull/") {
		t.Fatal("paired smoke must identify its verified draft pull request")
	}
	if report.HelperPolicy.MultiAgent == nil || *report.HelperPolicy.MultiAgent || !report.HelperPolicy.Verified || report.HelperPolicy.Version == "" {
		t.Fatal("paired smoke lacks verified disabled internal multi-agent policy")
	}
	if len(report.Checks) == 0 || len(report.EvidenceFiles) == 0 {
		t.Fatal("paired smoke must provide checks and evidence")
	}
	for _, check := range report.Checks {
		if check.Name == "" || !check.Passed {
			t.Fatal("paired smoke reports missing or failed validation")
		}
	}
	for _, path := range report.EvidenceFiles {
		if !filepath.IsAbs(path) {
			t.Fatal("evidence paths must be absolute")
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("paired smoke evidence missing: %s", path)
		}
	}
	t.Logf("Paired smoke passed: %s, %d workers, %.1fs; draft PR %s; report %s", report.Model, report.WorkerCount, report.DurationSeconds, report.DraftPRURL, reportPath)
}
