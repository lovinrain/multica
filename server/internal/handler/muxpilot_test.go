package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/testutil"
)

type muxpilotTestCoordinator struct {
	project, token string
	generation     int64
}

func newMuxpilotCoordinator(t *testing.T) muxpilotTestCoordinator {
	t.Helper()
	project := dbfx.Project(t, "Muxpilot fixture")
	t.Cleanup(func() {
		for _, table := range []string{"muxpilot_supplement", "muxpilot_operation", "muxpilot_event", "muxpilot_run", "muxpilot_issue", "muxpilot_coordinator"} {
			_, _ = testPool.Exec(context.Background(), "DELETE FROM "+table+" WHERE project_id=$1", project)
		}
	})
	req := withURLParam(withChatTestWorkspaceCtx(t, newRequest("POST", "/lease", map[string]any{"operation_id": uuid.NewString(), "worker_limit": 2})), "projectId", project)
	var out struct {
		Token      string `json:"token"`
		Generation int64  `json:"generation"`
	}
	testutil.Call(t, testHandler.MuxpilotLease, req).Want(200).JSON(&out)
	return muxpilotTestCoordinator{project, out.Token, out.Generation}
}
func (f muxpilotTestCoordinator) request(method, path string, body any) *http.Request {
	r := withURLParam(newRequest(method, path, body), "projectId", f.project)
	r.Header.Set("Authorization", "Bearer "+f.token)
	r.Header.Set("X-Muxpilot-Generation", strconv.FormatInt(f.generation, 10))
	return r
}
func TestMuxpilotCoordinatorTaskIdempotencyAndStageScope(t *testing.T) {
	f := newMuxpilotCoordinator(t)
	op := uuid.NewString()
	payload := map[string]any{"operation_id": op, "action": "create_task", "title": "Coordinator epic", "stage": 0, "no_start": true, "status": "backlog"}
	var first, again map[string]any
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", payload)).Want(201).JSON(&first)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", payload)).Want(201).JSON(&again)
	if first["issue_id"] != again["issue_id"] {
		t.Fatal("retry created another task")
	}
	payload["title"] = "different"
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", payload)).Want(409)
	var worker map[string]any
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "create_task", "title": "Stage 1", "stage": 1, "parent_issue_id": first["issue_id"]})).Want(201).JSON(&worker)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "create_task", "title": "Stage 2", "stage": 2, "parent_issue_id": first["issue_id"]})).Want(201)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "activate_stage", "base_sha": strings.Repeat("a", 40), "stage": 2})).Want(409)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "activate_stage", "base_sha": strings.Repeat("a", 40), "stage": 1})).Want(200)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "update_task", "issue_id": worker["issue_id"], "status": "cancelled"})).Want(200)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "activate_stage", "base_sha": strings.Repeat("a", 40), "stage": 2})).Want(409)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "scope_remove", "issue_id": worker["issue_id"], "content": "Remove cancelled Stage 1 from goal"})).Want(200)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "activate_stage", "base_sha": strings.Repeat("a", 40), "stage": 2})).Want(200)
	var feed struct {
		Events       []map[string]any `json:"events"`
		PrevCursor   int64            `json:"prev_cursor"`
		PageComplete bool             `json:"page_complete"`
	}
	testutil.Call(t, testHandler.MuxpilotEvents, f.request("GET", "/events?after=0", nil)).Want(200).JSON(&feed)
	if len(feed.Events) < 5 || !feed.PageComplete || feed.PrevCursor != 0 {
		t.Fatalf("missing durable complete feed: %+v", feed)
	}
	for _, event := range feed.Events {
		if strings.Contains(func() string { raw, _ := json.Marshal(event); return string(raw) }(), f.token) {
			t.Fatal("credential entered event feed")
		}
	}
}
func TestMuxpilotLeaseReplayAndRevocation(t *testing.T) {
	f := newMuxpilotCoordinator(t)
	testutil.Call(t, testHandler.MuxpilotSnapshot, f.request("GET", "/snapshot", nil)).Want(200)
	forged := f
	forged.token = "mxpc_" + strings.Repeat("0", 64)
	testutil.Call(t, testHandler.MuxpilotSnapshot, forged.request("GET", "/snapshot", nil)).Want(409)
	op := uuid.NewString()
	leaseReq := func() *http.Request {
		return withURLParam(withChatTestWorkspaceCtx(t, newRequest("POST", "/lease", map[string]any{"operation_id": op, "expected_generation": f.generation, "worker_limit": 2})), "projectId", f.project)
	}
	var fresh, replay struct {
		Token      string `json:"token"`
		Generation int64  `json:"generation"`
	}
	testutil.Call(t, testHandler.MuxpilotLease, leaseReq()).Want(200).JSON(&fresh)
	testutil.Call(t, testHandler.MuxpilotLease, leaseReq()).Want(200).JSON(&replay)
	if fresh != replay || fresh.Generation != f.generation+1 {
		t.Fatalf("lease replay changed capability: %+v %+v", fresh, replay)
	}
	testutil.Call(t, testHandler.MuxpilotSnapshot, f.request("GET", "/snapshot", nil)).Want(409)
}
func TestMuxpilotSupplementExactRunAndTerminalURL(t *testing.T) {
	f := newMuxpilotCoordinator(t)
	s := newSupplementFixture(t, "codex", "running", true)
	dbfx.Exec(t, `UPDATE issue SET project_id=$2 WHERE id=$1`, s.issueID, f.project)
	dbfx.Exec(t, `INSERT INTO muxpilot_run(task_id,project_id,generation) VALUES($1,$2,$3)`, s.taskID, f.project, f.generation)
	payload := map[string]any{"operation_id": uuid.NewString(), "action": "supplement", "task_id": s.taskID, "content": "Reuse existing email service"}
	var receipt map[string]any
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", payload)).Want(201).JSON(&receipt)
	if receipt["status"] != "pending" || receipt["acknowledged"] != false {
		t.Fatal("delivery was falsely acknowledged")
	}
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", payload)).Want(201)
	if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1`, s.taskID); n != 1 {
		t.Fatalf("retry created %d supplements", n)
	}
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "bind_terminal", "task_id": s.taskID, "terminal_url": "javascript:alert(1)", "terminal_state": "live", "session_id": "exact"})).Want(400)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "bind_terminal", "task_id": s.taskID, "terminal_url": "http://localhost:8788/mux/worker?runId=" + s.taskID, "terminal_state": "live", "session_id": "server/pane/incarnation"})).Want(200)
	if _, err := testHandler.Queries.ClaimNextTaskSupplement(t.Context(), parseUUID(s.taskID)); err != nil {
		t.Fatal(err)
	}
	lease := func() *http.Request {
		return withURLParam(withChatTestWorkspaceCtx(t, newRequest("POST", "/lease", map[string]any{"operation_id": uuid.NewString(), "expected_generation": f.generation, "worker_limit": 2})), "projectId", f.project)
	}
	testutil.Call(t, testHandler.MuxpilotLease, lease()).Want(409)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "revoke"})).Want(409)
	authorityReq := func() *http.Request {
		return withURLParams(newDaemonTokenRequest("GET", "/authority", nil, testWorkspaceID, "legit-daemon"), "taskId", s.taskID, "commentId", receipt["comment_id"].(string))
	}
	testutil.Call(t, testHandler.MuxpilotSupplementAuthority, authorityReq()).Want(200)
	dbfx.Exec(t, `UPDATE muxpilot_coordinator SET user_id=$2 WHERE project_id=$1`, f.project, uuid.NewString())
	testutil.Call(t, testHandler.MuxpilotSupplementAuthority, authorityReq()).Want(409)
	dbfx.Exec(t, `UPDATE muxpilot_coordinator SET user_id=$2 WHERE project_id=$1`, f.project, testUserID)
	ackReq := func(body any) *http.Request {
		return withURLParams(newDaemonTokenRequest("POST", "/ack", body, testWorkspaceID, "legit-daemon"), "taskId", s.taskID, "commentId", receipt["comment_id"].(string))
	}
	testutil.Call(t, testHandler.AckTaskSupplement, ackReq(map[string]any{"delivered": false, "outcome_unknown": true, "error": "delivery_unknown"})).Want(200)
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", map[string]any{"operation_id": uuid.NewString(), "action": "cancel", "task_id": s.taskID})).Want(200)
	if n := dbfx.Count(t, `SELECT count(*) FROM muxpilot_supplement WHERE task_id=$1 AND delivery_active AND outcome_unknown`, s.taskID); n != 1 {
		t.Fatal("cancel released or erased uncertain transport reservation")
	}
	testutil.Call(t, testHandler.MuxpilotLease, lease()).Want(409)
	testutil.Call(t, testHandler.AckTaskSupplement, ackReq(map[string]any{"delivered": true})).Want(200)
	if n := dbfx.Count(t, `SELECT count(*) FROM muxpilot_supplement WHERE task_id=$1 AND delivery_active`, s.taskID); n != 0 {
		t.Fatal("factual late delivery did not settle reservation")
	}

	payload["operation_id"] = uuid.NewString()
	testutil.Call(t, testHandler.MuxpilotCommand, f.request("POST", "/commands", payload)).Want(409)
}

func TestMuxpilotCopilotCancelAndContinueCarriesInstruction(t *testing.T) {
	f := newMuxpilotCoordinator(t)
	// Copilot runs negotiate no live supplement capability.
	s := newSupplementFixture(t, "copilot", "running", false)
	dbfx.Exec(t, `UPDATE issue SET project_id=$2 WHERE id=$1`, s.issueID, f.project)
	dbfx.Exec(t, `INSERT INTO muxpilot_run(task_id,project_id,generation) VALUES($1,$2,$3)`, s.taskID, f.project, f.generation)
	dbfx.Cleanup(t, `DELETE FROM agent_task_queue WHERE rerun_of_task_id=$1`, s.taskID)
	command := func(body map[string]any) *http.Request { return f.request("POST", "/commands", body) }

	testutil.Call(t, testHandler.MuxpilotCommand, command(map[string]any{"operation_id": uuid.NewString(), "action": "supplement", "task_id": s.taskID, "content": "Use the shared email service"})).Want(412)
	testutil.Call(t, testHandler.MuxpilotCommand, command(map[string]any{"operation_id": uuid.NewString(), "action": "continue", "task_id": s.taskID, "content": "too early"})).Want(409)
	testutil.Call(t, testHandler.MuxpilotCommand, command(map[string]any{"operation_id": uuid.NewString(), "action": "cancel", "task_id": s.taskID})).Want(200)

	resume := map[string]any{"operation_id": uuid.NewString(), "action": "continue", "task_id": s.taskID, "content": "  Use the shared email service; keep the API unchanged.  "}
	var first, again map[string]any
	testutil.Call(t, testHandler.MuxpilotCommand, command(resume)).Want(201).JSON(&first)
	testutil.Call(t, testHandler.MuxpilotCommand, command(resume)).Want(201).JSON(&again)
	if first["task_id"] != again["task_id"] || first["previous_task_id"] != s.taskID || first["instruction_attached"] != true {
		t.Fatalf("continue receipt = %+v, retry = %+v", first, again)
	}
	var note, lineage string
	dbfx.QueryRow(t, `SELECT handoff_note, rerun_of_task_id::text FROM agent_task_queue WHERE id=$1`, first["task_id"]).Scan(&note, &lineage)
	if note != "[Muxpilot coordinator follow-up]\nUse the shared email service; keep the API unchanged." || lineage != s.taskID {
		t.Fatalf("continued attempt note=%q lineage=%q", note, lineage)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE rerun_of_task_id=$1`, s.taskID); n != 1 {
		t.Fatalf("retry created %d attempts", n)
	}
	testutil.Call(t, testHandler.MuxpilotCommand, command(map[string]any{"operation_id": uuid.NewString(), "action": "continue", "task_id": s.taskID})).Want(409)
}

func TestMuxpilotNativeHumanChangeRetainsActor(t *testing.T) {
	f := newMuxpilotCoordinator(t)
	issue := dbfx.Issue(t, "Human edited", testutil.Cols{"project_id": f.project})
	req := withURLParam(newRequest("PATCH", "/issues/"+issue, map[string]any{"title": "Human actual edit"}), "id", issue)
	testutil.Call(t, testHandler.UpdateIssue, req).Want(200)
	var actor, id string
	dbfx.QueryRow(t, `SELECT actor_type,actor_id FROM muxpilot_event WHERE project_id=$1 AND type='issue.update' ORDER BY sequence DESC LIMIT 1`, f.project).Scan(&actor, &id)
	if actor != "member" || id != testUserID {
		t.Fatalf("human edit actor=%q/%q", actor, id)
	}
}

func TestMuxpilotRecoveryExpiredLeaseIsReadonly(t *testing.T) {
	f := newMuxpilotCoordinator(t)
	dbfx.Exec(t, `UPDATE muxpilot_coordinator SET expires_at=now()-interval '1 second' WHERE project_id=$1`, f.project)
	testutil.Call(t, testHandler.MuxpilotSnapshot, f.request("GET", "/snapshot", nil)).Want(409)
	req := withURLParam(withChatTestWorkspaceCtx(t, newRequest("GET", "/recovery", nil)), "projectId", f.project)
	var out map[string]any
	testutil.Call(t, testHandler.MuxpilotRecovery, req).Want(200).JSON(&out)
	if out["readonly"] != true || out["generation"] != float64(f.generation) {
		t.Fatalf("recovery modified ownership: %+v", out)
	}
}
