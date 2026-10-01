package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/issueposition"
	"github.com/multica-ai/multica/server/internal/muxpilot"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// Muxpilot owns a scoped coordinator credential. It is intentionally not a PAT
// and cannot authenticate ordinary Multica endpoints or account operations.
func (h *Handler) MuxpilotCapabilities(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"protocol": "muxpilot-v1", "capabilities": []string{"coordinator-fence-v1", "operation-receipts-v1", "staged-dispatch-v1", "durable-events-v1", "exact-run-control-v1", "task-terminal-links-v1"}, "supplement_capability": protocol.DaemonCapabilityTaskSupplementV1, "worker_input": "structured-stdio", "interactive_takeover": false})
}

type muxpilotRegisterRequest struct {
	DaemonID    string `json:"daemon_id"`
	OperationID string `json:"operation_id"`
	Name        string `json:"name"`
	Goal        string `json:"goal"`
	RepoRoot    string `json:"repo_root"`
	WorkerLimit int    `json:"worker_limit"`
}

func muxpilotHash(v any) string {
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func muxpilotToken(project, operation string, generation int64) string {
	mac := hmac.New(sha256.New, auth.JWTSecret())
	fmt.Fprintf(mac, "muxpilot-coordinator-v1:%s:%s:%d", project, operation, generation)
	return "mxpc_" + hex.EncodeToString(mac.Sum(nil))
}
func muxpilotBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		writeError(w, 400, "invalid muxpilot request")
		return false
	}
	return true
}
func muxpilotProblem(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, muxpilot.ErrFence):
		writeErrorCode(w, 409, "coordinator_fence_stale", err.Error())
	case errors.Is(err, muxpilot.ErrOperationConflict):
		writeErrorCode(w, 409, "operation_conflict", err.Error())
	default:
		writeError(w, 500, "muxpilot operation failed")
	}
}
func (h *Handler) MuxpilotRegister(w http.ResponseWriter, r *http.Request) {
	projectID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "projectId"), "project id")
	if !ok {
		return
	}
	wsID, ok := parseUUIDOrBadRequest(w, ctxWorkspaceID(r.Context()), "workspace id")
	if !ok {
		return
	}
	userID, ok := parseUUIDOrBadRequest(w, requestUserID(r), "user id")
	if !ok {
		return
	}
	var req muxpilotRegisterRequest
	if !muxpilotBody(w, r, &req) {
		return
	}
	if _, ok := parseUUIDOrBadRequest(w, req.OperationID, "operation_id"); !ok {
		return
	}
	if strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.Goal) == "" || !strings.HasPrefix(req.RepoRoot, "/") {
		writeError(w, 400, "name, goal and absolute repo_root are required")
		return
	}
	resourceRaw, _ := json.Marshal(localDirectoryRef{LocalPath: req.RepoRoot, DaemonID: req.DaemonID, ExecutionMode: localDirectoryModeWorktree})
	normalized, resourceErr := validateLocalDirectoryRef(resourceRaw)
	if resourceErr != nil {
		writeError(w, 400, resourceErr.Error())
		return
	}
	if !h.requireWorktreeCapableDaemon(w, r, wsID, "local_directory", normalized) {
		return
	}
	var daemonOwned bool
	if e := h.DB.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM agent_runtime WHERE workspace_id=$1 AND owner_id=$2 AND daemon_id=$3)`, wsID, userID, req.DaemonID).Scan(&daemonOwned); e != nil || !daemonOwned {
		writeError(w, 403, "daemon does not belong to this project operator")
		return
	}
	tx, err := h.TxStarter.Begin(r.Context())
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	defer tx.Rollback(r.Context())
	// Reuse the workspace write fence before any project/coordinator locks.
	if _, err = h.Queries.WithTx(tx).LockWorkspaceForChatSessionCreate(r.Context(), wsID); err != nil {
		writeError(w, 404, "workspace not found")
		return
	}
	// Registration has no coordinator yet. The advisory lock serializes retries.
	_, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, uuidToString(projectID))
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	hash := muxpilotHash(req)
	prior, err := muxpilot.ReplayOperation(r.Context(), tx, uuidToString(projectID), req.OperationID, 0, hash)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	if prior != nil {
		writeJSON(w, prior.Status, prior.Response)
		return
	}
	var existingWorkspace string
	err = tx.QueryRow(r.Context(), `SELECT workspace_id::text FROM project WHERE id=$1`, projectID).Scan(&existingWorkspace)
	if err == nil && existingWorkspace != uuidToString(wsID) {
		writeError(w, 404, "project not found")
		return
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		muxpilotProblem(w, err)
		return
	}
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(r.Context(), `INSERT INTO project(id,workspace_id,title,description,status,priority,lead_type,lead_id) VALUES($1,$2,$3,$4,'in_progress','none','member',$5)`, projectID, wsID, req.Name, req.Goal, userID)
		if err != nil {
			muxpilotProblem(w, err)
			return
		}
	}
	var conflictingResources int
	if e := tx.QueryRow(r.Context(), `SELECT count(*) FROM project_resource WHERE project_id=$1 AND workspace_id=$2 AND resource_type='local_directory' AND resource_ref<>$3::jsonb`, projectID, wsID, normalized).Scan(&conflictingResources); e != nil {
		muxpilotProblem(w, e)
		return
	}
	if conflictingResources > 0 {
		writeErrorCode(w, 409, "repository_mapping_conflict", "use explicit repository remapping after all attempts end")
		return
	}
	var resourceID string
	err = tx.QueryRow(r.Context(), `INSERT INTO project_resource(project_id,workspace_id,resource_type,resource_ref,created_by) VALUES($1,$2,'local_directory',$3,$4) ON CONFLICT(project_id,resource_type,resource_ref) DO UPDATE SET resource_ref=EXCLUDED.resource_ref RETURNING id::text`, projectID, wsID, normalized, userID).Scan(&resourceID)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	result := map[string]any{"project_id": uuidToString(projectID), "workspace_id": uuidToString(wsID), "registered": true, "resource_id": resourceID, "execution_mode": "worktree", "repo_root": req.RepoRoot, "daemon_id": req.DaemonID}
	raw, _ := json.Marshal(result)
	err = muxpilot.SaveOperation(r.Context(), tx, uuidToString(projectID), req.OperationID, 0, hash, 201, raw)
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	writeJSON(w, 201, result)
}

type muxpilotLeaseRequest struct {
	OperationID        string `json:"operation_id"`
	ExpectedGeneration *int64 `json:"expected_generation"`
	WorkerLimit        int    `json:"worker_limit"`
	RuntimeProfileID   string `json:"runtime_profile_id,omitempty"`
}

func (h *Handler) MuxpilotLease(w http.ResponseWriter, r *http.Request) {
	projectID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "projectId"), "project id")
	if !ok {
		return
	}
	wsID, ok := parseUUIDOrBadRequest(w, ctxWorkspaceID(r.Context()), "workspace id")
	if !ok {
		return
	}
	owner, ok := parseUUIDOrBadRequest(w, requestUserID(r), "user id")
	if !ok {
		return
	}
	var req muxpilotLeaseRequest
	if !muxpilotBody(w, r, &req) {
		return
	}
	if _, ok := parseUUIDOrBadRequest(w, req.OperationID, "operation_id"); !ok {
		return
	}
	if req.WorkerLimit == 0 {
		req.WorkerLimit = 4
	}
	if req.WorkerLimit < 1 || req.WorkerLimit > 64 {
		writeError(w, 400, "worker_limit must be 1..64")
		return
	}
	tx, err := h.TxStarter.Begin(r.Context())
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	defer tx.Rollback(r.Context())
	// Serialize fresh coordinator metadata with workspace teardown before
	// taking project/coordinator locks, matching deletion's lock order.
	if _, err = h.Queries.WithTx(tx).LockWorkspaceForChatSessionCreate(r.Context(), wsID); err != nil {
		writeError(w, 404, "workspace not found")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, uuidToString(projectID)); err != nil {
		muxpilotProblem(w, err)
		return
	}
	q := h.Queries.WithTx(tx)
	if _, err = q.GetProjectInWorkspace(r.Context(), db.GetProjectInWorkspaceParams{ID: projectID, WorkspaceID: wsID}); err != nil {
		writeError(w, 404, "project not found")
		return
	}
	var approvedProfile pgtype.UUID
	if req.RuntimeProfileID != "" {
		approvedProfile, err = muxpilotUUID(req.RuntimeProfileID, "runtime_profile_id")
		if err != nil {
			var e *muxpilotRequestError
			errors.As(err, &e)
			writeError(w, e.status, e.message)
			return
		}
		var command string
		var enabled bool
		err = tx.QueryRow(r.Context(), `SELECT command_name,enabled FROM runtime_profile WHERE id=$1 AND workspace_id=$2`, approvedProfile, wsID).Scan(&command, &enabled)
		basename := command[strings.LastIndex(command, "/")+1:]
		if err != nil || !enabled || (basename != "muxpilot-worker" && basename != "muxpilot-worker-fake") {
			writeError(w, 400, "runtime profile must be an enabled Muxpilot worker wrapper in this workspace")
			return
		}
	}
	hash := muxpilotHash(req)
	var previous json.RawMessage
	var previousHash, previousProject string
	var previousOwner string
	err = tx.QueryRow(r.Context(), `SELECT response,request_hash,project_id::text FROM muxpilot_operation WHERE operation_id=$1`, req.OperationID).Scan(&previous, &previousHash, &previousProject)
	if err == nil {
		if previousHash != hash || previousProject != uuidToString(projectID) {
			muxpilotProblem(w, muxpilot.ErrOperationConflict)
			return
		}
		var result map[string]any
		_ = json.Unmarshal(previous, &result)
		var current int64
		err = tx.QueryRow(r.Context(), `SELECT generation,user_id::text FROM muxpilot_coordinator WHERE project_id=$1 AND workspace_id=$2 FOR UPDATE`, projectID, wsID).Scan(&current, &previousOwner)
		if err != nil || previousOwner != uuidToString(owner) || int64(result["generation"].(float64)) != current {
			muxpilotProblem(w, muxpilot.ErrFence)
			return
		}
		result["token"] = muxpilotToken(uuidToString(projectID), req.OperationID, current)
		writeJSON(w, 200, result)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		muxpilotProblem(w, err)
		return
	}
	var generation int64
	var currentOwner string
	var expires time.Time
	err = tx.QueryRow(r.Context(), `SELECT generation,user_id::text,expires_at FROM muxpilot_coordinator WHERE project_id=$1 FOR UPDATE`, projectID).Scan(&generation, &currentOwner, &expires)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		muxpilotProblem(w, err)
		return
	}
	if generation > 0 && (req.ExpectedGeneration == nil || *req.ExpectedGeneration != generation) {
		writeErrorCode(w, 409, "coordinator_ownership_conflict", "expected_generation must match the current owner epoch")
		return
	}
	var delivering int
	if e := tx.QueryRow(r.Context(), `SELECT count(*) FROM muxpilot_supplement m JOIN task_supplement s ON s.task_id=m.task_id AND s.comment_id=m.comment_id WHERE m.project_id=$1 AND m.delivery_active`, projectID).Scan(&delivering); e != nil {
		muxpilotProblem(w, e)
		return
	}
	if delivering > 0 {
		writeErrorCode(w, 409, "delivery_in_progress", "coordinator ownership cannot change until in-flight guidance has a factual delivery or failure receipt")
		return
	}
	generation++
	// Revoke old queued coordinator guidance. Delivering guidance is resolved by
	// the daemon receiver authority check; a lost transport receipt remains factual.
	_, err = tx.Exec(r.Context(), `UPDATE task_supplement s SET status='failed',failure_reason='coordinator_revoked',updated_at=now() FROM muxpilot_supplement m WHERE s.comment_id=m.comment_id AND s.task_id=m.task_id AND m.project_id=$1 AND m.generation<$2 AND s.status='pending'`, projectID, generation)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	token := muxpilotToken(uuidToString(projectID), req.OperationID, generation)
	err = tx.QueryRow(r.Context(), `INSERT INTO muxpilot_coordinator(project_id,workspace_id,user_id,generation,token_hash,expires_at,dispatch_held,worker_limit,runtime_profile_id) VALUES($1,$2,$3,$4,$5,clock_timestamp()+interval '30 minutes',true,$6,$7) ON CONFLICT(project_id) DO UPDATE SET user_id=EXCLUDED.user_id,generation=EXCLUDED.generation,token_hash=EXCLUDED.token_hash,expires_at=EXCLUDED.expires_at,worker_limit=EXCLUDED.worker_limit,runtime_profile_id=COALESCE(EXCLUDED.runtime_profile_id,muxpilot_coordinator.runtime_profile_id),dispatch_held=true RETURNING expires_at`, projectID, wsID, owner, generation, muxpilot.TokenHash(token), req.WorkerLimit, approvedProfile).Scan(&expires)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	result := map[string]any{"project_id": uuidToString(projectID), "workspace_id": uuidToString(wsID), "generation": generation, "expires_at": expires, "worker_limit": req.WorkerLimit}
	raw, _ := json.Marshal(result)
	err = muxpilot.SaveOperation(r.Context(), tx, uuidToString(projectID), req.OperationID, generation, hash, 200, raw)
	if err == nil {
		err = muxpilotRecord(r.Context(), tx, uuidToString(projectID), uuidToString(wsID), "member", uuidToString(owner), "coordinator.acquired", map[string]any{"generation": generation})
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	result["token"] = token
	writeJSON(w, 200, result)
}

// The scoped endpoints authenticate directly, outside ordinary PAT middleware.
// LockFence validates authority again inside the transaction holding mutations.
func (h *Handler) muxpilotFence(w http.ResponseWriter, r *http.Request) (pgx.Tx, string, string, string, int64, bool) {
	projectID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "projectId"), "project id")
	if !ok {
		return nil, "", "", "", 0, false
	}
	generation, err := strconv.ParseInt(r.Header.Get("X-Muxpilot-Generation"), 10, 64)
	if err != nil || generation < 1 {
		writeError(w, 401, "X-Muxpilot-Generation is required")
		return nil, "", "", "", 0, false
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !strings.HasPrefix(token, "mxpc_") {
		writeError(w, 401, "coordinator bearer token is required")
		return nil, "", "", "", 0, false
	}
	tx, err := h.TxStarter.Begin(r.Context())
	if err != nil {
		muxpilotProblem(w, err)
		return nil, "", "", "", 0, false
	}
	var workspace, owner string
	err = tx.QueryRow(r.Context(), `SELECT c.workspace_id::text,c.user_id::text FROM muxpilot_coordinator c JOIN project p ON p.id=c.project_id AND p.workspace_id=c.workspace_id WHERE c.project_id=$1`, projectID).Scan(&workspace, &owner)
	if err == nil {
		err = muxpilot.LockFence(r.Context(), tx, workspace, uuidToString(projectID), owner, generation, token)
	}
	// Revoking workspace membership also revokes the capability.
	if err == nil {
		var member bool
		err = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2)`, workspace, owner).Scan(&member)
		if err == nil && !member {
			err = muxpilot.ErrFence
		}
	}
	if err != nil {
		tx.Rollback(r.Context())
		if errors.Is(err, pgx.ErrNoRows) {
			err = muxpilot.ErrFence
		}
		muxpilotProblem(w, err)
		return nil, "", "", "", 0, false
	}
	return tx, uuidToString(projectID), workspace, owner, generation, true
}
func muxpilotRecord(ctx context.Context, tx pgx.Tx, project, workspace, actor, actorID, kind string, payload any) error {
	raw, _ := json.Marshal(payload)
	_, err := tx.Exec(ctx, `INSERT INTO muxpilot_event(project_id,workspace_id,actor_type,actor_id,type,payload) VALUES($1,$2,$3,$4,$5,$6)`, project, workspace, actor, actorID, kind, raw)
	return err
}

type muxpilotCommand struct {
	RepoRoot      string `json:"repo_root,omitempty"`
	DaemonID      string `json:"daemon_id,omitempty"`
	NoStart       bool   `json:"no_start,omitempty"`
	BaseSHA       string `json:"base_sha,omitempty"`
	OperationID   string `json:"operation_id"`
	Action        string `json:"action"`
	Title         string `json:"title,omitempty"`
	Description   string `json:"description,omitempty"`
	Acceptance    string `json:"acceptance,omitempty"`
	ParentIssueID string `json:"parent_issue_id,omitempty"`
	AgentID       string `json:"agent_id,omitempty"`
	Stage         int    `json:"stage,omitempty"`
	Held          *bool  `json:"held,omitempty"`
	TaskID        string `json:"task_id,omitempty"`
	Content       string `json:"content,omitempty"`
	TerminalURL   string `json:"terminal_url,omitempty"`
	TerminalState string `json:"terminal_state,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	IssueID       string `json:"issue_id,omitempty"`
	Status        string `json:"status,omitempty"`
}

func (h *Handler) MuxpilotCommand(w http.ResponseWriter, r *http.Request) {
	var req muxpilotCommand
	if !muxpilotBody(w, r, &req) {
		return
	}
	if _, ok := parseUUIDOrBadRequest(w, req.OperationID, "operation_id"); !ok {
		return
	}
	tx, project, workspace, owner, generation, ok := h.muxpilotFence(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	hash := muxpilotHash(req)
	prior, err := muxpilot.ReplayOperation(r.Context(), tx, project, req.OperationID, generation, hash)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	if prior != nil {
		writeJSON(w, prior.Status, prior.Response)
		return
	}
	_, err = tx.Exec(r.Context(), `SELECT set_config('muxpilot.actor_type','coordinator',true),set_config('muxpilot.actor_id',$1,true)`, owner)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	result, status, post, err := h.muxpilotApply(r.Context(), tx, project, workspace, owner, req)
	if err != nil {
		var pe *muxpilotRequestError
		if errors.As(err, &pe) {
			writeErrorCode(w, pe.status, pe.code, pe.message)
		} else {
			muxpilotProblem(w, err)
		}
		return
	}
	result["operation_id"] = req.OperationID
	result["generation"] = generation
	raw, _ := json.Marshal(result)
	err = muxpilot.SaveOperation(r.Context(), tx, project, req.OperationID, generation, hash, status, raw)
	if err == nil {
		err = muxpilotRecord(r.Context(), tx, project, workspace, "coordinator", owner, "command."+req.Action, map[string]any{"operation_id": req.OperationID, "generation": generation, "receipt": result})
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	if post != nil {
		post()
	}
	writeJSON(w, status, result)
}

type muxpilotRequestError struct {
	status        int
	code, message string
}

func (e *muxpilotRequestError) Error() string        { return e.message }
func muxpilotBad(status int, code, msg string) error { return &muxpilotRequestError{status, code, msg} }
func muxpilotUUID(s, field string) (pgtype.UUID, error) {
	var id pgtype.UUID
	if err := id.Scan(s); err != nil || !id.Valid {
		return id, muxpilotBad(400, "invalid_uuid", field+" must be a UUID")
	}
	return id, nil
}

func (h *Handler) muxpilotApply(ctx context.Context, tx pgx.Tx, project, workspace, owner string, req muxpilotCommand) (map[string]any, int, func(), error) {
	q := h.Queries.WithTx(tx)
	ws := parseUUID(workspace)
	p := parseUUID(project)
	user := parseUUID(owner)
	switch req.Action {
	case "remap_repository":
		if strings.TrimSpace(req.Content) == "" {
			return nil, 0, nil, muxpilotBad(400, "decision_required", "repository remapping requires a recorded reason")
		}
		resourceRaw, _ := json.Marshal(localDirectoryRef{LocalPath: req.RepoRoot, DaemonID: req.DaemonID, ExecutionMode: localDirectoryModeWorktree})
		normalized, e := validateLocalDirectoryRef(resourceRaw)
		if e != nil {
			return nil, 0, nil, muxpilotBad(400, "invalid_repository", e.Error())
		}
		var active int
		e = tx.QueryRow(ctx, `SELECT count(*) FROM agent_task_queue t JOIN issue i ON i.id=t.issue_id WHERE i.project_id=$1 AND t.status NOT IN ('completed','failed','cancelled')`, p).Scan(&active)
		if e != nil {
			return nil, 0, nil, e
		}
		var unresolved int
		if e := tx.QueryRow(ctx, `SELECT count(*) FROM muxpilot_supplement WHERE project_id=$1 AND delivery_active`, p).Scan(&unresolved); e != nil {
			return nil, 0, nil, e
		}
		if unresolved > 0 {
			return nil, 0, nil, muxpilotBad(409, "delivery_in_progress", "resolve in-flight guidance before remapping its repository")
		}
		if active > 0 {
			return nil, 0, nil, muxpilotBad(409, "project_active", "repository remapping requires all attempts to end")
		}
		runtimes, e := q.ListAgentRuntimes(ctx, ws)
		if e != nil {
			return nil, 0, nil, e
		}
		if !daemonAdvertisesWorktree(runtimes, req.DaemonID) {
			return nil, 0, nil, muxpilotBad(412, "worktree_unsupported", "registered daemon lacks native isolated worktree capability")
		}
		var owned bool
		e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_runtime WHERE workspace_id=$1 AND owner_id=$2 AND daemon_id=$3)`, ws, user, req.DaemonID).Scan(&owned)
		if e != nil || !owned {
			return nil, 0, nil, muxpilotBad(403, "daemon_forbidden", "daemon does not belong to project operator")
		}
		_, e = tx.Exec(ctx, `DELETE FROM project_resource WHERE project_id=$1 AND workspace_id=$2 AND resource_type='local_directory'`, p, ws)
		if e != nil {
			return nil, 0, nil, e
		}
		_, e = q.CreateProjectResource(ctx, db.CreateProjectResourceParams{ProjectID: p, WorkspaceID: ws, ResourceType: "local_directory", ResourceRef: normalized, CreatedBy: user})
		if e != nil {
			return nil, 0, nil, e
		}
		_, e = tx.Exec(ctx, `UPDATE muxpilot_coordinator SET dispatch_held=true WHERE project_id=$1`, p)
		return map[string]any{"repo_root": req.RepoRoot, "daemon_id": req.DaemonID, "execution_mode": "worktree", "held": true, "decision": req.Content}, 200, nil, e
	case "hold":
		if req.Held == nil {
			return nil, 0, nil, muxpilotBad(400, "invalid_hold", "held is required")
		}
		_, err := tx.Exec(ctx, `UPDATE muxpilot_coordinator SET dispatch_held=$2 WHERE project_id=$1`, p, *req.Held)
		return map[string]any{"held": *req.Held}, 200, nil, err
	case "renew":
		var expiry time.Time
		err := tx.QueryRow(ctx, `UPDATE muxpilot_coordinator SET expires_at=clock_timestamp()+interval '30 minutes' WHERE project_id=$1 RETURNING expires_at`, p).Scan(&expiry)
		return map[string]any{"expires_at": expiry}, 200, nil, err
	case "revoke":
		var delivering int
		e := tx.QueryRow(ctx, `SELECT count(*) FROM muxpilot_supplement m JOIN task_supplement s ON s.task_id=m.task_id AND s.comment_id=m.comment_id WHERE m.project_id=$1 AND m.delivery_active`, p).Scan(&delivering)
		if e != nil {
			return nil, 0, nil, e
		}
		if delivering > 0 {
			return nil, 0, nil, muxpilotBad(409, "delivery_in_progress", "cancel the exact run or reconcile guidance delivery before revoking ownership")
		}
		_, err := tx.Exec(ctx, `UPDATE muxpilot_coordinator SET expires_at=clock_timestamp(),dispatch_held=true WHERE project_id=$1`, p)
		return map[string]any{"revoked": true}, 200, nil, err
	case "create_task":
		if req.Status != "" && req.Status != "backlog" {
			return nil, 0, nil, muxpilotBad(400, "invalid_status", "new tasks must stay in backlog until stage activation")
		}
		if strings.TrimSpace(req.Title) == "" || len(req.Title) > 1024 || req.Stage < 0 {
			return nil, 0, nil, muxpilotBad(400, "invalid_task", "title and nonnegative stage are required")
		}
		var parent, agentID pgtype.UUID
		var assignee pgtype.Text
		var err error
		if req.ParentIssueID != "" {
			parent, err = muxpilotUUID(req.ParentIssueID, "parent_issue_id")
			if err != nil {
				return nil, 0, nil, err
			}
			existing, e := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: parent, WorkspaceID: ws})
			if e != nil || existing.ProjectID != p {
				return nil, 0, nil, muxpilotBad(404, "parent_not_found", "parent must belong to this project")
			}
		}
		if req.AgentID != "" {
			agentID, err = muxpilotUUID(req.AgentID, "agent_id")
			if err != nil {
				return nil, 0, nil, err
			}
			agent, e := q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: agentID, WorkspaceID: ws})
			if e != nil || !h.canInvokeAgent(ctx, agent, "member", owner, owner, workspace) {
				return nil, 0, nil, muxpilotBad(403, "agent_forbidden", "agent is not available to this coordinator")
			}
			var allowed bool
			e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_runtime r JOIN runtime_profile rp ON rp.id=r.profile_id JOIN muxpilot_coordinator c ON c.runtime_profile_id=rp.id AND c.workspace_id=rp.workspace_id WHERE r.id=$1 AND c.project_id=$2 AND rp.enabled AND regexp_replace(rp.command_name,'^.*/','') IN ('muxpilot-worker','muxpilot-worker-fake'))`, agent.RuntimeID, p).Scan(&allowed)
			if e != nil || !allowed {
				return nil, 0, nil, muxpilotBad(412, "muxpilot_worker_profile_required", "worker must use this project's approved visible Muxpilot runtime profile")
			}
			assignee = pgtype.Text{String: "agent", Valid: true}
		}
		number, err := service.AllocateIssueNumber(ctx, q, ws, service.ResolveIssueCountPolicy(ctx, h.Entitlements, ws))
		if err != nil {
			return nil, 0, nil, err
		}
		position, err := issueposition.NextTopPosition(ctx, tx, ws, "backlog")
		if err != nil {
			return nil, 0, nil, err
		}
		description := req.Description
		if req.Acceptance != "" {
			description += "\n\nAcceptance criteria:\n" + req.Acceptance
		}
		issue, err := q.CreateIssue(ctx, db.CreateIssueParams{ID: dbid.NewV7(), WorkspaceID: ws, ProjectID: p, Title: sanitizeNullBytes(req.Title), Description: pgtype.Text{String: sanitizeNullBytes(description), Valid: description != ""}, Status: "backlog", Priority: "none", CreatorType: "member", CreatorID: user, ParentIssueID: parent, AssigneeType: assignee, AssigneeID: agentID, Number: number, Position: position, Stage: pgtype.Int4{Int32: int32(req.Stage), Valid: req.Stage > 0}})
		if err != nil {
			return nil, 0, nil, err
		}
		_, err = tx.Exec(ctx, `UPDATE muxpilot_issue SET stage=$2,eligible=false WHERE issue_id=$1`, issue.ID, req.Stage)
		post := func() {
			h.publish(protocol.EventIssueCreated, workspace, "member", owner, map[string]any{"issue_id": uuidToString(issue.ID)})
		}
		return map[string]any{"issue_id": uuidToString(issue.ID), "status": "backlog", "stage": req.Stage, "eligible": false}, 201, post, err
	case "activate_stage":
		if !muxpilotValidSHA(req.BaseSHA) {
			return nil, 0, nil, muxpilotBad(400, "base_sha_required", "stage activation requires an exact 40 or 64 character commit SHA")
		}
		if req.Stage < 1 {
			return nil, 0, nil, muxpilotBad(400, "invalid_stage", "stage must be >= 1")
		}
		var changedPins int
		e := tx.QueryRow(ctx, `SELECT count(*) FROM muxpilot_issue WHERE project_id=$1 AND stage=$2 AND eligible AND base_sha<>$3`, p, req.Stage, req.BaseSHA).Scan(&changedPins)
		if e != nil {
			return nil, 0, nil, e
		}
		if changedPins > 0 {
			return nil, 0, nil, muxpilotBad(409, "stage_revision_conflict", "an active stage keeps its recorded base SHA")
		}
		var incomplete int
		err := tx.QueryRow(ctx, `SELECT count(*) FROM muxpilot_issue m JOIN issue i ON i.id=m.issue_id WHERE m.project_id=$1 AND m.stage>0 AND m.stage<$2 AND i.status<>'done'`, p, req.Stage).Scan(&incomplete)
		if err != nil {
			return nil, 0, nil, err
		}
		if incomplete > 0 {
			return nil, 0, nil, muxpilotBad(409, "stage_prerequisites", "earlier stages have not been accepted")
		}
		_, err = tx.Exec(ctx, `UPDATE muxpilot_issue SET eligible=true,base_sha=$3 WHERE project_id=$1 AND stage=$2`, p, req.Stage, req.BaseSHA)
		if err != nil {
			return nil, 0, nil, err
		}
		rows, err := tx.Query(ctx, `SELECT i.id::text FROM issue i JOIN muxpilot_issue m ON m.issue_id=i.id WHERE m.project_id=$1 AND m.stage=$2 AND i.status='backlog' ORDER BY i.id`, p, req.Stage)
		if err != nil {
			return nil, 0, nil, err
		}
		ids := []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return nil, 0, nil, err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, 0, nil, err
		}
		_, err = tx.Exec(ctx, `UPDATE agent_task_queue t SET context=jsonb_set(COALESCE(t.context,'{}'::jsonb),'{muxpilot_base_sha}',to_jsonb($3::text)) FROM muxpilot_issue m WHERE t.issue_id=m.issue_id AND m.project_id=$1 AND m.stage=$2 AND t.status='queued'`, p, req.Stage, req.BaseSHA)
		if err != nil {
			return nil, 0, nil, err
		}
		tasks := []db.AgentTaskQueue{}
		for _, id := range ids {
			issue, e := q.UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{ID: parseUUID(id), WorkspaceID: ws, Status: "todo"})
			if e != nil {
				return nil, 0, nil, e
			}
			if issue.AssigneeType.String != "agent" {
				continue
			}
			agent, e := q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: issue.AssigneeID, WorkspaceID: ws})
			if e != nil || !agent.RuntimeID.Valid || !h.canInvokeAgent(ctx, agent, "member", owner, owner, workspace) {
				return nil, 0, nil, muxpilotBad(409, "agent_unavailable", "eligible worker has no available runtime")
			}
			pending, e := q.HasPendingTaskForIssueAndAgent(ctx, db.HasPendingTaskForIssueAndAgentParams{IssueID: issue.ID, AgentID: agent.ID})
			if e != nil {
				return nil, 0, nil, e
			}
			if pending {
				continue
			}
			task, e := q.CreateAgentTask(ctx, db.CreateAgentTaskParams{ID: dbid.NewV7(), AgentID: agent.ID, RuntimeID: agent.RuntimeID, IssueID: issue.ID, Priority: 2, OriginatorUserID: user, AccountableUserID: user, OriginatorSource: pgtype.Text{String: "external_coordinator", Valid: true}})
			if e != nil {
				return nil, 0, nil, e
			}
			tasks = append(tasks, task)
		}
		_, err = tx.Exec(ctx, `UPDATE muxpilot_coordinator SET dispatch_held=false WHERE project_id=$1`, p)
		taskIDs := []string{}
		for _, task := range tasks {
			taskIDs = append(taskIDs, uuidToString(task.ID))
		}
		post := func() {
			for _, task := range tasks {
				h.TaskService.NotifyTaskEnqueued(ctx, task)
			}
			h.publish(protocol.EventIssueUpdated, workspace, "member", owner, map[string]any{"project_id": project})
		}
		return map[string]any{"stage": req.Stage, "issue_ids": ids, "task_ids": taskIDs, "held": false, "base_sha": req.BaseSHA}, 200, post, err
	case "scope_remove":
		id, err := muxpilotUUID(req.IssueID, "issue_id")
		if err != nil {
			return nil, 0, nil, err
		}
		if strings.TrimSpace(req.Content) == "" {
			return nil, 0, nil, muxpilotBad(400, "scope_reason_required", "scope removal requires a recorded decision")
		}
		tag, err := tx.Exec(ctx, `UPDATE muxpilot_issue SET stage=0,eligible=false WHERE issue_id=$1 AND project_id=$2`, id, p)
		if err == nil && tag.RowsAffected() == 0 {
			return nil, 0, nil, muxpilotBad(404, "task_not_found", "task not found in project")
		}
		return map[string]any{"issue_id": req.IssueID, "scope_removed": true, "decision": req.Content}, 200, nil, err
	case "update_task":
		id, err := muxpilotUUID(req.IssueID, "issue_id")
		if err != nil {
			return nil, 0, nil, err
		}
		issue, e := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: id, WorkspaceID: ws})
		if e != nil || issue.ProjectID != p {
			return nil, 0, nil, muxpilotBad(404, "task_not_found", "task not found in project")
		}
		if req.Status != "done" && req.Status != "backlog" && req.Status != "cancelled" {
			return nil, 0, nil, muxpilotBad(400, "invalid_status", "status must be done, cancelled or backlog")
		}
		updated, err := q.UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{ID: id, WorkspaceID: ws, Status: req.Status})
		return map[string]any{"issue_id": uuidToString(updated.ID), "status": updated.Status}, 200, func() {
			h.publish(protocol.EventIssueUpdated, workspace, "member", owner, map[string]any{"issue_id": req.IssueID})
		}, err
	case "supplement", "cancel", "bind_terminal", "adopt", "adopt_run", "continue":
		taskID, err := muxpilotUUID(req.TaskID, "task_id")
		if err != nil {
			return nil, 0, nil, err
		}
		task, e := q.GetAgentTaskInWorkspace(ctx, db.GetAgentTaskInWorkspaceParams{ID: taskID, WorkspaceID: ws})
		if e != nil || !task.IssueID.Valid {
			return nil, 0, nil, muxpilotBad(404, "run_not_found", "run not found")
		}
		issue, e := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: task.IssueID, WorkspaceID: ws})
		if e != nil || issue.ProjectID != p {
			return nil, 0, nil, muxpilotBad(404, "run_not_found", "run not found in project")
		}
		agent, e := q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: task.AgentID, WorkspaceID: ws})
		if e != nil || !h.canInvokeAgent(ctx, agent, "member", owner, owner, workspace) {
			return nil, 0, nil, muxpilotBad(403, "agent_forbidden", "worker is not available to this coordinator")
		}
		if req.Action == "adopt" || req.Action == "adopt_run" {
			_, err = tx.Exec(ctx, `UPDATE muxpilot_run r SET generation=c.generation FROM muxpilot_coordinator c WHERE r.task_id=$1 AND r.project_id=$2 AND c.project_id=r.project_id`, taskID, p)
			return map[string]any{"task_id": req.TaskID, "adopted": true, "status": task.Status}, 200, func() {
				if task.Status == "queued" {
					h.TaskService.NotifyTaskEnqueued(ctx, task)
				}
			}, err
		}
		if req.Action == "continue" {
			if task.Status != "completed" && task.Status != "failed" && task.Status != "cancelled" {
				return nil, 0, nil, muxpilotBad(409, "run_active", "this exact run has not ended")
			}
			if issue.Status == "done" {
				return nil, 0, nil, muxpilotBad(409, "task_accepted", "reopen accepted work before creating a new attempt")
			}
			if !agent.RuntimeID.Valid {
				return nil, 0, nil, muxpilotBad(409, "runtime_unavailable", "worker has no available runtime")
			}
			pending, e := q.HasPendingTaskForIssueAndAgent(ctx, db.HasPendingTaskForIssueAndAgentParams{IssueID: issue.ID, AgentID: agent.ID})
			if e != nil {
				return nil, 0, nil, e
			}
			if pending {
				return nil, 0, nil, muxpilotBad(409, "run_already_pending", "a new attempt is already pending")
			}
			next, e := q.CreateAgentTask(ctx, db.CreateAgentTaskParams{ID: dbid.NewV7(), AgentID: agent.ID, RuntimeID: agent.RuntimeID, IssueID: issue.ID, Priority: task.Priority, OriginatorUserID: user, AccountableUserID: user, OriginatorSource: pgtype.Text{String: "external_coordinator", Valid: true}, RerunOfTaskID: task.ID})
			return map[string]any{"task_id": uuidToString(next.ID), "previous_task_id": req.TaskID, "status": "queued"}, 201, func() { h.TaskService.NotifyTaskEnqueued(ctx, next) }, e
		}
		if req.Action == "bind_terminal" {
			parsed, e := url.Parse(req.TerminalURL)
			if e != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || req.SessionID == "" || (req.TerminalState != "live" && req.TerminalState != "history" && req.TerminalState != "unavailable") {
				return nil, 0, nil, muxpilotBad(400, "invalid_terminal", "exact HTTP(S) terminal URL, session identity and state are required")
			}
			_, err = tx.Exec(ctx, `INSERT INTO muxpilot_run(task_id,project_id,terminal_url,terminal_state,session_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(task_id) DO UPDATE SET terminal_url=EXCLUDED.terminal_url,terminal_state=EXCLUDED.terminal_state,session_id=EXCLUDED.session_id`, taskID, p, req.TerminalURL, req.TerminalState, req.SessionID)
			return map[string]any{"task_id": req.TaskID, "terminal_url": req.TerminalURL, "terminal_state": req.TerminalState}, 200, nil, err
		}
		if req.Action == "cancel" {
			cancelled, e := q.CancelAgentTaskByUser(ctx, db.CancelAgentTaskByUserParams{ID: taskID, CancelledByType: pgtype.Text{String: "member", Valid: true}, CancelledByID: user})
			if errors.Is(e, pgx.ErrNoRows) {
				cancelled = task
				e = nil
			}
			if e == nil {
				e = service.SettleTerminalTaskState(ctx, q, cancelled)
			}
			post := func() {
				h.TaskService.ReconcileAgentStatus(ctx, task.AgentID)
				h.TaskService.NotifyTaskFinished(cancelled)
				h.publish(protocol.EventTaskCancelled, workspace, "member", owner, map[string]any{"task_id": req.TaskID, "agent_id": uuidToString(task.AgentID), "status": cancelled.Status})
			}
			return map[string]any{"task_id": req.TaskID, "status": cancelled.Status, "receipt": "cancel_requested", "provider_terminated_confirmed": false}, 200, post, e
		}
		if strings.TrimSpace(req.Content) == "" || len(req.Content) > maxCommentContentBytes {
			return nil, 0, nil, muxpilotBad(400, "invalid_content", "nonempty bounded content is required")
		}
		if task.Status != "running" {
			return nil, 0, nil, muxpilotBad(409, "task_supplement_turn_ended", "this exact run is not running")
		}
		capability, e := q.GetTaskSupplementCapability(ctx, task.ID)
		if e != nil || capability.Capability != protocol.DaemonCapabilityTaskSupplementV1 {
			return nil, 0, nil, muxpilotBad(412, "task_supplement_unsupported", "this run does not support additional messages")
		}
		created, e := q.CreateTaskSupplement(ctx, db.CreateTaskSupplementParams{TaskID: taskID, IssueID: issue.ID, WorkspaceID: ws, AuthorID: user, Content: sanitizeNullBytes("[Muxpilot coordinator]\n" + req.Content), ClientRequestID: parseUUID(req.OperationID)})
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, 0, nil, muxpilotBad(409, "task_supplement_turn_ended", "this exact run ended before delivery")
		}
		if e == nil {
			_, e = tx.Exec(ctx, `INSERT INTO muxpilot_supplement(comment_id,task_id,project_id,generation) SELECT $1,$2,$3,generation FROM muxpilot_coordinator WHERE project_id=$3`, created.ID, taskID, p)
		}
		return map[string]any{"task_id": req.TaskID, "comment_id": uuidToString(created.ID), "status": created.SupplementStatus, "acknowledged": false}, 201, func() { h.notifyTaskSupplementAvailable(task) }, e
	default:
		return nil, 0, nil, muxpilotBad(400, "unsupported_action", "unsupported muxpilot command")
	}
}

func (h *Handler) MuxpilotEvents(w http.ResponseWriter, r *http.Request) {
	tx, project, workspace, _, _, ok := h.muxpilotFence(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	after, err := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	if r.URL.Query().Get("after") == "" {
		after = 0
		err = nil
	}
	if err != nil || after < 0 {
		writeError(w, 400, "after must be a nonnegative sequence")
		return
	}
	limit := 200
	if r.URL.Query().Get("limit") != "" {
		limit, err = strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit < 1 || limit > 1000 {
			writeError(w, 400, "limit must be 1..1000")
			return
		}
	}
	rows, err := tx.Query(r.Context(), `SELECT sequence,id::text,project_id::text,workspace_id::text,actor_type,actor_id,type,payload,occurred_at FROM muxpilot_event WHERE project_id=$1 AND workspace_id=$2 AND sequence>$3 ORDER BY sequence LIMIT $4`, project, workspace, after, limit)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	events := []muxpilot.Event{}
	cursor := after
	for rows.Next() {
		var event muxpilot.Event
		if err = rows.Scan(&event.Sequence, &event.ID, &event.ProjectID, &event.WorkspaceID, &event.ActorType, &event.ActorID, &event.Type, &event.Payload, &event.OccurredAt); err != nil {
			rows.Close()
			muxpilotProblem(w, err)
			return
		}
		events = append(events, event)
		cursor = event.Sequence
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"events": events, "cursor": cursor, "has_more": len(events) == limit, "retention_gap": false, "prev_cursor": after, "page_complete": true})
}
func (h *Handler) MuxpilotOperation(w http.ResponseWriter, r *http.Request) {
	tx, project, _, _, _, ok := h.muxpilotFence(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	id, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "operationId"), "operation_id")
	if !ok {
		return
	}
	var raw json.RawMessage
	var status int
	err := tx.QueryRow(r.Context(), `SELECT response,status FROM muxpilot_operation WHERE project_id=$1 AND operation_id=$2`, project, id).Scan(&raw, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "operation not found")
		return
	}
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"status": status, "response": raw})
}
func (h *Handler) MuxpilotSnapshot(w http.ResponseWriter, r *http.Request) {
	tx, project, workspace, owner, generation, ok := h.muxpilotFence(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	rows, err := tx.Query(r.Context(), `SELECT i.id::text,i.title,i.status,COALESCE(i.assignee_id::text,''),COALESCE(i.parent_issue_id::text,''),m.stage,m.eligible FROM issue i JOIN muxpilot_issue m ON m.issue_id=i.id WHERE m.project_id=$1 AND i.workspace_id=$2 ORDER BY m.stage,i.created_at`, project, workspace)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	issues := []map[string]any{}
	for rows.Next() {
		var id, title, status, agent, parent string
		var stage int
		var eligible bool
		if err = rows.Scan(&id, &title, &status, &agent, &parent, &stage, &eligible); err != nil {
			rows.Close()
			muxpilotProblem(w, err)
			return
		}
		issues = append(issues, map[string]any{"id": id, "title": title, "status": status, "agent_id": agent, "parent_issue_id": parent, "stage": stage, "eligible": eligible})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	rows, err = tx.Query(r.Context(), `SELECT t.id::text,t.issue_id::text,t.agent_id::text,t.status,COALESCE(t.session_id,''),COALESCE(t.work_dir,''),COALESCE(r.terminal_url,''),COALESCE(r.terminal_state,'unavailable'),COALESCE(r.session_id,'') FROM agent_task_queue t JOIN issue i ON i.id=t.issue_id LEFT JOIN muxpilot_run r ON r.task_id=t.id WHERE i.project_id=$1 AND i.workspace_id=$2 ORDER BY t.created_at`, project, workspace)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	runs := []map[string]any{}
	for rows.Next() {
		var id, issue, agent, status, session, workdir, terminal, state, binding string
		if err = rows.Scan(&id, &issue, &agent, &status, &session, &workdir, &terminal, &state, &binding); err != nil {
			rows.Close()
			muxpilotProblem(w, err)
			return
		}
		runs = append(runs, map[string]any{"id": id, "task_id": id, "issue_id": issue, "agent_id": agent, "status": status, "provider_session_id": session, "work_dir": workdir, "terminal_url": terminal, "terminal_state": state, "session_id": binding})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	rosterRows, e := tx.Query(r.Context(), `SELECT a.id::text,a.name,r.provider,COALESCE(a.model,'') FROM agent a JOIN agent_runtime r ON r.id=a.runtime_id JOIN runtime_profile rp ON rp.id=r.profile_id JOIN muxpilot_coordinator c ON c.runtime_profile_id=rp.id AND c.workspace_id=a.workspace_id WHERE c.project_id=$1 AND a.workspace_id=$2 AND rp.enabled AND a.archived_at IS NULL ORDER BY a.name`, project, workspace)
	if e != nil {
		muxpilotProblem(w, e)
		return
	}
	approved := []map[string]any{}
	for rosterRows.Next() {
		var id, name, provider, model string
		if e = rosterRows.Scan(&id, &name, &provider, &model); e != nil {
			rosterRows.Close()
			muxpilotProblem(w, e)
			return
		}
		agent, loadErr := h.Queries.GetAgentInWorkspace(r.Context(), db.GetAgentInWorkspaceParams{ID: parseUUID(id), WorkspaceID: parseUUID(workspace)})
		if loadErr == nil && h.canInvokeAgent(r.Context(), agent, "member", owner, owner, workspace) {
			approved = append(approved, map[string]any{"id": id, "name": name, "provider": provider, "model": model, "input": "structured-stdio", "interactive_takeover": false})
		}
	}
	e = rosterRows.Err()
	rosterRows.Close()
	if e != nil {
		muxpilotProblem(w, e)
		return
	}
	controlRows, e := tx.Query(r.Context(), `SELECT m.task_id::text,m.comment_id::text,m.generation,m.delivery_active,m.outcome_unknown,s.status,COALESCE(s.failure_reason,'') FROM muxpilot_supplement m JOIN task_supplement s ON s.task_id=m.task_id AND s.comment_id=m.comment_id WHERE m.project_id=$1 ORDER BY s.created_at`, project)
	if e != nil {
		muxpilotProblem(w, e)
		return
	}
	controls := []map[string]any{}
	for controlRows.Next() {
		var taskID, commentID, status, reason string
		var gen int64
		var active, unknown bool
		if e = controlRows.Scan(&taskID, &commentID, &gen, &active, &unknown, &status, &reason); e != nil {
			controlRows.Close()
			muxpilotProblem(w, e)
			return
		}
		controls = append(controls, map[string]any{"task_id": taskID, "comment_id": commentID, "generation": gen, "delivery_reserved": active, "status": status, "failure_reason": reason, "outcome_unknown": unknown})
	}
	e = controlRows.Err()
	controlRows.Close()
	if e != nil {
		muxpilotProblem(w, e)
		return
	}
	var held bool
	var expiry time.Time
	err = tx.QueryRow(r.Context(), `SELECT dispatch_held,expires_at FROM muxpilot_coordinator WHERE project_id=$1`, project).Scan(&held, &expiry)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"project_id": project, "workspace_id": workspace, "generation": generation, "dispatch_held": held, "expires_at": expiry, "issues": issues, "runs": runs, "approved_agents": approved, "controls": controls})
}

// Hydration is workspace-scoped and preserves responses from older deployments.
func (h *Handler) hydrateMuxpilotTaskMetadata(ctx context.Context, workspaceID pgtype.UUID, tasks []db.AgentTaskQueue, resp []AgentTaskResponse) {
	if h.DB == nil {
		return
	}
	ids := make([]pgtype.UUID, 0, len(tasks))
	index := map[string]int{}
	for i, task := range tasks {
		ids = append(ids, task.ID)
		index[uuidToString(task.ID)] = i
	}
	rows, err := h.DB.Query(ctx, `SELECT r.task_id::text,r.terminal_url,r.terminal_state,r.session_id,r.generation,COALESCE(t.context->>'muxpilot_base_sha','') FROM muxpilot_run r JOIN agent_task_queue t ON t.id=r.task_id JOIN issue i ON i.id=t.issue_id AND i.project_id=r.project_id WHERE r.task_id=ANY($1::uuid[]) AND i.workspace_id=$2`, ids, workspaceID)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, url, state, session, baseSHA string
		var gen int64
		if rows.Scan(&id, &url, &state, &session, &gen, &baseSHA) != nil {
			return
		}
		if i, ok := index[id]; ok {
			resp[i].MuxpilotTerminalURL = url
			resp[i].MuxpilotTerminalState = state
			resp[i].MuxpilotSessionID = session
			resp[i].MuxpilotGeneration = gen
			resp[i].MuxpilotBaseSHA = baseSHA
		}
	}
}

// MuxpilotSupplementAuthority is a daemon-scoped receiver check immediately
// before provider input. It does not claim atomic acceptance by an independent
// provider after the HTTP response; that boundary is reported separately.
func (h *Handler) MuxpilotSupplementAuthority(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskId")
	if _, ok := h.requireDaemonTaskAccess(w, r, taskID); !ok {
		return
	}
	commentID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "commentId"), "comment_id")
	if !ok {
		return
	}
	var generation int64
	var project string
	err := h.DB.QueryRow(r.Context(), `SELECT m.generation,m.project_id::text FROM muxpilot_supplement m JOIN muxpilot_coordinator c ON c.project_id=m.project_id JOIN task_supplement s ON s.task_id=m.task_id AND s.comment_id=m.comment_id JOIN agent_task_queue t ON t.id=m.task_id JOIN issue i ON i.id=t.issue_id AND i.project_id=m.project_id AND i.workspace_id=c.workspace_id JOIN member membership ON membership.workspace_id=c.workspace_id AND membership.user_id=c.user_id WHERE m.task_id=$1 AND m.comment_id=$2 AND m.generation=c.generation AND c.expires_at>clock_timestamp() AND m.delivery_active AND s.status='delivering' AND t.status='running'`, parseUUID(taskID), commentID).Scan(&generation, &project)
	if errors.Is(err, pgx.ErrNoRows) {
		writeErrorCode(w, 409, "coordinator_revoked", "coordinator guidance authority has ended")
		return
	}
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"generation": generation, "project_id": project, "authorized": true})
}

func muxpilotValidSHA(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

// Recovery reports factual persisted state for an authorized human even when
// the coordinator credential has expired. It never renews or adopts ownership.
func (h *Handler) MuxpilotRecovery(w http.ResponseWriter, r *http.Request) {
	projectID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "projectId"), "project_id")
	if !ok {
		return
	}
	workspaceID, ok := parseUUIDOrBadRequest(w, ctxWorkspaceID(r.Context()), "workspace_id")
	if !ok {
		return
	}
	tx, err := h.TxStarter.Begin(r.Context())
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	defer tx.Rollback(r.Context())
	if _, err = tx.Exec(r.Context(), `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
		muxpilotProblem(w, err)
		return
	}
	q := h.Queries.WithTx(tx)
	if _, err = q.GetProjectInWorkspace(r.Context(), db.GetProjectInWorkspaceParams{ID: projectID, WorkspaceID: workspaceID}); err != nil {
		writeError(w, 404, "project not found")
		return
	}
	var generation int64
	var held bool
	var expires time.Time
	var owner string
	err = tx.QueryRow(r.Context(), `SELECT generation,dispatch_held,expires_at,user_id::text FROM muxpilot_coordinator WHERE project_id=$1 AND workspace_id=$2`, projectID, workspaceID).Scan(&generation, &held, &expires, &owner)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, 200, map[string]any{"project_id": uuidToString(projectID), "registered": false, "readonly": true})
		return
	}
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	rows, err := tx.Query(r.Context(), `SELECT m.task_id::text,m.comment_id::text,m.generation,m.delivery_active,m.outcome_unknown,s.status,COALESCE(s.failure_reason,'') FROM muxpilot_supplement m JOIN task_supplement s ON s.task_id=m.task_id AND s.comment_id=m.comment_id WHERE m.project_id=$1 ORDER BY s.created_at`, projectID)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	controls := []map[string]any{}
	for rows.Next() {
		var task, comment, status, reason string
		var gen int64
		var active, unknown bool
		if err = rows.Scan(&task, &comment, &gen, &active, &unknown, &status, &reason); err != nil {
			rows.Close()
			muxpilotProblem(w, err)
			return
		}
		controls = append(controls, map[string]any{"task_id": task, "comment_id": comment, "generation": gen, "delivery_reserved": active, "status": status, "failure_reason": reason, "outcome_unknown": unknown})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	rows, err = tx.Query(r.Context(), `SELECT operation_id::text,generation,status FROM muxpilot_operation WHERE project_id=$1 ORDER BY operation_id LIMIT 1000`, projectID)
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	ops := []map[string]any{}
	for rows.Next() {
		var id string
		var gen int64
		var status int
		if err = rows.Scan(&id, &gen, &status); err != nil {
			rows.Close()
			muxpilotProblem(w, err)
			return
		}
		ops = append(ops, map[string]any{"operation_id": id, "generation": gen, "status": status})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		muxpilotProblem(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"project_id": uuidToString(projectID), "workspace_id": uuidToString(workspaceID), "generation": generation, "owner_id": owner, "dispatch_held": held, "expires_at": expires, "controls": controls, "operations": ops, "readonly": true})
}
