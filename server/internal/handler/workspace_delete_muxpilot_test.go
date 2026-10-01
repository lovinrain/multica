package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/middleware"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestDeleteWorkspace_RemovesMuxpilotMetadataWithoutRecreatingFeed(t *testing.T) {
	if testPool == nil {
		t.Skip("database not available")
	}
	f := struct {
		victimID, neighbourID, victimIssue, neighbourIssue, victimAgent, victimRuntime, taskViaIssue, neighbourTask string
	}{}
	f.victimID = dbfx.Workspace(t, "Muxpilot deletion victim", "handler-tests-delete-muxpilot-"+uuid.NewString())
	f.neighbourID = dbfx.Workspace(t, "Muxpilot deletion neighbour", "handler-tests-delete-muxpilot-"+uuid.NewString())
	dbfx.Member(t, f.victimID, testUserID, "owner")
	f.victimRuntime = dbfx.Runtime(t, "Victim runtime", testutil.Cols{"workspace_id": f.victimID})
	neighbourRuntime := dbfx.Runtime(t, "Neighbour runtime", testutil.Cols{"workspace_id": f.neighbourID})
	f.victimAgent = dbfx.Agent(t, "Victim agent", f.victimRuntime, testutil.Cols{"workspace_id": f.victimID})
	neighbourAgent := dbfx.Agent(t, "Neighbour agent", neighbourRuntime, testutil.Cols{"workspace_id": f.neighbourID})
	f.victimIssue = dbfx.Issue(t, "Victim issue", testutil.Cols{"workspace_id": f.victimID})
	f.neighbourIssue = dbfx.Issue(t, "Neighbour issue", testutil.Cols{"workspace_id": f.neighbourID})
	f.taskViaIssue = dbfx.Task(t, f.victimAgent, testutil.Cols{"issue_id": f.victimIssue, "runtime_id": f.victimRuntime, "status": "completed", "completed_at": time.Now()})
	f.neighbourTask = dbfx.Task(t, neighbourAgent, testutil.Cols{"issue_id": f.neighbourIssue, "runtime_id": neighbourRuntime, "status": "completed", "completed_at": time.Now()})
	victimProject := dbfx.Project(t, "Managed victim", testutil.Cols{"workspace_id": f.victimID})
	orphanProject := dbfx.Project(t, "Victim without coordinator", testutil.Cols{"workspace_id": f.victimID})
	neighbourProject := dbfx.Project(t, "Managed neighbour", testutil.Cols{"workspace_id": f.neighbourID})
	dbfx.Exec(t, `UPDATE issue SET project_id=$1 WHERE id=$2`, victimProject, f.victimIssue)
	dbfx.Exec(t, `UPDATE issue SET project_id=$1 WHERE id=$2`, neighbourProject, f.neighbourIssue)
	orphanIssue := dbfx.Issue(t, "Orphan metadata owner", testutil.Cols{"workspace_id": f.victimID, "project_id": orphanProject})
	orphanTask := dbfx.Task(t, f.victimAgent, testutil.Cols{"issue_id": orphanIssue, "runtime_id": f.victimRuntime, "status": "completed", "completed_at": time.Now()})

	seed := func(ws, project, issue, task string, coordinator bool) {
		t.Helper()
		if coordinator {
			dbfx.InsertNoID(t, "muxpilot_coordinator", testutil.Cols{
				"project_id": project, "workspace_id": ws, "user_id": testUserID,
				"generation": 1, "token_hash": "test-fence", "expires_at": time.Now().Add(time.Hour),
			}, "project_id=$1", project)
		}
		dbfx.InsertNoID(t, "muxpilot_issue", testutil.Cols{"issue_id": issue, "project_id": project}, "project_id=$1", project)
		dbfx.InsertNoID(t, "muxpilot_run", testutil.Cols{"task_id": task, "project_id": project, "generation": 1}, "project_id=$1", project)
		dbfx.InsertNoID(t, "muxpilot_supplement", testutil.Cols{
			"comment_id": uuid.NewString(), "task_id": task, "project_id": project, "generation": 1,
		}, "project_id=$1", project)
		dbfx.InsertNoID(t, "muxpilot_operation", testutil.Cols{
			"operation_id": uuid.NewString(), "project_id": project, "generation": 1,
			"request_hash": "test-request", "response": "{}", "status": 200,
		}, "project_id=$1", project)
		dbfx.Insert(t, "muxpilot_event", testutil.Cols{
			"project_id": project, "workspace_id": ws, "actor_type": "system", "type": "fixture",
		})
	}
	seed(f.victimID, victimProject, f.victimIssue, f.taskViaIssue, true)
	seed(f.victimID, orphanProject, orphanIssue, orphanTask, false)
	seed(f.neighbourID, neighbourProject, f.neighbourIssue, f.neighbourTask, true)

	req := withURLParam(newRequest("DELETE", "/api/workspaces/"+f.victimID, nil), "id", f.victimID)
	testutil.Call(t, testHandler.DeleteWorkspace, req).Want(http.StatusNoContent)
	for _, table := range []string{"muxpilot_coordinator", "muxpilot_issue", "muxpilot_run", "muxpilot_supplement", "muxpilot_operation", "muxpilot_event"} {
		var victimCount, neighbourCount int
		if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM `+table+` WHERE project_id=ANY($1::uuid[])`, []string{victimProject, orphanProject}).Scan(&victimCount); err != nil {
			t.Fatalf("count victim %s: %v", table, err)
		}
		if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM `+table+` WHERE project_id=$1`, neighbourProject).Scan(&neighbourCount); err != nil {
			t.Fatalf("count neighbour %s: %v", table, err)
		}
		if victimCount != 0 || neighbourCount != 1 {
			t.Errorf("%s after deletion: victim=%d (want 0), neighbour=%d (want 1)", table, victimCount, neighbourCount)
		}
	}
}

func TestMuxpilotCoordinatorCreation_FencedByWorkspaceDeletion(t *testing.T) {
	if testPool == nil {
		t.Skip("database not available")
	}
	for _, action := range []string{"lease", "register"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			ws := dbfx.Workspace(t, "Coordinator deletion fence", "handler-tests-muxpilot-fence-"+uuid.NewString())
			dbfx.Member(t, ws, testUserID, "owner")
			project := dbfx.Project(t, "Coordinator fence project", testutil.Cols{"workspace_id": ws})
			member, err := testHandler.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{WorkspaceID: parseUUID(ws), UserID: parseUUID(testUserID)})
			if err != nil {
				t.Fatal(err)
			}
			payload := map[string]any{"operation_id": uuid.NewString(), "worker_limit": 2}
			handler := testHandler.MuxpilotLease
			if action == "register" {
				daemonID := uuid.NewString()
				dbfx.Runtime(t, "Fence daemon", testutil.Cols{"workspace_id": ws, "daemon_id": daemonID, "metadata": `{"capabilities":["local-worktree-v1"]}`})
				payload["daemon_id"] = daemonID
				payload["name"] = "Fence registration"
				payload["goal"] = "Fixture only"
				payload["repo_root"] = t.TempDir()
				handler = testHandler.MuxpilotRegister
			}
			req := withURLParam(newRequest("POST", "/"+action, payload), "projectId", project)
			req = req.WithContext(middleware.SetMemberContext(req.Context(), ws, member))
			holder, err := testPool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Rollback(ctx)
			if _, err := holder.Exec(ctx, `SELECT id FROM workspace WHERE id=$1 FOR UPDATE`, ws); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { handler(response, req); close(done) }()
			// Prove the handler is queued behind OUR workspace lock rather
			// than using a timing assumption about the deletion window.
			blocked := false
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if err := testPool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1::integer=ANY(pg_blocking_pids(pid)))`, holder.Conn().PgConn().PID()).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				select {
				case <-done:
					t.Fatalf("%s escaped workspace deletion fence: status %d: %s", action, response.Code, response.Body.String())
				default:
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !blocked {
				t.Fatal("coordinator writer did not queue behind workspace lock")
			}
			if _, err := holder.Exec(ctx, `DELETE FROM workspace WHERE id=$1`, ws); err != nil {
				t.Fatal(err)
			}
			if err := holder.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("coordinator writer did not exit after workspace deletion")
			}
			if response.Code != http.StatusNotFound {
				t.Fatalf("%s after deletion: status %d, want 404: %s", action, response.Code, response.Body.String())
			}
			for _, table := range []string{"muxpilot_coordinator", "muxpilot_operation"} {
				var count int
				if err := testPool.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE project_id=$1`, project).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Errorf("%s recreated %s after deletion", action, table)
				}
			}
		})
	}
}
