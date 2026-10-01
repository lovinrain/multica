package muxpilot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func testStore(t *testing.T) (*pgxpool.Pool, *testutil.Fixture, string) {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("requires DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := "muxpilot_test_" + uuid.New().String()[:8]
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) })
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, `CREATE TABLE issue(id uuid PRIMARY KEY DEFAULT gen_random_uuid(), workspace_id uuid NOT NULL,project_id uuid,title text,status text,assignee_type text,assignee_id uuid,creator_type text,creator_id uuid,priority text,position float,number int);
 CREATE TABLE issue_status(workspace_id uuid,key text,category text);
 CREATE TABLE comment(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),issue_id uuid,workspace_id uuid,content text,author_type text,author_id uuid,type text);
 CREATE TABLE agent_task_queue(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),issue_id uuid,agent_id uuid,status text,priority int,runtime_config jsonb,dispatched_at timestamptz,runtime_id uuid,context jsonb);
 CREATE TABLE runtime_profile(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),workspace_id uuid,enabled boolean DEFAULT true,command_name text);
 CREATE TABLE agent_runtime(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),workspace_id uuid,profile_id uuid,daemon_id text);
 CREATE TABLE project_resource(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),project_id uuid,workspace_id uuid,resource_type text,resource_ref jsonb);
 CREATE TABLE task_supplement(comment_id uuid,task_id uuid,workspace_id uuid,issue_id uuid,author_id uuid,client_request_id uuid,status text,attempt_count int DEFAULT 0,failure_reason text,updated_at timestamptz,created_at timestamptz DEFAULT now(),delivered_at timestamptz);`)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"564_muxpilot.up.sql", "565_muxpilot_event_cursor.up.sql", "566_muxpilot_issue_project.up.sql", "567_muxpilot_supplement.up.sql"} {
		sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	// Existing fixture cases use an activated stage source. Pin-specific
	// regressions below explicitly remove or invalidate this fixture default.
	if _, err := pool.Exec(ctx, `ALTER TABLE muxpilot_issue ALTER COLUMN base_sha SET DEFAULT repeat('a',40)`); err != nil {
		t.Fatal(err)
	}
	f := testutil.New(pool, uuid.NewString(), uuid.NewString())
	profile := f.Insert(t, "runtime_profile", testutil.Cols{"workspace_id": f.WorkspaceID, "command_name": "/test/muxpilot-worker-fake"})
	runtime := f.Insert(t, "agent_runtime", testutil.Cols{"workspace_id": f.WorkspaceID, "profile_id": profile, "daemon_id": "muxpilot-test-daemon"})
	f.Exec(t, `ALTER TABLE agent_task_queue ALTER COLUMN runtime_id SET DEFAULT '`+runtime+`'::uuid`)
	project := uuid.NewString()
	f.InsertNoID(t, "muxpilot_coordinator", testutil.Cols{"project_id": project, "workspace_id": f.WorkspaceID, "user_id": f.UserID, "runtime_profile_id": profile, "generation": 1, "token_hash": TokenHash("token"), "expires_at": testutil.Raw("clock_timestamp()+interval '1 hour'"), "dispatch_held": true}, "project_id=$1", project)
	f.Insert(t, "project_resource", testutil.Cols{"project_id": project, "workspace_id": f.WorkspaceID, "resource_type": "local_directory", "resource_ref": testutil.Raw(`'{"daemon_id":"muxpilot-test-daemon","local_path":"/test/repo","execution_mode":"worktree"}'::jsonb`)})
	return pool, f, project
}

func TestDurableFeedAndAdmission(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	f.Issue(t, "Coordinator epic", testutil.Cols{"project_id": project, "status": "backlog"})
	issue := f.Issue(t, "Managed", testutil.Cols{"project_id": project})
	if n := f.Count(t, `SELECT count(*) FROM muxpilot_issue WHERE issue_id=$1 AND NOT eligible`, issue); n != 1 {
		t.Fatal("issue was not automatically held")
	}
	task := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue, "runtime_config": testutil.Raw(`'{"token":"private"}'::jsonb`)})
	f.Comment(t, issue, "A human comment while held")
	reject := func(task string) {
		t.Helper()
		_, err := pool.Exec(ctx, `UPDATE agent_task_queue SET status='dispatched' WHERE id=$1`, task)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Detail != "muxpilot_admission" {
			t.Fatalf("wanted admission rejection, got %v", err)
		}
	}
	reject(task)
	var claimable bool
	f.QueryRow(t, `SELECT muxpilot_task_claimable($1,$2)`, issue, task).Scan(&claimable)
	if claimable {
		t.Fatal("held project remained in claim selection")
	}
	f.QueryRow(t, `SELECT muxpilot_task_claimable(NULL,NULL)`).Scan(&claimable)
	if !claimable {
		t.Fatal("non-issue tasks must remain claimable")
	}
	f.Exec(t, `UPDATE muxpilot_coordinator SET dispatch_held=false WHERE project_id=$1`, project)
	reject(task) // Native issue registration is not permission to start.
	f.Exec(t, `UPDATE muxpilot_issue SET eligible=true,stage=1 WHERE issue_id=$1`, issue)
	f.Exec(t, `UPDATE issue SET status='backlog' WHERE id=$1`, issue)
	reject(task)
	f.Exec(t, `UPDATE issue SET status='todo' WHERE id=$1`, issue)
	later := f.Issue(t, "Later stage", testutil.Cols{"project_id": project})
	f.Exec(t, `UPDATE muxpilot_issue SET eligible=true,stage=2 WHERE issue_id=$1`, later)
	laterTask := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": later})
	reject(laterTask)
	f.Exec(t, `UPDATE issue SET status='cancelled' WHERE id=$1`, issue)
	reject(laterTask) // Cancellation does not satisfy a scoped prerequisite.
	f.Exec(t, `UPDATE issue SET status='backlog' WHERE id=$1`, issue)
	reject(laterTask) // Backlog real stages remain explicit unfinished scope.
	f.Exec(t, `UPDATE issue SET status='todo' WHERE id=$1`, issue)
	f.Exec(t, `UPDATE agent_task_queue SET status='dispatched' WHERE id=$1`, task)
	reject(laterTask)
	f.Exec(t, `UPDATE agent_task_queue SET status='completed' WHERE id=$1`, task)
	f.Exec(t, `UPDATE issue SET status='done' WHERE id=$1`, issue)
	f.Exec(t, `UPDATE agent_task_queue SET status='running' WHERE id=$1`, laterTask)
	f.Exec(t, `UPDATE muxpilot_coordinator SET dispatch_held=true WHERE project_id=$1`, project)
	f.Exec(t, `UPDATE agent_task_queue SET status='completed' WHERE id=$1`, laterTask)
	detached := f.Issue(t, "Detach", testutil.Cols{"project_id": project})
	f.Exec(t, `UPDATE issue SET project_id=NULL WHERE id=$1`, detached)
	if n := f.Count(t, `SELECT count(*) FROM muxpilot_event WHERE type='issue.detach' AND payload->>'issue_id'=$1`, detached); n != 1 {
		t.Fatal("managed project detach was not recorded")
	}
	if n := f.Count(t, `SELECT count(*) FROM muxpilot_issue WHERE issue_id=$1`, detached); n != 0 {
		t.Fatal("detached issue registration survived")
	}

	events, err := FeedEvents(ctx, pool, f.WorkspaceID, project, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 8 {
		t.Fatalf("missing durable events: %d", len(events))
	}
	for n, e := range events {
		var payload map[string]any
		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if _, ok := payload["runtime_config"]; ok {
			t.Fatal("runtime configuration leaked")
		}
		if n > 0 && e.Sequence <= events[n-1].Sequence {
			t.Fatal("cursor not increasing")
		}
	}
	replay, err := FeedEvents(ctx, pool, f.WorkspaceID, project, events[len(events)-1].Sequence, 100)
	if err != nil || len(replay) != 0 {
		t.Fatalf("cursor replay: %v %d", err, len(replay))
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE issue SET title='rolled back' WHERE id=$1`, issue); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)
	if n := f.Count(t, `SELECT count(*) FROM muxpilot_event WHERE payload->>'title'='rolled back'`); n != 0 {
		t.Fatal("feed survived source rollback")
	}
}

func TestFenceAndOperationReplay(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := LockFence(ctx, tx, f.WorkspaceID, project, f.UserID, 1, "wrong"); !errors.Is(err, ErrFence) {
		t.Fatalf("wrong token: %v", err)
	}
	if err := LockFence(ctx, tx, f.WorkspaceID, project, f.UserID, 2, "token"); !errors.Is(err, ErrFence) {
		t.Fatalf("wrong generation: %v", err)
	}
	if err := LockFence(ctx, tx, f.WorkspaceID, project, f.UserID, 1, "token"); err != nil {
		t.Fatal(err)
	}
	op := uuid.NewString()
	if err := SaveOperation(ctx, tx, project, op, 1, "hash", 201, json.RawMessage(`{"created":true}`)); err != nil {
		t.Fatal(err)
	}
	replay, err := ReplayOperation(ctx, tx, project, op, 1, "hash")
	if err != nil || replay == nil || replay.Status != 201 {
		t.Fatalf("replay: %v %v", replay, err)
	}
	if _, err := ReplayOperation(ctx, tx, project, op, 1, "different"); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("reused operation: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE muxpilot_coordinator SET expires_at=clock_timestamp()-interval '1 second' WHERE project_id=$1`, project); err != nil {
		t.Fatal(err)
	}
	if err := LockFence(ctx, tx, f.WorkspaceID, project, f.UserID, 1, "token"); !errors.Is(err, ErrFence) {
		t.Fatalf("expired fence: %v", err)
	}
}

func TestConcurrentClaimsHonorWorkerLimit(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	f.Exec(t, `UPDATE muxpilot_coordinator SET dispatch_held=false WHERE project_id=$1`, project)
	tasks := make([]string, 2)
	for n := range tasks {
		issue := f.Issue(t, "Parallel", testutil.Cols{"project_id": project})
		f.Exec(t, `UPDATE muxpilot_issue SET eligible=true WHERE issue_id=$1`, issue)
		tasks[n] = f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue})
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, task := range tasks {
		wg.Add(1)
		go func(task string) {
			defer wg.Done()
			_, err := pool.Exec(ctx, `UPDATE agent_task_queue SET status='dispatched' WHERE id=$1`, task)
			results <- err
		}(task)
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else {
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Detail != "muxpilot_admission" {
				t.Fatal(err)
			}
		}
	}
	if success != 1 {
		t.Fatalf("concurrent admissions=%d want 1", success)
	}
}

func TestTokens(t *testing.T) {
	a, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 64 || a == b || len(TokenHash(a)) != 64 || TokenHash(a) == a {
		t.Fatal("invalid token entropy or hashing")
	}
}

func TestTakeoverRequiresQueuedTaskAdoption(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	issue := f.Issue(t, "Generation", testutil.Cols{"project_id": project})
	f.Exec(t, `UPDATE muxpilot_issue SET eligible=true WHERE issue_id=$1`, issue)
	task := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue})
	f.Exec(t, `UPDATE muxpilot_coordinator SET generation=2,dispatch_held=false WHERE project_id=$1`, project)
	var claimable bool
	f.QueryRow(t, `SELECT muxpilot_task_claimable($1,$2)`, issue, task).Scan(&claimable)
	if claimable {
		t.Fatal("old queued task remained claimable after takeover")
	}
	if _, err := pool.Exec(ctx, `UPDATE agent_task_queue SET status='dispatched' WHERE id=$1`, task); err == nil {
		t.Fatal("old generation was dispatched")
	}
	f.Exec(t, `UPDATE muxpilot_run SET generation=2 WHERE task_id=$1`, task)
	f.Exec(t, `UPDATE agent_task_queue SET status='dispatched',dispatched_at=now() WHERE id=$1`, task)
	f.Exec(t, `UPDATE muxpilot_coordinator SET generation=3 WHERE project_id=$1`, project)
	if _, err := pool.Exec(ctx, `UPDATE agent_task_queue SET status='running' WHERE id=$1`, task); err == nil {
		t.Fatal("old dispatched generation started")
	}
	if _, err := pool.Exec(ctx, `UPDATE agent_task_queue SET dispatched_at=clock_timestamp() WHERE id=$1`, task); err == nil {
		t.Fatal("old dispatched generation was reclaimed")
	}
}

func TestFeedUsesTrustedNativeMutationActor(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	issue := f.Issue(t, "Attributed", testutil.Cols{"project_id": project})
	comment := f.Comment(t, issue, "Original", testutil.Cols{"author_id": uuid.NewString(), "author_type": "agent"})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('multica.actor_type','member',true),set_config('multica.actor_id',$1,true)`, f.UserID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE issue SET title='Human edit' WHERE id=$1`, issue); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE comment SET content='Human editor' WHERE id=$1`, comment); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := FeedEvents(ctx, pool, f.WorkspaceID, project, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, event := range events {
		if event.Type == "issue.update" || event.Type == "comment.update" {
			found++
			if event.ActorType != "member" || event.ActorID != f.UserID {
				t.Fatalf("editor attribution: %+v", event)
			}
		}
	}
	if found != 2 {
		t.Fatalf("attributed edits=%d want 2", found)
	}
}

func TestCoordinatorSupplementClaimsRejectRevokedAuthority(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	f.Exec(t, `CREATE TABLE task_supplement_capability(task_id uuid,capability text);
 CREATE TABLE "user"(id uuid,name text);`)
	issue := f.Issue(t, "Supplement", testutil.Cols{"project_id": project})
	f.Exec(t, `UPDATE muxpilot_issue SET eligible=true WHERE issue_id=$1`, issue)
	f.Exec(t, `UPDATE muxpilot_coordinator SET dispatch_held=false WHERE project_id=$1`, project)
	task := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue})
	f.Exec(t, `UPDATE agent_task_queue SET status='running' WHERE id=$1`, task)
	f.Exec(t, `UPDATE muxpilot_coordinator SET generation=2 WHERE project_id=$1`, project)
	f.InsertNoID(t, "task_supplement_capability", testutil.Cols{"task_id": task, "capability": "task-supplement-v1"}, "task_id=$1", task)
	supplement := func(content string, generation int64) string {
		comment := f.Comment(t, issue, content)
		f.InsertNoID(t, "task_supplement", testutil.Cols{"comment_id": comment, "task_id": task, "workspace_id": f.WorkspaceID, "issue_id": issue, "author_id": f.UserID, "status": "pending"}, "comment_id=$1", comment)
		if generation > 0 {
			f.InsertNoID(t, "muxpilot_supplement", testutil.Cols{"comment_id": comment, "task_id": task, "project_id": project, "generation": generation}, "comment_id=$1", comment)
		}
		return comment
	}
	stale := supplement("Old coordinator", 1)
	current := supplement("Current coordinator", 2)
	human := supplement("Human direction", 0)
	q := db.New(pool)
	claimed, err := q.ClaimNextTaskSupplement(ctx, util.MustParseUUID(task))
	if err != nil {
		t.Fatal(err)
	}
	if claimed.CommentID != util.MustParseUUID(current) || !claimed.MuxpilotGeneration.Valid || claimed.MuxpilotGeneration.Int64 != 2 || claimed.MuxpilotProjectID != util.MustParseUUID(project) {
		t.Fatalf("current claim metadata: %+v", claimed)
	}
	if n := f.Count(t, `SELECT count(*) FROM muxpilot_supplement WHERE comment_id=$1 AND delivery_active`, current); n != 1 {
		t.Fatal("claimed coordinator supplement did not reserve factual delivery")
	}
	f.Exec(t, `UPDATE task_supplement SET status='failed',failure_reason='turn_ended' WHERE comment_id=$1`, current)
	if n := f.Count(t, `SELECT count(*) FROM muxpilot_supplement WHERE comment_id=$1 AND delivery_active`, current); n != 1 {
		t.Fatal("generic terminal settlement released delivery barrier")
	}
	if _, err := q.AckTaskSupplementFailed(ctx, db.AckTaskSupplementFailedParams{TaskID: util.MustParseUUID(task), CommentID: util.MustParseUUID(current), FailureReason: pgtype.Text{String: "provider_rejected", Valid: true}}); err != nil {
		t.Fatalf("factual failed acknowledgement after terminal settlement: %v", err)
	}
	f.Exec(t, `UPDATE muxpilot_coordinator SET expires_at=clock_timestamp()-interval '1 second' WHERE project_id=$1`, project)
	claimed, err = q.ClaimNextTaskSupplement(ctx, util.MustParseUUID(task))
	if err != nil {
		t.Fatal(err)
	}
	if claimed.CommentID != util.MustParseUUID(human) || claimed.MuxpilotGeneration.Valid || claimed.MuxpilotProjectID.Valid {
		t.Fatalf("human claim changed: %+v", claimed)
	}
	if _, err := q.ClaimNextTaskSupplement(ctx, util.MustParseUUID(task)); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expired coordinator claimed: %v", err)
	}
	f.Exec(t, `UPDATE muxpilot_coordinator SET generation=3,expires_at=clock_timestamp()+interval '1 hour' WHERE project_id=$1`, project)
	newCurrent := supplement("New coordinator", 3)
	claimed, err = q.ClaimNextTaskSupplement(ctx, util.MustParseUUID(task))
	if err != nil {
		t.Fatal(err)
	}
	if claimed.CommentID != util.MustParseUUID(newCurrent) {
		t.Fatal("new coordinator cannot steer existing live old-generation task")
	}
	if n := f.Count(t, `SELECT count(*) FROM task_supplement WHERE comment_id=$1 AND status='pending'`, stale); n != 1 {
		t.Fatal("revoked pending supplement was claimed")
	}
	blocked := supplement("Needs managed run", 3)
	f.Exec(t, `DELETE FROM muxpilot_run WHERE task_id=$1`, task)
	if _, err := q.ClaimNextTaskSupplement(ctx, util.MustParseUUID(task)); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing managed run claimed comment %s: %v", blocked, err)
	}
}

func TestTaskAdmissionRequiresManagedWrapperProfile(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	issue := f.Issue(t, "Wrapper", testutil.Cols{"project_id": project})
	f.Exec(t, `UPDATE muxpilot_issue SET eligible=true WHERE issue_id=$1`, issue)
	f.Exec(t, `UPDATE muxpilot_coordinator SET dispatch_held=false WHERE project_id=$1`, project)
	task := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue})
	var profile string
	f.QueryRow(t, `SELECT runtime_profile_id::text FROM muxpilot_coordinator WHERE project_id=$1`, project).Scan(&profile)
	assertBlocked := func() {
		t.Helper()
		var claimable bool
		f.QueryRow(t, `SELECT muxpilot_task_claimable($1,$2)`, issue, task).Scan(&claimable)
		if claimable {
			t.Fatal("unmanaged wrapper was selected")
		}
		if _, err := pool.Exec(ctx, `UPDATE agent_task_queue SET status='dispatched' WHERE id=$1`, task); err == nil {
			t.Fatal("unmanaged wrapper dispatched")
		}
	}
	f.Exec(t, `UPDATE runtime_profile SET enabled=false WHERE id=$1`, profile)
	assertBlocked()
	f.Exec(t, `UPDATE runtime_profile SET enabled=true,command_name='arbitrary-agent' WHERE id=$1`, profile)
	assertBlocked()
	f.Exec(t, `UPDATE runtime_profile SET command_name='muxpilot-worker-fake' WHERE id=$1`, profile)
	f.Exec(t, `UPDATE muxpilot_coordinator SET runtime_profile_id=NULL WHERE project_id=$1`, project)
	assertBlocked()
	f.Exec(t, `UPDATE muxpilot_coordinator SET runtime_profile_id=$2 WHERE project_id=$1`, project, profile)
	f.Exec(t, `UPDATE agent_task_queue SET status='dispatched' WHERE id=$1`, task)
	if _, err := pool.Exec(ctx, `UPDATE agent_task_queue SET runtime_id=NULL WHERE id=$1`, task); err == nil {
		t.Fatal("runtime rebinding bypassed wrapper gate")
	}
}

func TestManagedNativeTasksInheritPinnedSourceAndRequireWorktree(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	issue := f.Issue(t, "Pinned native task", testutil.Cols{"project_id": project})
	f.Exec(t, `UPDATE muxpilot_issue SET eligible=true,base_sha='' WHERE issue_id=$1`, issue)
	f.Exec(t, `UPDATE muxpilot_coordinator SET dispatch_held=false WHERE project_id=$1`, project)
	task := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue})
	assertBlocked := func() {
		t.Helper()
		var claimable bool
		f.QueryRow(t, `SELECT muxpilot_task_claimable($1,$2)`, issue, task).Scan(&claimable)
		if claimable {
			t.Fatal("unpinned or unisolated native task selected")
		}
		if _, err := pool.Exec(ctx, `UPDATE agent_task_queue SET status='dispatched' WHERE id=$1`, task); err == nil {
			t.Fatal("unpinned or unisolated native task dispatched")
		}
	}
	assertBlocked()
	f.Exec(t, `UPDATE muxpilot_issue SET base_sha=repeat('b',40) WHERE issue_id=$1`, issue)
	assertBlocked() // Earlier queued task has no activated pin.
	f.Exec(t, `UPDATE agent_task_queue SET context=jsonb_build_object('muxpilot_base_sha',repeat('b',40)) WHERE id=$1`, task)
	f.Exec(t, `UPDATE project_resource SET resource_ref=jsonb_set(resource_ref,'{execution_mode}','"in_place"') WHERE project_id=$1`, project)
	assertBlocked()
	f.Exec(t, `UPDATE project_resource SET resource_ref=jsonb_set(resource_ref,'{execution_mode}','"worktree"') WHERE project_id=$1`, project)
	f.Exec(t, `UPDATE project_resource SET resource_ref=jsonb_set(resource_ref,'{local_path}','"relative/repo"') WHERE project_id=$1`, project)
	assertBlocked()
	f.Exec(t, `UPDATE project_resource SET resource_ref=jsonb_set(resource_ref,'{local_path}','"/test/repo"') WHERE project_id=$1`, project)
	f.Exec(t, `UPDATE project_resource SET resource_ref=jsonb_set(resource_ref,'{daemon_id}','"another-daemon"') WHERE project_id=$1`, project)
	assertBlocked()
	f.Exec(t, `UPDATE project_resource SET resource_ref=jsonb_set(resource_ref,'{daemon_id}','"muxpilot-test-daemon"') WHERE project_id=$1`, project)
	f.Exec(t, `UPDATE agent_task_queue SET status='dispatched' WHERE id=$1`, task)
	native := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue, "context": testutil.Raw(`'{"muxpilot_base_sha":"malicious","keep":"value"}'::jsonb`)})
	var pin, keep string
	f.QueryRow(t, `SELECT context->>'muxpilot_base_sha',context->>'keep' FROM agent_task_queue WHERE id=$1`, native).Scan(&pin, &keep)
	if pin != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" || keep != "value" {
		t.Fatalf("native inherited pin %q and context %q", pin, keep)
	}
}

func TestDeliveredSupplementReceiptIsDurableFeedEvent(t *testing.T) {
	pool, f, project := testStore(t)
	ctx := context.Background()
	issue := f.Issue(t, "Receipt feed", testutil.Cols{"project_id": project})
	task := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue})
	comment := f.Comment(t, issue, "Control addition")
	f.InsertNoID(t, "task_supplement", testutil.Cols{"comment_id": comment, "task_id": task, "workspace_id": f.WorkspaceID, "issue_id": issue, "author_id": f.UserID, "status": "delivering", "attempt_count": 1}, "comment_id=$1", comment)
	f.Exec(t, `UPDATE task_supplement SET status='delivered',delivered_at=clock_timestamp() WHERE comment_id=$1 AND task_id=$2`, comment, task)
	events, err := FeedEvents(ctx, pool, f.WorkspaceID, project, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	delivered := 0
	for _, event := range events {
		if event.Type == "task_supplement.update" {
			var receipt struct {
				CommentID   string `json:"comment_id"`
				TaskID      string `json:"task_id"`
				Status      string `json:"status"`
				DeliveredAt string `json:"delivered_at"`
			}
			if err := json.Unmarshal(event.Payload, &receipt); err != nil {
				t.Fatal(err)
			}
			if receipt.CommentID == comment && receipt.TaskID == task && receipt.Status == "delivered" && receipt.DeliveredAt != "" {
				delivered++
			}
		}
	}
	if delivered != 1 {
		t.Fatalf("delivered receipt feed events=%d want1", delivered)
	}
}

func TestSupplementClaimWaitsForTakeoverBeforeReceiptLock(t *testing.T) {
	pool, f, project := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f.Exec(t, `CREATE TABLE task_supplement_capability(task_id uuid,capability text);CREATE TABLE "user"(id uuid,name text);`)
	issue := f.Issue(t, "Takeover receipt", testutil.Cols{"project_id": project})
	f.Exec(t, `UPDATE muxpilot_issue SET eligible=true WHERE issue_id=$1`, issue)
	f.Exec(t, `UPDATE muxpilot_coordinator SET dispatch_held=false WHERE project_id=$1`, project)
	task := f.Task(t, uuid.NewString(), testutil.Cols{"issue_id": issue})
	f.Exec(t, `UPDATE agent_task_queue SET status='running' WHERE id=$1`, task)
	comment := f.Comment(t, issue, "Pending old generation")
	f.InsertNoID(t, "task_supplement_capability", testutil.Cols{"task_id": task, "capability": "task-supplement-v1"}, "task_id=$1", task)
	f.InsertNoID(t, "task_supplement", testutil.Cols{"task_id": task, "comment_id": comment, "issue_id": issue, "workspace_id": f.WorkspaceID, "author_id": f.UserID, "status": "pending"}, "comment_id=$1", comment)
	f.InsertNoID(t, "muxpilot_supplement", testutil.Cols{"task_id": task, "comment_id": comment, "project_id": project, "generation": 1}, "comment_id=$1", comment)
	takeover, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer takeover.Rollback(ctx)
	if err := LockFence(ctx, takeover, f.WorkspaceID, project, f.UserID, 1, "token"); err != nil {
		t.Fatal(err)
	}
	if _, err := takeover.Exec(ctx, `UPDATE muxpilot_coordinator SET generation=2 WHERE project_id=$1`, project); err != nil {
		t.Fatal(err)
	}
	delivery, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer delivery.Release()
	results := make(chan error, 1)
	go func() {
		_, err := db.New(delivery).ClaimNextTaskSupplement(ctx, util.MustParseUUID(task))
		results <- err
	}()
	blocked := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx, `SELECT cardinality(pg_blocking_pids($1))>0`, int32(delivery.Conn().PgConn().PID())).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("supplement claim did not wait for the coordinator fence")
	}
	// The blocked claim must not hold the receipt first: takeover can inspect
	// or revoke it while it owns the coordinator without a lock cycle.
	var receipt string
	if err := takeover.QueryRow(ctx, `SELECT comment_id::text FROM task_supplement WHERE task_id=$1 AND comment_id=$2 FOR UPDATE NOWAIT`, task, comment).Scan(&receipt); err != nil {
		t.Fatalf("claim locked receipt before coordinator: %v", err)
	}
	if err := takeover.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-results:
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("old generation delivery after takeover: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if n := f.Count(t, `SELECT count(*) FROM task_supplement WHERE comment_id=$1 AND status='pending'`, comment); n != 1 {
		t.Fatal("stale delivery was reserved")
	}
}
