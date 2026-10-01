-- Muxpilot is opt-in per project. Relationships are validated by the API;
-- no foreign keys or cascade actions are installed.
CREATE TABLE IF NOT EXISTS muxpilot_coordinator (
 project_id uuid PRIMARY KEY,
 workspace_id uuid NOT NULL,
 user_id uuid NOT NULL,
 runtime_profile_id uuid,
 generation bigint NOT NULL CHECK (generation > 0),
 token_hash text NOT NULL,
 expires_at timestamptz NOT NULL,
 dispatch_held boolean NOT NULL DEFAULT true,
 worker_limit integer NOT NULL DEFAULT 1 CHECK (worker_limit BETWEEN 1 AND 64)
);
CREATE TABLE IF NOT EXISTS muxpilot_issue (
 issue_id uuid PRIMARY KEY,
 project_id uuid NOT NULL,
 stage integer NOT NULL DEFAULT 0 CHECK (stage >= 0),
 eligible boolean NOT NULL DEFAULT false,
 base_sha text NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS muxpilot_run (
 task_id uuid PRIMARY KEY,
 project_id uuid NOT NULL,
 generation bigint NOT NULL DEFAULT 0,
 terminal_url text NOT NULL DEFAULT '',
 terminal_state text NOT NULL DEFAULT 'pending',
 session_id text NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS muxpilot_event (
 sequence bigserial PRIMARY KEY,
 id uuid NOT NULL DEFAULT gen_random_uuid(),
 project_id uuid NOT NULL,
 workspace_id uuid NOT NULL,
 actor_type text NOT NULL,
 actor_id text NOT NULL DEFAULT '',
 type text NOT NULL,
 payload jsonb NOT NULL DEFAULT '{}',
 occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS muxpilot_operation (
 operation_id uuid PRIMARY KEY,
 project_id uuid NOT NULL,
 generation bigint NOT NULL,
 request_hash text NOT NULL,
 response jsonb NOT NULL,
 status integer NOT NULL CHECK (status BETWEEN 100 AND 599)
);

-- Lock the coordinator before assigning a feed sequence. This gives each
-- project's cursor commit ordering, and makes control writes and admission
-- mutually exclusive. Payloads deliberately omit task runtime_config,
-- prompts, results, credentials and execution output.
CREATE OR REPLACE FUNCTION record_muxpilot_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE item jsonb; project uuid; ws uuid; body jsonb; actor text; actor_id text;
BEGIN
 IF TG_OP = 'DELETE' THEN item := to_jsonb(OLD); ELSE item := to_jsonb(NEW); END IF;
 IF TG_TABLE_NAME = 'issue' THEN
  project := (item->>'project_id')::uuid;
  ws := (item->>'workspace_id')::uuid;
  body := jsonb_build_object('issue_id',item->'id','title',item->'title','status',item->'status',
    'assignee_type',item->'assignee_type','assignee_id',item->'assignee_id');
  IF TG_OP='INSERT' THEN actor := item->>'creator_type'; actor_id := item->>'creator_id';
  ELSE actor := 'system'; actor_id := ''; END IF;
 ELSIF TG_TABLE_NAME = 'comment' THEN
  SELECT project_id,workspace_id INTO project,ws FROM issue WHERE id=(item->>'issue_id')::uuid;
  body := jsonb_build_object('comment_id',item->'id','issue_id',item->'issue_id',
    'content',item->'content','author_type',item->'author_type','author_id',item->'author_id','deleted_at',item->'deleted_at');
  IF TG_OP='INSERT' THEN actor := item->>'author_type'; actor_id := item->>'author_id';
  ELSE actor := 'system'; actor_id := ''; END IF;
 ELSIF TG_TABLE_NAME = 'task_supplement' THEN
  SELECT project_id,workspace_id INTO project,ws FROM issue WHERE id=(item->>'issue_id')::uuid;
  body := jsonb_build_object('comment_id',item->'comment_id','task_id',item->'task_id',
   'status',item->'status','failure_reason',item->'failure_reason','delivered_at',item->'delivered_at');
  actor := 'daemon';
  SELECT runtime_id::text INTO actor_id FROM agent_task_queue WHERE id=(item->>'task_id')::uuid;
 ELSE
  SELECT project_id,workspace_id INTO project,ws FROM issue WHERE id=(item->>'issue_id')::uuid;
  body := jsonb_build_object('task_id',item->'id','issue_id',item->'issue_id',
    'agent_id',item->'agent_id','status',item->'status');
  actor := 'agent'; actor_id := item->>'agent_id';
 END IF;
 actor := COALESCE(NULLIF(current_setting('muxpilot.actor_type',true),''),NULLIF(current_setting('multica.actor_type',true),''),actor,'system');
 actor_id := COALESCE(NULLIF(current_setting('muxpilot.actor_id',true),''),NULLIF(current_setting('multica.actor_id',true),''),actor_id,'');
 IF TG_TABLE_NAME = 'issue' THEN
  IF TG_OP = 'UPDATE' THEN
   IF OLD.project_id IS DISTINCT FROM NEW.project_id AND OLD.project_id IS NOT NULL THEN
    PERFORM 1 FROM muxpilot_coordinator WHERE project_id=OLD.project_id AND workspace_id=OLD.workspace_id FOR UPDATE;
    IF FOUND THEN
     INSERT INTO muxpilot_event(project_id,workspace_id,actor_type,actor_id,type,payload)
      VALUES(OLD.project_id,OLD.workspace_id,COALESCE(actor,'system'),COALESCE(actor_id,''),'issue.detach',body);
    END IF;
    DELETE FROM muxpilot_issue WHERE issue_id=OLD.id;
   END IF;
  ELSIF TG_OP = 'DELETE' THEN
   DELETE FROM muxpilot_issue WHERE issue_id=OLD.id;
  END IF;
 END IF;
 IF project IS NULL THEN RETURN NULL; END IF;
 PERFORM 1 FROM muxpilot_coordinator WHERE project_id=project AND workspace_id=ws FOR UPDATE;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF TG_TABLE_NAME = 'issue' AND TG_OP <> 'DELETE' THEN
  INSERT INTO muxpilot_issue(issue_id,project_id) VALUES ((item->>'id')::uuid,project)
   ON CONFLICT (issue_id) DO UPDATE SET project_id=EXCLUDED.project_id,eligible=false
    WHERE muxpilot_issue.project_id IS DISTINCT FROM EXCLUDED.project_id;
 END IF;
 INSERT INTO muxpilot_event(project_id,workspace_id,actor_type,actor_id,type,payload)
 VALUES(project,ws,COALESCE(actor,'system'),COALESCE(actor_id,''),TG_TABLE_NAME || '.' || lower(TG_OP),body);
 RETURN NULL;
END $$;
CREATE TRIGGER muxpilot_issue_feed AFTER INSERT OR UPDATE OR DELETE ON issue
 FOR EACH ROW EXECUTE FUNCTION record_muxpilot_event();
CREATE TRIGGER muxpilot_comment_feed AFTER INSERT OR UPDATE OR DELETE ON comment
 FOR EACH ROW EXECUTE FUNCTION record_muxpilot_event();
CREATE TRIGGER muxpilot_task_feed AFTER INSERT OR UPDATE OR DELETE ON agent_task_queue
 FOR EACH ROW EXECUTE FUNCTION record_muxpilot_event();
CREATE TRIGGER muxpilot_supplement_feed AFTER INSERT OR UPDATE OR DELETE ON task_supplement
 FOR EACH ROW EXECUTE FUNCTION record_muxpilot_event();

-- Selection is advisory; the write trigger rechecks after locking the
-- coordinator. Keeping blocked rows out of selection prevents starvation of
-- unrelated work sharing the same runtime or agent.
CREATE OR REPLACE FUNCTION muxpilot_task_claimable(issue_id uuid, task_id uuid) RETURNS boolean
 LANGUAGE sql STABLE AS $$
 SELECT NOT EXISTS (
  SELECT 1 FROM issue i JOIN muxpilot_coordinator c ON c.project_id=i.project_id AND c.workspace_id=i.workspace_id
  LEFT JOIN muxpilot_issue m ON m.issue_id=i.id AND m.project_id=i.project_id
  WHERE i.id=$1 AND (
   c.dispatch_held OR c.expires_at <= statement_timestamp() OR m.issue_id IS NULL OR NOT m.eligible
   OR NOT (m.base_sha ~ '^([0-9a-f]{40}|[0-9a-f]{64})$')
   OR NOT EXISTS(SELECT 1 FROM agent_task_queue task WHERE task.id=$2 AND task.context->>'muxpilot_base_sha'=m.base_sha)
   OR NOT EXISTS(SELECT 1 FROM agent_task_queue task JOIN agent_runtime runtime ON runtime.id=task.runtime_id
    JOIN project_resource resource ON resource.project_id=i.project_id AND resource.workspace_id=i.workspace_id
    WHERE task.id=$2 AND resource.resource_type='local_directory'
     AND resource.resource_ref->>'execution_mode'='worktree'
     AND NULLIF(resource.resource_ref->>'daemon_id','')=runtime.daemon_id
     AND (left(resource.resource_ref->>'local_path',1)='/'
     OR left(resource.resource_ref->>'local_path',2)=chr(92)||chr(92)
     OR (substring(resource.resource_ref->>'local_path',1,1) ~ '^[A-Za-z]$'
      AND substring(resource.resource_ref->>'local_path',2,1)=':'
      AND substring(resource.resource_ref->>'local_path',3,1) IN ('/',chr(92)))))
   OR NOT EXISTS(SELECT 1 FROM agent_task_queue task JOIN agent_runtime runtime ON runtime.id=task.runtime_id
    JOIN runtime_profile profile ON profile.id=runtime.profile_id
    WHERE task.id=$2 AND runtime.workspace_id=i.workspace_id AND profile.workspace_id=i.workspace_id
     AND profile.id=c.runtime_profile_id AND profile.enabled
     AND regexp_replace(profile.command_name,'^.*/','') IN ('muxpilot-worker','muxpilot-worker-fake'))
   OR NOT EXISTS(SELECT 1 FROM muxpilot_run r WHERE r.task_id=$2 AND r.project_id=i.project_id AND r.generation=c.generation)
   OR i.status IN ('backlog','triage','done','cancelled')
   OR EXISTS (SELECT 1 FROM issue_status s WHERE s.workspace_id=i.workspace_id AND s.key=i.status AND s.category='done')
   OR EXISTS (SELECT 1 FROM muxpilot_issue earlier JOIN issue child ON child.id=earlier.issue_id
    WHERE earlier.project_id=i.project_id AND child.project_id=i.project_id AND earlier.stage > 0 AND earlier.stage < m.stage
     AND child.status <> 'done'
     AND NOT EXISTS (SELECT 1 FROM issue_status s WHERE s.workspace_id=i.workspace_id AND s.key=child.status AND s.category='done'))
   OR (SELECT count(*) FROM agent_task_queue q JOIN issue active ON active.id=q.issue_id
    WHERE active.project_id=i.project_id AND active.workspace_id=i.workspace_id
     AND q.status IN ('dispatched','running','waiting_local_directory') AND q.id IS DISTINCT FROM $2) >= c.worker_limit
  )
 )
$$;

-- Every task producer and claimant passes this gate, including automation,
-- reruns, scheduler wakeups and direct SQL claims. Enqueueing remains allowed
-- while held so human comments commit normally; only dispatch is gated.
CREATE OR REPLACE FUNCTION admit_muxpilot_task() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE project uuid; ws uuid; issue_state text; control muxpilot_coordinator%ROWTYPE;
 item muxpilot_issue%ROWTYPE; active_count integer;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF NEW.status NOT IN ('dispatched','running','waiting_local_directory') THEN RETURN NEW; END IF;
  IF NEW.status IS NOT DISTINCT FROM OLD.status AND NEW.issue_id IS NOT DISTINCT FROM OLD.issue_id AND NEW.dispatched_at IS NOT DISTINCT FROM OLD.dispatched_at AND NEW.runtime_id IS NOT DISTINCT FROM OLD.runtime_id AND NEW.context->>'muxpilot_base_sha' IS NOT DISTINCT FROM OLD.context->>'muxpilot_base_sha' THEN RETURN NEW; END IF;
 END IF;
 SELECT project_id,workspace_id,status INTO project,ws,issue_state FROM issue WHERE id=NEW.issue_id;
 IF project IS NULL THEN RETURN NEW; END IF;
 SELECT * INTO control FROM muxpilot_coordinator WHERE project_id=project AND workspace_id=ws FOR UPDATE;
 IF NOT FOUND THEN RETURN NEW; END IF;
 SELECT * INTO item FROM muxpilot_issue WHERE issue_id=NEW.issue_id AND project_id=project;
 IF TG_OP='INSERT' THEN
  NEW.context := jsonb_set(COALESCE(NEW.context,'{}'::jsonb),'{muxpilot_base_sha}',to_jsonb(COALESCE(item.base_sha,'')));
  INSERT INTO muxpilot_run(task_id,project_id,generation) VALUES(NEW.id,project,control.generation)
   ON CONFLICT (task_id) DO NOTHING;
 END IF;
 IF NEW.status NOT IN ('dispatched','running','waiting_local_directory') THEN RETURN NEW; END IF;
 IF NOT EXISTS (SELECT 1 FROM muxpilot_run r WHERE r.task_id=NEW.id AND r.project_id=project AND r.generation=control.generation) THEN
  RAISE EXCEPTION 'muxpilot task coordinator generation is stale' USING ERRCODE='P0001', DETAIL='muxpilot_admission';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM agent_runtime runtime JOIN runtime_profile profile ON profile.id=runtime.profile_id
   WHERE runtime.id=NEW.runtime_id AND runtime.workspace_id=ws AND profile.workspace_id=ws
    AND profile.id=control.runtime_profile_id AND profile.enabled
    AND regexp_replace(profile.command_name,'^.*/','') IN ('muxpilot-worker','muxpilot-worker-fake')) THEN
  RAISE EXCEPTION 'muxpilot managed runtime profile is required' USING ERRCODE='P0001', DETAIL='muxpilot_admission';
 END IF;
 IF item.base_sha IS NULL OR item.base_sha !~ '^([0-9a-f]{40}|[0-9a-f]{64})$'
  OR NEW.context->>'muxpilot_base_sha' IS DISTINCT FROM item.base_sha
  OR NOT EXISTS(SELECT 1 FROM agent_runtime runtime JOIN project_resource resource
    ON resource.project_id=project AND resource.workspace_id=ws
    WHERE runtime.id=NEW.runtime_id AND resource.resource_type='local_directory'
     AND resource.resource_ref->>'execution_mode'='worktree'
     AND NULLIF(resource.resource_ref->>'daemon_id','')=runtime.daemon_id
     AND (left(resource.resource_ref->>'local_path',1)='/'
     OR left(resource.resource_ref->>'local_path',2)=chr(92)||chr(92)
     OR (substring(resource.resource_ref->>'local_path',1,1) ~ '^[A-Za-z]$'
      AND substring(resource.resource_ref->>'local_path',2,1)=':'
      AND substring(resource.resource_ref->>'local_path',3,1) IN ('/',chr(92))))) THEN
  RAISE EXCEPTION 'muxpilot pinned source and matching isolated worktree resource are required' USING ERRCODE='P0001', DETAIL='muxpilot_admission';
 END IF;
 IF control.dispatch_held OR control.expires_at <= clock_timestamp() THEN
  RAISE EXCEPTION 'muxpilot dispatch is held or lease expired' USING ERRCODE='P0001', DETAIL='muxpilot_admission';
 END IF;
 SELECT * INTO item FROM muxpilot_issue WHERE issue_id=NEW.issue_id AND project_id=project;
 IF NOT FOUND OR NOT item.eligible OR issue_state IN ('backlog','triage','done','cancelled')
  OR EXISTS(SELECT 1 FROM issue_status WHERE workspace_id=ws AND key=issue_state AND category IN ('backlog','done','closed','triage')) THEN
  RAISE EXCEPTION 'muxpilot issue is not eligible' USING ERRCODE='P0001', DETAIL='muxpilot_admission';
 END IF;
 IF EXISTS(SELECT 1 FROM muxpilot_issue m JOIN issue i ON i.id=m.issue_id
   WHERE m.project_id=project AND m.stage > 0 AND m.stage < item.stage AND i.project_id=project
    AND i.status <> 'done'
    AND NOT EXISTS(SELECT 1 FROM issue_status s WHERE s.workspace_id=ws AND s.key=i.status AND s.category='done')) THEN
  RAISE EXCEPTION 'muxpilot earlier stage is unfinished' USING ERRCODE='P0001', DETAIL='muxpilot_admission';
 END IF;
 IF NEW.status IN ('dispatched','running','waiting_local_directory') THEN
  SELECT count(*) INTO active_count FROM agent_task_queue q JOIN issue i ON i.id=q.issue_id
   WHERE i.project_id=project AND i.workspace_id=ws AND q.status IN ('dispatched','running','waiting_local_directory') AND q.id <> NEW.id;
  IF active_count >= control.worker_limit THEN
   RAISE EXCEPTION 'muxpilot project worker limit reached' USING ERRCODE='P0001', DETAIL='muxpilot_admission';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER muxpilot_task_admission BEFORE INSERT OR UPDATE OF status,issue_id,dispatched_at,runtime_id,context ON agent_task_queue
 FOR EACH ROW EXECUTE FUNCTION admit_muxpilot_task();
