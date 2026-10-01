CREATE INDEX CONCURRENTLY IF NOT EXISTS muxpilot_event_cursor ON muxpilot_event(workspace_id,project_id,sequence);
