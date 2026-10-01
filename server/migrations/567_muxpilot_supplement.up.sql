-- Coordinator provenance is separate from human supplement receipts so
-- ordinary human-authored additions retain their existing delivery semantics.
CREATE TABLE IF NOT EXISTS muxpilot_supplement (
 comment_id uuid NOT NULL,
 task_id uuid NOT NULL,
 project_id uuid NOT NULL,
 generation bigint NOT NULL CHECK (generation > 0),
 -- Remains true after generic turn settlement; only transport acknowledgement
 -- releases the takeover barrier for possibly accepted provider input.
 delivery_active boolean NOT NULL DEFAULT false,
 outcome_unknown boolean NOT NULL DEFAULT false,
 PRIMARY KEY (comment_id,task_id)
);
