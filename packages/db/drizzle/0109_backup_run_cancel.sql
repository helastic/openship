-- Make "cancel a backup capture" durable — the twin of 0095 for the other half
-- of the module.
--
-- Until now the only thing that could cancel a capture was a bare status flip in
-- project teardown, whose own comment said there was no worker-side abort signal
-- to go with it: the row read `cancelled` while `tar -c` kept streaming, and the
-- objects the run had already put at the destination were orphaned (retention
-- only prunes SUCCEEDED runs, so nothing ever collects them).
--
-- Same three columns and same reasoning as `backup_restore`: the node taking the
-- cancel is not guaranteed to be the node streaming the artifact, so the request
-- has to outlive the request handler; and `cancel_requested_at` is the window
-- after which a wedged in-flight row can be force-terminaled, which matters here
-- because an in-flight run blocks deleting its project.
ALTER TABLE "backup_run" ADD COLUMN IF NOT EXISTS "cancel_requested" boolean DEFAULT false NOT NULL;
--> statement-breakpoint
ALTER TABLE "backup_run" ADD COLUMN IF NOT EXISTS "cancel_requested_at" timestamp;
--> statement-breakpoint
ALTER TABLE "backup_run" ADD COLUMN IF NOT EXISTS "cancelled_at" timestamp;
