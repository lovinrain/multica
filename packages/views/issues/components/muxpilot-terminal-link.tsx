"use client";

import type { AgentTask } from "@multica/core/types";
import { safeHttpUrl } from "@multica/core/api/schema";
import { Terminal } from "lucide-react";
import { useT } from "../../i18n";

export function MuxpilotTerminalLink({ task }: { task: AgentTask }) {
  const { t } = useT("issues");
  const state = task.muxpilot_terminal_state;
  const url = safeHttpUrl(task.muxpilot_terminal_url);
  if (!state && !task.muxpilot_session_id && !task.muxpilot_terminal_url) return null;
  if (!url || (state !== "live" && state !== "history")) {
    return <span className="shrink-0 text-caption text-muted-foreground">{t(($) => $.muxpilot.terminal.unavailable)}</span>;
  }
  return <a href={url} target="_blank" rel="noopener noreferrer"
    className="inline-flex shrink-0 items-center gap-1 rounded-xs text-caption text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
    <Terminal aria-hidden className="size-3.5" />
    {state === "history" ? t(($) => $.muxpilot.terminal.history) : t(($) => $.muxpilot.terminal.live)}
  </a>;
}
