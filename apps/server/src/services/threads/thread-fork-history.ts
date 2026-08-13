import {
  appendDaemonEventsInTransaction,
  listStoredEventRows,
  type AppendDaemonEventInput,
  type StoredEventRow,
} from "@bb/db";
import type { ThreadEventScope, ThreadEventType } from "@bb/domain";
import type { AppDeps } from "../../types.js";

/**
 * Event types that carry the visible conversation. Cloning is limited to
 * these so a fork renders the history it inherits without dragging along the
 * source's provisioning, identity, delta-streaming, or usage bookkeeping —
 * the fork writes its own versions of those.
 */
const CLONED_CONVERSATION_EVENT_TYPES = [
  "client/turn/requested",
  "turn/started",
  "turn/input/accepted",
  "turn/completed",
  "item/completed",
] satisfies ThreadEventType[];

interface CloneForkSourceTimelineArgs {
  sourceThreadId: string;
  /** Inclusive last source sequence to clone; undefined clones everything. */
  sourceSeqEnd: number | undefined;
  targetThreadId: string;
  targetEnvironmentId: string | null;
}

function scopeForStoredRow(row: StoredEventRow): ThreadEventScope | null {
  if (row.scopeKind === "thread") {
    return { kind: "thread" };
  }
  if (row.scopeKind === "turn" && row.turnId !== null) {
    return { kind: "turn", turnId: row.turnId };
  }
  return null;
}

/**
 * Copy the source thread's conversation events into a freshly created fork so
 * its timeline shows the history its provider session inherits.
 *
 * The provider session clone (thread/fork at the bridge) already carries the
 * full conversation to the model; without this the fork's *visible* timeline
 * starts empty. Rows are re-appended through the daemon ingestion path so
 * sequences are re-numbered for the target thread, search segments are
 * indexed, and watchers receive the standard `events-appended` notification.
 *
 * Turn ids and item ids are copied verbatim: event uniqueness is scoped by
 * thread, and keeping the source ids preserves turn grouping in the timeline
 * projection. `providerThreadId` is also kept — it names the historical
 * provider session those events came from, which is exactly what the cloned
 * session file contains, and the timeline projection does not key on it.
 *
 * Runs before the fork's own provisioning events are appended, so cloned
 * history occupies the lowest sequences and renders above the fork's own
 * activity. A failure is logged and swallowed: a fork without visible history
 * (today's behavior) beats a failed fork.
 */
export function cloneForkSourceTimeline(
  deps: Pick<AppDeps, "db" | "hub" | "logger">,
  args: CloneForkSourceTimelineArgs,
): number {
  try {
    const rows = listStoredEventRows(deps.db, {
      threadId: args.sourceThreadId,
      types: CLONED_CONVERSATION_EVENT_TYPES,
      ...(args.sourceSeqEnd === undefined
        ? {}
        : { beforeSequence: args.sourceSeqEnd + 1 }),
    });
    const inputs: AppendDaemonEventInput[] = [];
    for (const row of rows) {
      const scope = scopeForStoredRow(row);
      if (scope === null) {
        continue;
      }
      inputs.push({
        data: row.data,
        environmentId: args.targetEnvironmentId,
        itemId: row.itemId,
        itemKind: row.itemKind,
        parentToolCallId: row.parentToolCallId,
        providerThreadId: row.providerThreadId,
        scope,
        threadId: args.targetThreadId,
        type: row.type as ThreadEventType,
      });
    }
    if (inputs.length === 0) {
      return 0;
    }
    const result = deps.db.transaction(
      (tx) => appendDaemonEventsInTransaction(tx, inputs),
      { behavior: "immediate" },
    );
    deps.hub.notifyThread(args.targetThreadId, ["events-appended"], {
      eventTypes: Array.from(new Set(inputs.map((input) => input.type))),
    });
    return result.acceptedEvents.length;
  } catch (error) {
    deps.logger.warn(
      `Failed to clone source timeline into fork ${args.targetThreadId} from ${args.sourceThreadId}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
    return 0;
  }
}
