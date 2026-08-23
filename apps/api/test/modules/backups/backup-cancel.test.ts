/**
 * Cancelling a CAPTURE — queued, and mid-flight.
 *
 * Until this existed, the only thing that could stop a running backup was a bare
 * status flip in project teardown, whose own comment admitted there was no
 * worker-side abort signal: the row read `cancelled` while `tar -c` kept
 * streaming, and every object the run had already put at the destination was
 * orphaned there permanently — retention only ever prunes SUCCEEDED runs, so
 * nothing collects them, and a run with no manifest is not a restore point
 * anybody can use. A queued run could not be stopped at all.
 *
 * What the tests below pin is not "cancel works" but the three things that are
 * easy to get wrong once it does:
 *
 *   1. a queued cancel means the worker never starts (nothing is uploaded);
 *   2. a mid-flight cancel is recorded as `cancelled`, NOT `failed` — the abort
 *      surfaces as an ordinary upload error, so a naive catch would file a
 *      deliberate cancel as a failure and page somebody for it;
 *   3. the objects already uploaded are reclaimed, and the abort reaches the
 *      PRODUCER rather than only the upload — a cancel that leaves the tar
 *      running (and a `quiesce`d container frozen behind it) has not cancelled
 *      anything the operator cares about.
 */

import { describe, expect, it, beforeEach, vi } from "vitest";

/** A one-shot rendezvous: the code under test parks, the test releases it. */
interface Gate {
  hit: Promise<void>;
  arrive: () => Promise<void>;
  release: () => void;
}

const h = vi.hoisted(() => {
  const makeGate = (): Gate => {
    let arrived: () => void = () => {};
    const hit = new Promise<void>((r) => {
      arrived = r;
    });
    let go: () => void = () => {};
    const open = new Promise<void>((r) => {
      go = r;
    });
    return {
      hit,
      arrive: async () => {
        arrived();
        await open;
      },
      release: () => go(),
    };
  };
  return {
    makeGate,
    gate: makeGate(),
    /** The live backup_run row. `findById` hands this back, so a checkpoint
     *  re-reading the cancel flag sees what `requestCancel` wrote. */
    row: {} as Record<string, unknown>,
    /** Artifacts the fake producer yields, in order. */
    artifacts: [] as Array<{ name: string; bytes: string }>,
    /** Which artifact index parks at the gate mid-upload. */
    gateAtArtifact: null as number | null,
    /** The signal the orchestrator handed the producer. */
    producerSignal: null as AbortSignal | null,
    puts: [] as string[],
    deleted: [] as string[],
    notifications: [] as string[],
  };
});

vi.mock("@repo/db", () => ({
  repos: {
    backupRun: {
      findById: async () => h.row,
      requestCancel: async () => {
        h.row.cancelRequested = true;
        h.row.cancelRequestedAt ??= new Date();
        return h.row;
      },
      transition: async (_id: string, status: string, patch?: Record<string, unknown>) => {
        // Mirrors the repo's terminal guard: whoever reaches a terminal state first
        // owns the verdict. Without it this fake would let the run's own `cancelled`
        // be overwritten and the test would pass on a race production refuses.
        const TERMINAL = ["succeeded", "failed", "cancelled", "server_error"];
        if (TERMINAL.includes(String(h.row.status))) return;
        Object.assign(h.row, { status }, patch ?? {});
      },
    },
    backupPolicy: {
      findById: async () => ({
        id: "pol_1",
        destinationId: "dst_1",
        sourceKind: "service",
        mailServerId: null,
        projectId: "prj_1",
        serviceId: "svc_1",
        payloadKind: "auto",
        preHook: null,
        postHook: null,
        hookTimeoutSeconds: 30,
        payloadConfig: {},
      }),
    },
    backupDestination: {
      findById: async () => ({
        id: "dst_1",
        organizationId: "org_1",
        name: "R2",
        pathPrefix: "openship",
      }),
      setLastVerified: async () => {},
    },
    service: {
      findById: async () => ({
        id: "svc_1",
        projectId: "prj_1",
        name: "postgres",
        image: "postgres:16-alpine",
        environment: null,
        volumes: null,
        namespaceVolumes: false,
        ports: null,
        command: null,
      }),
    },
    project: {
      findById: async () => ({
        id: "prj_1",
        slug: "openship",
        name: "Openship",
        organizationId: "org_1",
        activeDeploymentId: "dep_1",
      }),
      listEnvVars: async () => [],
      getEnvMap: async () => ({}),
    },
    deployment: { findById: async () => ({ id: "dep_1", meta: {} }) },
  },
}));

vi.mock("@repo/adapters", async () => {
  const { Readable, PassThrough } = await import("node:stream");
  const { PRESERVED_ARTIFACT_METADATA_KEYS } = await import(
    "../../../../../packages/adapters/src/backup/common/artifact-metadata"
  );
  const { sanitizeProducerOpts } = await import(
    "../../../../../packages/adapters/src/backup/common/producer-opts"
  );
  class FakeHasher extends PassThrough {
    private seen = 0;
    summary() {
      return { sha256: "d0", bytesWritten: this.seen };
    }
    override _transform(chunk: Buffer, _enc: BufferEncoding, cb: (e?: Error | null) => void) {
      this.seen += chunk.byteLength;
      cb(null, chunk);
    }
  }
  return {
    HashingPassthrough: FakeHasher,
    PRESERVED_ARTIFACT_METADATA_KEYS,
    sanitizeProducerOpts,
    artifactKey: (_b: unknown, name: string) => `openship/openship/postgres/bkr_live/${name}`,
    manifestKey: () => "openship/openship/postgres/bkr_live/manifest.json",
    runPrefix: () => "openship/openship/postgres/bkr_live",
    buildManifest: () => ({ version: 1 }),
    resolveDestination: () => ({
      preflight: async () => ({ ok: true }),
      put: async (key: string, body: AsyncIterable<Buffer>) => {
        // Drains, like every real destination — so a cancel that destroys the
        // source stream lands here as a rejection rather than being ignored.
        for await (const _chunk of body) {
          /* consume to EOF */
        }
        h.puts.push(key);
        return {};
      },
      deleteMany: async (keys: string[]) => {
        h.deleted.push(...keys);
        return { deleted: keys, failed: [] };
      },
    }),
    resolveExecutor: () => ({ readContainerEnv: async () => ({}) }),
    resolveProducerForService: () => ({
      kind: "volume",
      async *produce(_svc: unknown, _exec: unknown, opts: { signal?: AbortSignal }) {
        // The real volume producer forwards this into `streamPath`, which is what
        // stops the tar. Captured so the test can assert the orchestrator actually
        // handed one over and aborted it.
        h.producerSignal = opts.signal ?? null;
        for (const [i, a] of h.artifacts.entries()) {
          let stream: import("node:stream").Readable;
          if (h.gateAtArtifact === i) {
            // A stream that delivers its first chunk and then STALLS — never ends.
            // That parks the orchestrator inside `put`, awaiting bytes, which is
            // exactly where a cancel of a large volume lands, and it is the only
            // way to exercise the abort path rather than the next checkpoint. The
            // only thing that can end this upload is the cancel destroying it.
            const stalled = new PassThrough();
            stalled.write(Buffer.from(a.bytes));
            void h.gate.arrive();
            stream = stalled;
          } else {
            stream = Readable.from(a.bytes.length > 0 ? [Buffer.from(a.bytes)] : []);
          }
          yield {
            name: a.name,
            stream,
            payloadKind: "volume",
            metadata: {},
          };
        }
      },
    }),
    resolveProducer: () => ({ kind: "volume", async *produce() {} }),
  };
});

vi.mock("../../../src/lib/job-runner", () => ({
  getJobRunner: async () => ({ enqueueRun: async () => {} }),
}));
vi.mock("../../../src/lib/deployment-runtime", () => ({
  disposeRuntime: () => {},
  disposePlatform: () => {},
  resolveDeploymentPlatform: async () => ({ platform: { runtime: { name: "docker" } } }),
  resolveTargetPlatform: async () => ({ runtime: { name: "docker" } }),
}));
vi.mock("../../../src/lib/encryption", () => ({ decryptEnvMap: (v: unknown) => v }));
vi.mock("../../../src/lib/notification-dispatcher", () => ({
  notification: {
    emit: (e: { eventType: string }) => {
      h.notifications.push(e.eventType);
    },
  },
}));
vi.mock("../../../src/modules/backup-destinations/hydrate-server", () => ({
  toAdapterRow: async (row: unknown) => row,
}));
vi.mock("../../../src/modules/services/service-container", () => ({
  liveContainerIdForService: async () => "c_pg",
  liveContainerForService: async () => ({ containerId: "c_pg", running: true }),
}));

import { BackupOrchestrator } from "../../../src/modules/backups/backup.orchestrator";

const CTX = { organizationId: "org_1", userId: "usr_1" } as never;

beforeEach(() => {
  h.gate = h.makeGate();
  h.row = {
    id: "bkr_live",
    status: "queued",
    policyId: "pol_1",
    serviceId: "svc_1",
    mailServerId: null,
    organizationId: "org_1",
    cancelRequested: false,
    cancelRequestedAt: null,
  };
  h.artifacts = [{ name: "volume-pgdata.tar.zst", bytes: "x".repeat(4096) }];
  h.gateAtArtifact = null;
  h.producerSignal = null;
  h.puts.length = 0;
  h.deleted.length = 0;
  h.notifications.length = 0;
});

describe("cancelling a queued run costs nothing", () => {
  it("terminals the row and the worker then refuses to start it", async () => {
    const orch = new BackupOrchestrator();

    const outcome = await orch.cancel(CTX, "bkr_live");
    expect(outcome).toMatchObject({ accepted: true, status: "cancelled", forced: false });

    // The runner picks the job up regardless — BullMQ had it queued, or the
    // in-process poller listed it before the cancel landed. It must not run.
    await orch.execute("bkr_live");

    expect(h.row.status).toBe("cancelled");
    expect(h.puts).toEqual([]);
    expect(h.notifications).toEqual([]);
  });

  it("refuses to start a queued run whose flag landed after the status was read", async () => {
    // The race the status check alone cannot cover: `execute` read `queued`, and
    // the cancel wrote the flag before the first transition.
    h.row.cancelRequested = true;

    await new BackupOrchestrator().execute("bkr_live");

    expect(h.row.status).toBe("cancelled");
    expect(h.puts).toEqual([]);
  });
});

describe("cancelling mid-capture is a cancel, not a failure", () => {
  it("records cancelled, reclaims what it uploaded, and pages nobody", async () => {
    // Two artifacts: the first lands, the second is interrupted mid-upload. That
    // first object is the one that used to be orphaned at the destination forever.
    h.artifacts = [
      { name: "volume-data.tar.zst", bytes: "x".repeat(2048) },
      { name: "volume-more.tar.zst", bytes: "y".repeat(2048) },
    ];
    h.gateAtArtifact = 1;
    const orch = new BackupOrchestrator();

    const running = orch.execute("bkr_live");
    // Parked mid-upload of the second artifact, with the first already at the
    // destination. Nothing but the cancel can end this stream.
    await h.gate.hit;
    const outcome = await orch.cancel(CTX, "bkr_live");
    await running;

    // Accepted, but NOT terminal on the spot — the capture was between
    // checkpoints, so the caller gets "cancelling…" rather than a status it
    // cannot trust yet.
    expect(outcome.accepted).toBe(true);
    expect(outcome.status).not.toBe("cancelled");
    expect(outcome.forced).toBe(false);

    // The load-bearing assertion: `cancelled`, not `failed`. The abort arrives at
    // the catch as an ordinary upload error, and filing that as a failure would
    // both lie about the run and fire backup_run.failed at a notification channel.
    expect(h.row.status).toBe("cancelled");
    expect(String(h.row.errorMessage)).toMatch(/cancelled/i);
    expect(h.notifications).not.toContain("backup_run.failed");
    expect(h.notifications).not.toContain("backup_run.succeeded");

    // Nothing is left behind, and nothing claims to exist: retention skips
    // non-succeeded runs, so an artifact list naming deleted keys is the same lie
    // as a green run that captured nothing.
    expect(h.deleted).toContain("openship/openship/postgres/bkr_live/volume-data.tar.zst");
    // The interrupted upload never completed — which is also the proof that the
    // ABORT is what ended it: that stream has no other way to end, so a cancel
    // that failed to destroy it would hang this test rather than pass it.
    expect(h.puts).toEqual(["openship/openship/postgres/bkr_live/volume-data.tar.zst"]);
    expect(h.row.artifacts).toEqual([]);
    expect(h.row.bytesTransferred).toBe(0);
    expect(h.puts).not.toContain("openship/openship/postgres/bkr_live/manifest.json");
  });

  it("aborts the signal the producer is holding", async () => {
    // Without this the cancel would only stop the UPLOAD: `tar -c` runs to
    // completion reading the volume, and a quiesced container stays frozen behind
    // it for the rest of the copy.
    h.gateAtArtifact = 0;
    const orch = new BackupOrchestrator();

    const running = orch.execute("bkr_live");
    await h.gate.hit;
    expect(h.producerSignal?.aborted).toBe(false);
    await orch.cancel(CTX, "bkr_live");
    await running;

    expect(h.producerSignal?.aborted).toBe(true);
  });
});

describe("the cancels that decline, and the one that forces", () => {
  it("is idempotent on a run that already finished", async () => {
    h.row.status = "succeeded";

    const outcome = await new BackupOrchestrator().cancel(CTX, "bkr_live");

    // A double-click on a run that just finished is not an error worth showing.
    expect(outcome).toEqual({ accepted: false, status: "succeeded", forced: false });
    expect(h.row.status).toBe("succeeded");
  });

  it("refuses a run in another org", async () => {
    h.row.organizationId = "org_other";

    await expect(new BackupOrchestrator().cancel(CTX, "bkr_live")).rejects.toThrow(
      /not found/i,
    );
  });

  it("force-terminals an in-flight row whose window expired, and says what it left", async () => {
    // No checkpoint answered — the worker is gone, or wedged with no checkpoint
    // ahead of it. The row has to become terminal regardless: while it is
    // in-flight it blocks deleting the project.
    h.row.status = "uploading";
    h.row.cancelRequested = true;
    h.row.cancelRequestedAt = new Date(Date.now() - 5 * 60 * 1000);

    const outcome = await new BackupOrchestrator().cancel(CTX, "bkr_live");

    expect(outcome).toMatchObject({ accepted: true, status: "cancelled", forced: true });
    // A forced cancel knows LESS about what happened than a cooperative one, so it
    // must not imply the destination was cleaned up.
    expect(String(h.row.errorMessage)).toMatch(/left at the destination/i);
  });

  it("does not force one that has not had its window yet", async () => {
    h.row.status = "uploading";
    h.row.cancelRequested = true;
    h.row.cancelRequestedAt = new Date(Date.now() - 5_000);

    const outcome = await new BackupOrchestrator().cancel(CTX, "bkr_live");

    // Still cooperative: forcing here would report `cancelled` over a capture that
    // is demonstrably still streaming.
    expect(outcome).toMatchObject({ accepted: true, status: "uploading", forced: false });
    expect(h.row.status).toBe("uploading");
  });
});
