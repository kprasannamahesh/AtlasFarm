import { useCallback, useEffect, useMemo, useState } from "react";
import type { FormEvent, ReactNode } from "react";

const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080").replace(/\/$/, "");

class APIError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

type Worker = {
  id: string;
  status: string;
  cpu: number;
  used_cpu: number;
  memory_mb: number;
  used_memory_mb: number;
  gpu: number;
  used_gpu: number;
  runner_version: string;
  last_heartbeat: string;
};

type JobEvent = {
  sequence_num: number;
  event_type: string;
  payload: Record<string, unknown>;
  trace_id?: string;
  created_at: string;
};

type Job = {
  id: string;
  tenant_id: string;
  idempotency_key: string;
  command: string;
  req_cpu: number;
  req_memory_mb: number;
  req_gpu: number;
  status: string;
  worker_id?: string;
  execution_id: number;
  trace_id?: string;
  created_at: string;
  events?: JobEvent[];
};

type Rollout = {
  id: number;
  target_version: string;
  canary_percentage: number;
  error_threshold_pct: number;
  status: "ACTIVE" | "ROLLED_BACK" | "COMPLETED";
  created_at: string;
};

type Tab = "cluster" | "jobs" | "rollout";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new APIError(response.status, body.error ?? response.statusText);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

function percentage(used: number, total: number) {
  return total > 0 ? Math.min(100, Math.round((used / total) * 100)) : 0;
}

function statusColor(status: string) {
  const normalized = status.toLowerCase();
  if (["online", "succeeded", "completed"].includes(normalized)) return "bg-emerald-100 text-emerald-700";
  if (["running", "scheduled", "active"].includes(normalized)) return "bg-blue-100 text-blue-700";
  if (["failed", "rolled_back", "unhealthy"].includes(normalized)) return "bg-rose-100 text-rose-700";
  return "bg-slate-100 text-slate-600";
}

function App() {
  const [tab, setTab] = useState<Tab>("cluster");
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [showOfflineWorkers, setShowOfflineWorkers] = useState(false);
  const [purgingWorkers, setPurgingWorkers] = useState(false);
  const [notice, setNotice] = useState("");
  const [jobs, setJobs] = useState<Job[]>([]);
  const [rollout, setRollout] = useState<Rollout | null>(null);
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [error, setError] = useState("");
  const [connected, setConnected] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [rolloutBusy, setRolloutBusy] = useState(false);
  const [tenantId, setTenantId] = useState("tenant-a");
  const [command, setCommand] = useState("echo hello from AtlasFarm");
  const [cpu, setCpu] = useState(1);
  const [memory, setMemory] = useState(1024);
  const [gpu, setGpu] = useState(0);
  const [targetVersion, setTargetVersion] = useState("v2.0.0-canary");
  const [canaryPercentage, setCanaryPercentage] = useState(5);
  const [errorThreshold, setErrorThreshold] = useState(10);

  const refresh = useCallback(async () => {
    try {
      const [nextWorkers, nextJobs] = await Promise.all([
        request<Worker[]>("/api/v1/workers"),
        request<Job[]>("/api/v1/jobs"),
      ]);
      setWorkers(nextWorkers);
      setJobs(nextJobs);
      try {
        setRollout(await request<Rollout>("/api/v1/rollouts/current"));
      } catch (rolloutError) {
        if (rolloutError instanceof APIError && rolloutError.status === 404) setRollout(null);
        else throw rolloutError;
      }
      setLastUpdated(new Date());
      setConnected(true);
      setError("");
    } catch (refreshError) {
      setConnected(false);
      setError(refreshError instanceof Error ? refreshError.message : "Unable to refresh AtlasFarm data");
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  const onlineWorkers = useMemo(
    () => workers.filter(isActiveWorker),
    [workers],
  );
  const offlineWorkerCount = workers.length - onlineWorkers.length;
  const visibleWorkers = showOfflineWorkers ? workers : onlineWorkers;

  async function submitJob(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      const job = await request<Job>("/api/v1/jobs", {
        method: "POST",
        body: JSON.stringify({
          tenant_id: tenantId,
          idempotency_key: crypto.randomUUID(),
          command,
          req_cpu: Number(cpu),
          req_memory_mb: Number(memory),
          req_gpu: Number(gpu),
        }),
      });
      setTab("jobs");
      await refresh();
      await openJob(job.id);
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Job submission failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function openJob(id: string) {
    try {
      setSelectedJob(await request<Job>(`/api/v1/jobs/${id}`));
    } catch (jobError) {
      setError(jobError instanceof Error ? jobError.message : "Unable to load job details");
    }
  }

  async function saveRollout(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setRolloutBusy(true);
    setError("");
    try {
      setRollout(await request<Rollout>("/api/v1/rollouts", {
        method: "POST",
        body: JSON.stringify({
          target_version: targetVersion,
          canary_percentage: Number(canaryPercentage),
          error_threshold_pct: Number(errorThreshold),
        }),
      }));
      await refresh();
    } catch (rolloutError) {
      setError(rolloutError instanceof Error ? rolloutError.message : "Unable to create rollout");
    } finally {
      setRolloutBusy(false);
    }
  }

  async function rollback() {
    setRolloutBusy(true);
    setError("");
    try {
      setRollout(await request<Rollout>("/api/v1/rollouts/rollback", { method: "POST" }));
    } catch (rolloutError) {
      setError(rolloutError instanceof Error ? rolloutError.message : "Rollback failed");
    } finally {
      setRolloutBusy(false);
    }
  }

  async function purgeOfflineWorkers() {
    setPurgingWorkers(true);
    setError("");
    setNotice("");
    try {
      const result = await request<{ purged: number }>("/api/v1/workers/stale", { method: "DELETE" });
      setNotice(`Purged ${result.purged} offline worker${result.purged === 1 ? "" : "s"}.`);
      await refresh();
    } catch (purgeError) {
      setError(purgeError instanceof Error ? purgeError.message : "Unable to purge offline workers");
    } finally {
      setPurgingWorkers(false);
    }
  }

  return (
    <div className="min-h-screen bg-canvas">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-5 py-4 lg:px-8">
          <div className="flex items-center gap-3">
            <div className="grid h-10 w-10 place-items-center rounded-xl bg-brand text-lg font-bold text-white">A</div>
            <div>
              <p className="font-bold tracking-tight text-ink">AtlasFarm</p>
              <p className="text-xs text-muted">Operations console</p>
            </div>
          </div>
          <div className="flex items-center gap-3 text-sm">
            <span className="hidden text-muted sm:inline">
              {lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : "Connecting"}
            </span>
            <span className={`flex items-center gap-2 rounded-full px-3 py-1.5 font-medium ${connected ? "bg-emerald-50 text-emerald-700" : "bg-slate-100 text-slate-500"}`}>
              <span className={`h-2 w-2 rounded-full ${connected ? "bg-emerald-500" : "bg-slate-400"}`} />
              {connected ? "API connected" : "API disconnected"}
            </span>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-5 py-8 lg:px-8">
        <div className="mb-7 flex flex-col justify-between gap-5 md:flex-row md:items-end">
          <div>
            <p className="text-sm font-semibold uppercase tracking-[0.16em] text-brand">Control plane</p>
            <h1 className="mt-2 text-3xl font-bold tracking-tight text-ink">Fleet operations</h1>
            <p className="mt-2 text-muted">Monitor capacity, inspect executions, and manage safe releases.</p>
          </div>
          <div className="grid grid-cols-3 gap-3">
            <Summary label="Active workers" value={onlineWorkers.length} />
            <Summary label="Recent jobs" value={jobs.length} />
            <Summary label="Canary traffic" value={`${rollout?.canary_percentage ?? 0}%`} />
          </div>
        </div>

        {error && (
          <div role="alert" className="mb-5 flex items-center justify-between rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800">
            <span>{error}</span>
            <button className="font-semibold" onClick={() => setError("")}>Dismiss</button>
          </div>
        )}
        {notice && (
          <div role="status" className="mb-5 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-800">
            {notice}
          </div>
        )}
        {rollout?.status === "ROLLED_BACK" && (
          <div role="alert" className="mb-5 rounded-xl border border-rose-300 bg-rose-50 px-5 py-4 text-rose-900 shadow-card">
            <p className="font-bold">Automatic rollback triggered</p>
            <p className="mt-1 text-sm">
              Release {rollout.target_version} exceeded its {rollout.error_threshold_pct}% error threshold.
              Canary traffic has been reduced to 0%.
            </p>
          </div>
        )}

        <nav className="mb-6 flex gap-1 overflow-x-auto rounded-xl border border-slate-200 bg-white p-1.5 shadow-card" aria-label="Operations sections">
          <TabButton active={tab === "cluster"} onClick={() => setTab("cluster")}>Cluster &amp; Workers</TabButton>
          <TabButton active={tab === "jobs"} onClick={() => setTab("jobs")}>Jobs &amp; Events</TabButton>
          <TabButton active={tab === "rollout"} onClick={() => setTab("rollout")}>Canary Rollout</TabButton>
        </nav>

        {tab === "cluster" && (
          <ClusterPanel
            workers={visibleWorkers}
            activeWorkers={onlineWorkers}
            offlineCount={offlineWorkerCount}
            showOfflineWorkers={showOfflineWorkers}
            onShowOfflineWorkersChange={setShowOfflineWorkers}
            onPurgeOffline={purgeOfflineWorkers}
            purging={purgingWorkers}
          />
        )}
        {tab === "jobs" && (
          <JobsPanel
            jobs={jobs}
            selectedJob={selectedJob}
            onSubmit={submitJob}
            onOpenJob={openJob}
            onCloseJob={() => setSelectedJob(null)}
            submitting={submitting}
            tenantId={tenantId}
            setTenantId={setTenantId}
            command={command}
            setCommand={setCommand}
            cpu={cpu}
            setCpu={setCpu}
            memory={memory}
            setMemory={setMemory}
            gpu={gpu}
            setGpu={setGpu}
          />
        )}
        {tab === "rollout" && (
          <RolloutPanel
            rollout={rollout}
            targetVersion={targetVersion}
            setTargetVersion={setTargetVersion}
            canaryPercentage={canaryPercentage}
            setCanaryPercentage={setCanaryPercentage}
            errorThreshold={errorThreshold}
            setErrorThreshold={setErrorThreshold}
            onSubmit={saveRollout}
            onRollback={rollback}
            busy={rolloutBusy}
          />
        )}
      </main>
    </div>
  );
}

function Summary({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="min-w-24 rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-card">
      <p className="text-xs font-medium text-muted">{label}</p>
      <p className="mt-1 text-xl font-bold text-ink">{value}</p>
    </div>
  );
}

function TabButton({ active, onClick, children }: { active: boolean; onClick: () => void; children: ReactNode }) {
  return (
    <button
      className={`whitespace-nowrap rounded-lg px-4 py-2.5 text-sm font-semibold transition ${
        active ? "bg-brand text-white shadow" : "text-muted hover:bg-slate-50 hover:text-ink"
      }`}
      onClick={onClick}
    >
      {children}
    </button>
  );
}

function isActiveWorker(worker: Worker) {
  if (worker.status.toLowerCase() === "healthy") return true;
  const heartbeat = Date.parse(worker.last_heartbeat);
  return Number.isFinite(heartbeat) && Date.now() - heartbeat <= 30_000;
}

function ClusterPanel(props: {
  workers: Worker[];
  activeWorkers: Worker[];
  offlineCount: number;
  showOfflineWorkers: boolean;
  onShowOfflineWorkersChange: (show: boolean) => void;
  onPurgeOffline: () => void;
  purging: boolean;
}) {
  const capacity = props.activeWorkers.reduce(
    (total, worker) => ({
      cpu: total.cpu + worker.cpu,
      memory: total.memory + worker.memory_mb,
      gpu: total.gpu + worker.gpu,
    }),
    { cpu: 0, memory: 0, gpu: 0 },
  );
  return (
    <section>
      <div className="mb-4 flex flex-col justify-between gap-4 sm:flex-row sm:items-center">
        <div>
          <h2 className="text-xl font-bold text-ink">Registered workers</h2>
          <p className="mt-1 text-sm text-muted">Active capacity and runner versions.</p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <label className="flex cursor-pointer items-center gap-2 text-sm font-medium text-muted">
            <input
              type="checkbox"
              checked={props.showOfflineWorkers}
              onChange={(event) => props.onShowOfflineWorkersChange(event.target.checked)}
              className="h-4 w-4 accent-blue-600"
            />
            Show Offline Workers ({props.offlineCount})
          </label>
          <button
            type="button"
            disabled={props.purging || props.offlineCount === 0}
            onClick={props.onPurgeOffline}
            className="rounded-lg border border-rose-200 px-3 py-2 text-sm font-semibold text-rose-700 hover:bg-rose-50 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {props.purging ? "Purging..." : "Purge Offline"}
          </button>
          <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-600">Refreshes every 5s</span>
        </div>
      </div>
      <div className="mb-5 grid gap-3 sm:grid-cols-3">
        <Summary label="Active CPU capacity" value={`${capacity.cpu} cores`} />
        <Summary label="Active RAM capacity" value={`${capacity.memory.toLocaleString()} MiB`} />
        <Summary label="Active GPU capacity" value={capacity.gpu} />
      </div>
      {props.workers.length === 0 ? (
        <EmptyState title="No workers registered" detail="Start a worker-agent to see its advertised capacity here." />
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          {props.workers.map((worker) => (
            <article key={worker.id} className="rounded-2xl border border-slate-200 bg-white p-5 shadow-card">
              <div className="mb-5 flex items-start justify-between">
                <div>
                  <h3 className="font-bold text-ink">{worker.id}</h3>
                  <p className="mt-1 text-sm text-muted">Runner {worker.runner_version}</p>
                </div>
                <StatusPill value={worker.status} />
              </div>
              <div className="space-y-4">
                <ResourceBar label="CPU cores" used={worker.used_cpu} total={worker.cpu} />
                <ResourceBar label="Memory" used={worker.used_memory_mb} total={worker.memory_mb} unit="MiB" />
                <ResourceBar label="GPU" used={worker.used_gpu} total={worker.gpu} />
              </div>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function ResourceBar({ label, used, total, unit = "" }: { label: string; used: number; total: number; unit?: string }) {
  const pct = percentage(used, total);
  return (
    <div>
      <div className="mb-1.5 flex justify-between text-xs font-medium">
        <span className="text-muted">{label}</span>
        <span className="text-ink">{used.toLocaleString()} / {total.toLocaleString()} {unit}<span className="ml-2 text-muted">{pct}%</span></span>
      </div>
      <div className="h-2 overflow-hidden rounded-full bg-slate-100">
        <div className={`h-full rounded-full ${pct > 85 ? "bg-amber-500" : "bg-brand"}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function JobsPanel(props: {
  jobs: Job[];
  selectedJob: Job | null;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onOpenJob: (id: string) => void;
  onCloseJob: () => void;
  submitting: boolean;
  tenantId: string;
  setTenantId: (value: string) => void;
  command: string;
  setCommand: (value: string) => void;
  cpu: number;
  setCpu: (value: number) => void;
  memory: number;
  setMemory: (value: number) => void;
  gpu: number;
  setGpu: (value: number) => void;
}) {
  return (
    <section className="grid gap-5 xl:grid-cols-[340px_1fr]">
      <form onSubmit={props.onSubmit} className="h-fit rounded-2xl border border-slate-200 bg-white p-5 shadow-card">
        <h2 className="text-lg font-bold text-ink">Submit a job</h2>
        <p className="mb-5 mt-1 text-sm text-muted">Requests are idempotent and quota checked.</p>
        <div className="space-y-4">
          <label className="field-label">Tenant
            <select className="field-input" value={props.tenantId} onChange={(e) => props.setTenantId(e.target.value)}>
              <option value="tenant-a">Tenant A</option>
              <option value="tenant-b">Tenant B</option>
              <option value="tenant-c">Tenant C</option>
            </select>
          </label>
          <label className="field-label">Command
            <textarea className="field-input min-h-24 resize-y font-mono text-xs" required value={props.command} onChange={(e) => props.setCommand(e.target.value)} />
          </label>
          <div className="grid grid-cols-3 gap-3">
            <NumberField label="CPU" min={1} value={props.cpu} onChange={props.setCpu} />
            <NumberField label="RAM MiB" min={1} value={props.memory} onChange={props.setMemory} />
            <NumberField label="GPU" min={0} value={props.gpu} onChange={props.setGpu} />
          </div>
          <button disabled={props.submitting} className="primary-button w-full">
            {props.submitting ? "Submitting..." : "Submit job"}
          </button>
        </div>
      </form>

      <div className="min-w-0 rounded-2xl border border-slate-200 bg-white shadow-card">
        <div className="border-b border-slate-100 px-5 py-4">
          <h2 className="font-bold text-ink">Recent jobs</h2>
          <p className="mt-1 text-sm text-muted">Select a row to inspect its event history.</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[700px] text-left text-sm">
            <thead className="bg-slate-50 text-xs uppercase tracking-wide text-muted">
              <tr><th className="px-5 py-3">Job</th><th className="px-4 py-3">Tenant</th><th className="px-4 py-3">Resources</th><th className="px-4 py-3">Worker</th><th className="px-4 py-3">Status</th></tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {props.jobs.map((job) => (
                <tr key={job.id} className="cursor-pointer hover:bg-blue-50/50" onClick={() => props.onOpenJob(job.id)}>
                  <td className="max-w-40 truncate px-5 py-3 font-mono text-xs text-ink">{job.id}</td>
                  <td className="px-4 py-3 text-muted">{job.tenant_id}</td>
                  <td className="px-4 py-3 text-muted">{job.req_cpu} CPU / {job.req_memory_mb} MiB / {job.req_gpu} GPU</td>
                  <td className="px-4 py-3 text-muted">{job.worker_id ?? "—"}</td>
                  <td className="px-4 py-3"><StatusPill value={job.status} /></td>
                </tr>
              ))}
              {props.jobs.length === 0 && <tr><td colSpan={5} className="px-5 py-12"><EmptyState title="No recent jobs" detail="Submit a job to begin." /></td></tr>}
            </tbody>
          </table>
        </div>
      </div>
      {props.selectedJob && (
        <JobDrawer job={props.selectedJob} onClose={props.onCloseJob} />
      )}
    </section>
  );
}

function NumberField({ label, min, max, value, onChange }: { label: string; min: number; max?: number; value: number; onChange: (value: number) => void }) {
  return (
    <label className="field-label text-xs">{label}
      <input className="field-input px-2" type="number" min={min} max={max} required value={value}
        onChange={(e) => onChange(Number(e.target.value))} />
    </label>
  );
}

function JobDrawer({ job, onClose }: { job: Job; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-20 flex justify-end bg-slate-900/30" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <aside className="h-full w-full max-w-xl overflow-y-auto bg-white p-6 shadow-2xl" aria-label="Job event timeline">
        <div className="mb-6 flex items-start justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wider text-brand">Execution detail</p>
            <h2 className="mt-1 break-all font-mono text-lg font-bold text-ink">{job.id}</h2>
            <div className="mt-2"><StatusPill value={job.status} /></div>
          </div>
          <button className="rounded-lg px-3 py-2 text-sm font-semibold text-muted hover:bg-slate-100" onClick={onClose}>Close</button>
        </div>
        <div className="rounded-xl bg-slate-50 p-4 text-sm">
          <p><span className="text-muted">Tenant:</span> {job.tenant_id}</p>
          <p className="mt-1"><span className="text-muted">Worker:</span> {job.worker_id ?? "Unassigned"}</p>
          <p className="mt-1"><span className="text-muted">Execution:</span> {job.execution_id}</p>
          {job.trace_id && (
            <a className="mt-3 inline-flex font-semibold text-brand hover:underline" target="_blank" rel="noreferrer"
              href={`http://localhost:16686/trace/${encodeURIComponent(job.trace_id)}`}>
              Open distributed trace in Jaeger
            </a>
          )}
        </div>
        <h3 className="mb-4 mt-7 font-bold text-ink">Event timeline</h3>
        <ol className="space-y-4">
          {(job.events ?? []).map((event) => (
            <li key={`${event.sequence_num}-${event.event_type}`} className="relative border-l-2 border-blue-100 pb-1 pl-5">
              <span className="absolute -left-[5px] top-1 h-2 w-2 rounded-full bg-brand" />
              <div className="flex items-center justify-between gap-3">
                <span className="text-sm font-bold text-ink">{event.event_type}</span>
                <time className="text-xs text-muted">{new Date(event.created_at).toLocaleString()}</time>
              </div>
              <p className="mt-1 text-xs text-muted">Sequence {event.sequence_num}</p>
              <pre className="mt-2 overflow-x-auto rounded-lg bg-slate-50 p-3 text-xs text-slate-600">{JSON.stringify(event.payload, null, 2)}</pre>
            </li>
          ))}
          {(job.events ?? []).length === 0 && <p className="text-sm text-muted">No events recorded.</p>}
        </ol>
      </aside>
    </div>
  );
}

function RolloutPanel(props: {
  rollout: Rollout | null;
  targetVersion: string;
  setTargetVersion: (value: string) => void;
  canaryPercentage: number;
  setCanaryPercentage: (value: number) => void;
  errorThreshold: number;
  setErrorThreshold: (value: number) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onRollback: () => void;
  busy: boolean;
}) {
  return (
    <section className="grid gap-5 lg:grid-cols-[1fr_1.2fr]">
      <form onSubmit={props.onSubmit} className="rounded-2xl border border-slate-200 bg-white p-6 shadow-card">
        <p className="text-sm font-semibold text-brand">Progressive delivery</p>
        <h2 className="mt-1 text-xl font-bold text-ink">Configure canary release</h2>
        <p className="mb-6 mt-2 text-sm text-muted">Traffic is deterministically split by job ID and version-matched workers.</p>
        <div className="space-y-5">
          <label className="field-label">Target runner version
            <input className="field-input" required maxLength={64} value={props.targetVersion}
              onChange={(e) => props.setTargetVersion(e.target.value)} placeholder="v2.0.0-canary" />
          </label>
          <fieldset>
            <legend className="mb-2 text-sm font-semibold text-ink">Canary traffic</legend>
            <div className="grid grid-cols-4 gap-2">
              {[5, 25, 50, 100].map((value) => (
                <button key={value} type="button" onClick={() => props.setCanaryPercentage(value)}
                  className={`rounded-lg border px-3 py-2 text-sm font-semibold ${props.canaryPercentage === value ? "border-brand bg-blue-50 text-brand" : "border-slate-200 text-muted hover:border-slate-300"}`}>
                  {value}%
                </button>
              ))}
            </div>
          </fieldset>
          <NumberField label="Error threshold (%)" min={0} max={100} value={props.errorThreshold} onChange={props.setErrorThreshold} />
          <button disabled={props.busy} className="primary-button w-full">
            {props.busy ? "Applying..." : "Start rollout"}
          </button>
        </div>
      </form>
      <article className="rounded-2xl border border-slate-200 bg-white p-6 shadow-card">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-sm font-semibold text-muted">Current release</p>
            <h2 className="mt-1 text-xl font-bold text-ink">{props.rollout?.target_version ?? "No rollout configured"}</h2>
          </div>
          {props.rollout && <StatusPill value={props.rollout.status} />}
        </div>
        {props.rollout ? (
          <>
            <div className="my-7">
              <div className="mb-2 flex justify-between text-sm"><span className="font-semibold text-ink">Canary traffic</span><span className="font-bold text-brand">{props.rollout.canary_percentage}%</span></div>
              <div className="h-3 overflow-hidden rounded-full bg-slate-100">
                <div className="h-full rounded-full bg-brand transition-all" style={{ width: `${props.rollout.canary_percentage}%` }} />
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <Stat label="Error threshold" value={`${props.rollout.error_threshold_pct}%`} />
              <Stat label="Created" value={new Date(props.rollout.created_at).toLocaleDateString()} />
            </div>
            <button disabled={props.busy || props.rollout.status !== "ACTIVE"} onClick={props.onRollback}
              className="mt-6 w-full rounded-xl border border-rose-200 px-4 py-3 text-sm font-bold text-rose-700 transition hover:bg-rose-50 disabled:cursor-not-allowed disabled:opacity-50">
              Roll back release
            </button>
          </>
        ) : (
          <p className="mt-4 text-sm text-muted">Create a rollout to begin routing selected jobs to a target worker version.</p>
        )}
      </article>
    </section>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return <div className="rounded-xl bg-slate-50 p-4"><p className="text-xs text-muted">{label}</p><p className="mt-1 font-bold text-ink">{value}</p></div>;
}

function StatusPill({ value }: { value: string }) {
  return <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-semibold ${statusColor(value)}`}>{value}</span>;
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return <div className="py-8 text-center"><p className="font-semibold text-ink">{title}</p><p className="mt-1 text-sm text-muted">{detail}</p></div>;
}

export default App;
