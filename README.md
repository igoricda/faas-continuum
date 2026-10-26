# Faas-Continuum

A open-source framework for running serverless functions across a device
continuum (edge, fog and cloud) with a risk-aware scheduler.

The continuum is a set of hosts of different capability, from an
ARM-based set-top box on the edge to a public cloud. Faas-Continuum keeps
one control plane over that set: every node runs the same agent, every
function is a container image with a small watchdog as its entry point,
and a single arbiter decides where each request runs.

The scheduler is the core of the project. Instead of routing to the
"least loaded" node, it learns, per function and per node, a predictive
distribution of the end-to-end latency, and only picks a target when the
predicted upper bound meets the deadline of the request. Everything else
in the repository exists to feed that decision with honest numbers: the
telemetry, the capacity accounting, the token-authenticated node
protocol, the container runtime integration and the persistence that
survives an agent restart.

## Status

Research prototype. It runs, it is tested and it is documented, but it
has not been hardened for production use.

## Architecture

```
                       ┌──────────────────────────┐
                       │   Arbiter  (cmd/arbitro) │   :8090
                       │  risk selector + learner │
                       └────────────┬─────────────┘
            node registration,      │        function invoke
            capacity + telemetry    │        (risk-based)
       ┌───────────────┬────────────┴──────────┬───────────────┐
       │               │                       │               │
┌──────┴──────┐┌───────┴───────┐      ┌────────┴───────┐┌───────┴───────┐
│  Edge node  ││  Fog node     │      │  Fog node      ││  Cloud        │
│ (cmd/agente)││ (cmd/agente)  │      │  (cmd/agente)  ││  connector    │
└──────┬──────┘└───────┬───────┘      └────────┬───────┘└───────┬───────┘
       │               │                       │                │
   containerd      containerd             containerd       Serverless
       │               │                       │           (e.g. Azure
   ┌───┴───┐       ┌───┴───┐               ┌───┴───┐        Functions)
   │ warm  │       │ warm  │               │ warm  │
   │ pool  │       │ pool  │               │ pool  │
   └───────┘       └───────┘               └───────┘
```

### Components

| Component | Package / command | Role |
|---|---|---|
| Arbiter | `cmd/arbitro` | Gateway. Registers nodes, keeps their live telemetry, runs the risk selector, invokes functions, falls back to the cloud. |
| Node agent | `cmd/agente` | Runs on edge and fog. Owns containerd, the instance pool, the local registry and the telemetry. Registers with the Arbiter. |
| Watchdog | `cmd/watchdog-lite` | PID 1 inside every function container. Listens on a Unix socket, proxies to the real process, reports the readinesEOF

### Components

| Component | Command | Role |
|---|---|---|
| Arbiter | `cmd/arbitro` | Gateway. Registers nodes, keeps their live telemetry, runs the risk selector, invokes functions and falls back to the cloud. |
| Node agent | `cmd/agente` | Runs on edge and fog. Owns containerd, the instance pool, the local registry and the telemetry. Registers with the Arbiter. |
| Watchdog | `cmd/watchdog-lite` | Entry point of every function container. Listens on a Unix socket, proxies to the real process and reports readiness. |
| Simulator | `cmd/simulador` | Replays a recorded invocation log against mocked nodes, so the scheduler can be evaluated without the lab hardware. |
| Spikes | `cmd/spike-psi`, `cmd/spike-energia` | Probes for kernel capabilities: Pressure Stall Information and the energy counters. |


### Execution contract

A function container is created with its own, empty network namespace:
no CNI, no host network. The only way to reach the process of the
function is a Unix socket, bind-mounted from a per-instance directory on
the host.

The watchdog is the entry point of the image and honours three
environment variables:

| Variable | Meaning |
|---|---|
| `FPROCESS` | Command to run as the real function process. |
| `UPSTREAM_PORT` | Port of the local process to proxy to. |
| `SOCKET_PATH` | Path of the Unix socket inside the container. |

The watchdog removes and recreates the socket on every start, so it never
inherits a stale one, and answers a readiness request on a dedicated path
so the pool knows when the instance can serve its first request.

### Risk-based selection

The Arbiter keeps one learned model per (function group, node) pair in
`internal/arbitro/learn`:

- a **Normal-Inverse-Gamma** regression of `ln(service time)` against
  `ln(requests in flight)`, for the warm path, plus a concurrency-free
  variant for cold starts;
- a **Beta** reliability counter with exponential decay, so a node that
  has been failing for a while stops looking reliable;
- an **adaptive conformal** calibrator, so the predictive bound it emits
  stays a true `1 - risk` quantile in the long run, without assuming the
  model is correct.

Given a request with a deadline, the selector filters the candidate nodes
and picks the one whose predicted upper bound fits. Concretely
(`internal/arbitro/selection/risk_selector.go`):

- nodes without enough evidence, or below the minimum success
  probability, go to **probation** and are only used when nothing else can
  serve the request;
- every request has an **exploration** chance of being sent to a
  randomised candidate, which is what lets the learner keep improving;
- when no node can meet the deadline, the queue allowance decides whether
  the request waits in the node queue or is rejected as an overload.


### Telemetry

Two mechanisms, both with automatic detection (`internal/telemetry`):

- **usage**: cgroups v2 when its counters are readable, otherwise procfs,
  which covers cgroups v1 and devices without usable cgroups. Always
  reported.
- **pressure**: Pressure Stall Information, only when the kernel exposes
  it. Otherwise the fields are omitted, and the node runs in
  compatibility mode.
- **energy**: the cumulative hardware counter when it is readable with
  privileges, otherwise the instantaneous power of the power supply
  class, otherwise nothing.

The model is hybrid: a compact summary rides along in the response of
every invocation, and a full snapshot is also sent periodically, so a
window without invocations still produces data.

### Authentication

- Management and operator endpoints use a password credential derived once
  with a key-derivation function, printed once at bootstrap.
- Node-to-arbiter calls use per-node bearer tokens, hashed at rest.
- Function invocations use short-lived per-function tokens on the hot
  path, so the checks are cheap.
- Every route family has its own per-IP rate limit and body-size cap.

