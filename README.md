# Faas-Continuum

An open-source framework for running serverless functions across a device
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
│ (cmd/agente)││ (cmd/agente)  │      │ (cmd/agente)   ││  connector    │
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

| Component | Command | Role |
|---|---|---|
| Arbiter | `cmd/arbitro` | Gateway. Registers nodes, keeps their live telemetry, runs the risk selector, invokes functions and falls back to the cloud. |
| Node agent | `cmd/agente` | Runs on edge and fog. Owns containerd, the instance pool, the local registry and the telemetry. Registers with the arbiter. |
| Watchdog | `cmd/watchdog-lite` | Entry point of every function container. Listens on a Unix socket, proxies to the real process and reports readiness. |
| Simulator | `cmd/simulador` | Replays a recorded invocation log against mocked nodes, so the scheduler can be evaluated without any hardware. |
| Diagnostics | `cmd/spike-psi`, `cmd/spike-energia` | Probe for kernel capabilities: Pressure Stall Information and the energy counters. |
| Run analysis | `tools/analyze-runs` | Summarises the invocation logs produced by the arbiter. |

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
so the pool knows when the instance can serve its first request. The
readiness route is `GET /_/ready`: it answers `200` when the instance is
ready and `503` while it is not, and `POST` on the same path always
answers `404`. The socket is only opened after the first `200`, so no
request ever reaches an instance that is still loading.

The readiness poll is configured with three more variables:

| Variable | Default | Meaning |
|---|---|---|
| `READY_PATH` | `/_/ready` | The route that is polled. |
| `READY_TIMEOUT` | `90s` | The ceiling of the poll, in the Go duration syntax. |
| `READY_INTERVAL` | `250ms` | The interval between the polls. |

The function process is started exactly once and stays resident between
invocations, which is what makes the warm path cheap: the model, the
runtime and the weights are loaded a single time per instance.

### Risk-based selection

The arbiter keeps one learned model per (function group, node) pair in
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

The `QueueSelector` remains the deterministic baseline: it applies the
hard limits alone, with no learning, and it is the reference against
which the risk selector is compared.

The default parameters of the selector are a half life of 10 minutes, a
risk of 0.05, a conformal gamma of 0.01, a minimum of 3 samples, an
exploration probability of 0.05, a stretch of 2.0, a tie band of 0.10, a
queue allowance of 1.0 and a minimum success probability of 0.5. Every
one of them is overridable, as shown in `config.example.jsonc`.

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

The limits are, per IP and per minute: 600 for the node registration and
the telemetry, 600 for the invocations, 600 for the late reports and 300
for the management routes. The body of an invocation is capped at 8 MiB
and the body of a management route at 64 KiB.

## Build and test

The repository is a single Go module and needs Go 1.22 or newer.

```bash
# resolve the dependencies and run the build, the vet and the tests
./bootstrap.sh

# or the individual steps
go build ./...
go vet ./...
go test ./...
```

The `bootstrap.sh` script also cross-compiles the watchdog and the
diagnostics for `linux/arm64`, which is what the function images of the
ARM hosts need.

The `Makefile` offers the same steps as targets (`build`, `test`, `vet`,
`tidy`) plus the stripped and the unstripped arm64 cross builds
(`build-arm64` and `build-arm64-raw`).

### Running the two processes

```bash
# the arbiter, the central gateway
go run ./cmd/arbitro -listen :8090

# the node agent, on a host that has containerd running
go run ./cmd/agente -listen :8080 -arbitro-url http://<arbiter>:8090
```

The arbiter prints the administrative credential once at startup, and the
node agent prints its node token once, on the first registration.

### Offline evaluation

The simulator replays a recorded invocation log against mocked nodes, so
the scheduler can be evaluated without any hardware. The gateway, the
selectors, the history and the monitor are the production code; only the
node side of the wire is mocked.

```bash
go run ./cmd/simulador -invlog invocation.jsonl -intervals 5,2 -reps 1 -burst 20s -out results
```

The `-frio` flag starts with empty node pools, so the first request pays
a real cold start, `-cloudfrio` adds a cold cloud tail, and `-prime`
warms the history before the measured stage.

## Configuration

Every setting is resolved with the same precedence:

```
command line flags > FAAS_* environment variables > config.jsonc > built-in defaults
```

The built-in defaults live in `Defaults()` in `internal/config/config.go`.
`config.example.jsonc` is a commented copy of the whole configuration
surface, with the flag and the environment variable of each field and its
default value. To use it, copy it to `config.jsonc` and point the process
at it with the `-config` flag; the file accepts the `//` and the `/* */`
comments.

The configuration is grouped in seven sections:

| Section | Governs |
|---|---|
| `runtime` | The connection to the containerd: socket, namespace and the root of the per-instance sockets. |
| `registry` | The persistence of the function definitions and the allowlist of the registry hosts. |
| `telemetry` | The collection of the usage, the pressure and the energy, and the reporting periods. |
| `nodeapi` | The HTTP API of the node: the address, the identity, the timeouts, the body ceilings and the late-report queue. |
| `scaling` | The autoscaling defaults, in particular the idle time before scaling down. |
| `arbitro` | The central gateway: the registry of the nodes, the history, the selector and its parameters, the cloud fallback and the hard limits. |

## Repository layout

```
cmd/
  arbitro/         the central gateway and the risk selector
  agente/          the node agent: runtime, pool, registry, telemetry
  simulador/       the offline replay of a recorded invocation log
  watchdog-lite/   the entry point of every function container
  spike-psi/       the diagnostic for the Pressure Stall Information
  spike-energia/   the diagnostic for the energy counters
internal/
  arbitro/         the gateway, the nodes, the monitor, the history,
                   the selection, the learner and the cloud connector
  auth/            the Argon2id credential, the tokens and the limiters
  config/          the loading, the validation and the bindings
  containerruntime/ the containerd integration
  fsutil/          the durable writes
  imageref/        the parsing of the OCI references
  nodeapi/         the HTTP API of the node
  ntime/           the time helpers
  pool/            the instance pool and the scaling
  reconcile/       the startup reconciliation of the surviving containers
  registry/        the function registry and its persistence
  scaler/          the scaling decisions
  telemetry/       the collector, the reporter and the kernel counters
examples/          the function examples, with their images and their READMEs
tests/e2e/         the end-to-end tests
tools/analyze-runs/ the analysis of the invocation logs
```

## Examples

The `examples/` directory holds complete, runnable functions. Each one has
its own README with the model, the measured numbers and the registration
commands.

| Example | Backend | Target host |
|---|---|---|
| [`hello-world`](examples/hello-world) | none, a pure string function | any, for a smoke test of the whole chain |
| [`sleep`](examples/sleep) | none, a controlled delay | any, for the timeouts and the scaling |
| [`alpr`](examples/alpr) | torch with CUDA and the ultralytics runtime | an amd64 server with a GPU |
| [`alpr-ncnn`](examples/alpr-ncnn) | ncnn through the ultralytics runtime | an ARM edge host |
| [`alpr-ncnn-slim`](examples/alpr-ncnn-slim) | ncnn alone | an ARM edge host |
| [`alpr-onnx-slim`](examples/alpr-onnx-slim) | the ONNX runtime | an x86 host |
| [`ocr`](examples/ocr) | the ONNX runtime | amd64 and arm64 |
| [`yolo26n-coco`](examples/yolo26n-coco) | torch with the ultralytics runtime | an amd64 server |
| [`yolo26n-coco-ncnn-slim`](examples/yolo26n-coco-ncnn-slim) | ncnn alone | an ARM edge host |
| [`yolo26n-coco-onnx-slim`](examples/yolo26n-coco-onnx-slim) | the ONNX runtime | an x86 host |

All of them follow the same execution contract: the container entry point
is the watchdog, the function process is started exactly once, the
readiness route is `GET /_/ready` and the invocations arrive over a Unix
socket.

The watchdog binary is built outside the image, for the architecture of
the target host, and is ignored by Git. **Build it before the image**:

```bash
GOOS=linux GOARCH=amd64 go build -o examples/hello-world/watchdog-lite ./cmd/watchdog-lite
docker build -t faas-continuum/hello-world:latest \
  -f examples/hello-world/Dockerfile examples/hello-world
```

The model weights of the license plate and of the text recognition
examples are published with the repository, so those examples build
without any extra download.

## References

The statistical and scheduling foundations of the selector are:

- M. Mitzenmacher, *How Useful is Old Information?*, IEEE Transactions on
  Parallel and Distributed Systems, 2000.
- M. Mitzenmacher, K. Richa, R. Sitaraman, *The Power of Two Random
  Choices: A Survey of Techniques and Results*, 2001.
- M. Dahlin, *Interpreting Stale Load Information*, Proceedings of the
  19th IEEE ICDCS, 1999.
- K. Ousterhout et al., *Sparrow: Distributed, Low Latency Scheduling*,
  SOSP 2013.
- I. Gibbs and E. Candès, *Adaptive Conformal Inference Under
  Distribution Shift*, NeurIPS 2021.
- A. Garivier and E. Moulines, *On Upper-Confidence Bound Policies for
  Non-Stationary Bandit Problems*, ALT 2011.
- Y. Wu, R. Shariff, T. Lattimore and C. Szepesvári, *Conservative
  Bandits*, ICML 2016.
- *Latency-Aware, Predictive In-Memory Routing*, arXiv 2505.07417, for
  the structure of predicting, filtering the viable candidates and
  ordering them by cost.
- *QUARTZ: Quantile-Aware Routing and Queueing for TTFT SLOs in LLM
  Serving*, Findings of ACL 2026, for the conservative quantiles and the
  queue occupancy signals.
- K. Murphy, *Conjugate Bayesian Analysis of the Gaussian Distribution*,
  University of British Columbia technical report, for the
  Normal-Inverse-Gamma posterior and the Student-t predictive.
- M. Abramowitz and I. Stegun, *Handbook of Mathematical Functions*,
  NBS AMS 55, for the expansion of the Student-t quantile.
- M. Harchol-Balter, *Performance Modeling and Design of Computer
  Systems: Queueing Theory in Action*, Cambridge University Press, 2013.
- J. Dean and L. Barroso, *The Tail at Scale*, Communications of the ACM,
  2013.
- N. Bronson et al., *Metastable Failures in Distributed Systems*, HotOS
  2021, and L. Huang et al., *Metastable Failures in the Wild*, OSDI
  2022.

The LPR models were trained using the Rodosol ALPR dataset:
- R. Laroca, E. V. Cardoso, D. R. Lucio, V. Estevam, and D. Menotti, ''On the Cross-dataset Generalization in License Plate Recognition'' in International Conference on Computer Vision Theory and Applications (VISAPP), pp. 166-178, Feb 2022.

The OCR models were based on https://github.com/ankandrew/fast-plate-ocr and finetuned with the Rodosol dataset.

The foundations of the container runtime and of the interfaces are the
OCI image specification, the containerd API and the gRPC HTTP/2 protocol.

## Publication

This repository is the public copy of the project. The companion
material, the design records and the measurement reports are kept in the
private repository of the author and are not part of this publication.

The module path is `github.com/igoricda/faas-continuum`, and the public
remote is `git@github.com:igoricda/faas-continuum.git`. The license of the
project has not been stated yet; the terms of the repository apply until
one is added.
