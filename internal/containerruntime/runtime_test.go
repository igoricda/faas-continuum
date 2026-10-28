package containerruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	statsv1 "github.com/containerd/cgroups/v3/cgroup1/stats"
	statsv2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	"github.com/containerd/containerd"
	ctrdapi "github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/namespaces"
	typeurl "github.com/containerd/typeurl/v2"
	ocispec "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/igoricda/faas-continuum/internal/imageref"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// synthAny produces the payload that the container daemon really carries
// in the data field of a metric: an envelope with the type URL of the
// protobuf type and the serialized value, built offline and without a
// daemon, with the same encoding as the real path of the task metrics.
func synthAny(t *testing.T, msg interface{}) *anypb.Any {
	t.Helper()
	any, err := typeurl.MarshalAny(msg)
	if err != nil {
		t.Fatalf("typeurl.MarshalAny(%T) erro inesperado: %v", msg, err)
	}
	return &anypb.Any{TypeUrl: any.GetTypeUrl(), Value: any.GetValue()}
}

// metricsCapturingTask is a fake of the task that captures the context
// received by the metrics call. The real daemon requires the namespace in
// that context and rejects the call when it is missing, while the fake
// does not validate it, so the test below reads the namespace key of the
// context to prove that the wrapper of the runtime is present.
type metricsCapturingTask struct {
	containerd.Task
	gotCtx   context.Context
	metadata *ctrdapi.Metric
}

// Metrics captures the context and returns the metric prepared by the
// test, whose payload is a zeroed cgroup v2 sample, which is a valid
// partial sample for the telemetry.
func (f *metricsCapturingTask) Metrics(ctx context.Context) (*ctrdapi.Metric, error) {
	f.gotCtx = ctx
	return f.metadata, nil
}

// ---------------------------------------------------------------------------
// Tests of the image resolution
// ---------------------------------------------------------------------------

// The normalization lives in its own package, and these cases pin the
// parity of the behavior of the pull, which reuses it unchanged.
func TestNormalizeImageRef(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "usuário/imagem sem tag nem registry (o caso que quebrou em produção)",
			input: "igoricda/alpr-yolo:latest",
			want:  "docker.io/igoricda/alpr-yolo:latest",
		},
		{
			name:  "usuário/imagem sem tag nenhuma",
			input: "igoricda/alpr-yolo",
			want:  "docker.io/igoricda/alpr-yolo:latest",
		},
		{
			name:  "imagem oficial de um segmento só",
			input: "python",
			want:  "docker.io/library/python:latest",
		},
		{
			name:  "registry já explícito, não deve mexer no host",
			input: "registry.local/alpr:1.0",
			want:  "registry.local/alpr:1.0",
		},
		{
			name:  "docker.io já explícito",
			input: "docker.io/igoricda/alpr-yolo:latest",
			want:  "docker.io/igoricda/alpr-yolo:latest",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := imageref.NormalizeImageRef(tc.input)
			if err != nil {
				t.Fatalf("NormalizeImageRef(%q) erro inesperado: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("NormalizeImageRef(%q) = %q, esperado %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestNormalizeImageRefRejectsInvalidReference(t *testing.T) {
	_, err := imageref.NormalizeImageRef("::::nao-e-uma-referencia-valida")
	if err == nil {
		t.Fatal("esperava erro para referência de imagem inválida")
	}
}

func TestPrepareSocketDirIsWorldWritableRegardlessOfUmask(t *testing.T) {
	// It reproduces the real scenario: an agent running with a
	// restrictive mask of permissions, which is common when it runs as
	// root through a shell, must not leave the socket directory without
	// write permission for the unprivileged user of the container.
	oldUmask := syscall.Umask(0o022)
	defer syscall.Umask(oldUmask)

	dir := filepath.Join(t.TempDir(), "instancia-0")
	if err := prepareSocketDir(dir); err != nil {
		t.Fatalf("prepareSocketDir() erro inesperado: %v", err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}

	got := info.Mode().Perm()
	want := os.FileMode(0o777)
	if got != want {
		t.Errorf("permissão do diretório = %o, esperado %o (mesmo com umask 022)", got, want)
	}
}

func TestPrepareSocketDirIsIdempotent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instancia-0")

	if err := prepareSocketDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := prepareSocketDir(dir); err != nil {
		t.Fatalf("2ª chamada deveria ser idempotente, erro: %v", err)
	}
}

// TestTableTestLoadRunningHandle verifies the mapping of the sentinel
// error for the loading of a running handle: a container with no task
// returns the not-running error,
func TestTableTestLoadRunningHandle(t *testing.T) {
	if ErrNotRunning == nil {
		t.Fatal("ErrNotRunning should not be nil")
	}
	if ErrNotRunning.Error() != "containerruntime: not running" {
		t.Errorf("ErrNotRunning.Error() = %q, want %q", ErrNotRunning.Error(), "containerruntime: not running")
	}
}

// ---------------------------------------------------------------------------
// Tests of the provisioning options
// ---------------------------------------------------------------------------

// TestProvisionCreateOptionsSemEnv pins that the creation of the container
// of a function never injects environment variables: the function
// inherits only the default environment of the image, so the secrets of
// the agent never leak into it.
func TestProvisionCreateOptionsSemEnv(t *testing.T) {
	opts := provisionCreateOptions("f-0", "/run/faas-continuum/sockets/f-0", false)
	if opts.ID != "f-0" {
		t.Errorf("ID = %q, esperado f-0", opts.ID)
	}
	if opts.Env != nil {
		t.Errorf("Env = %v, esperado nil (nenhum segredo do agente na função)", opts.Env)
	}
	if opts.GPU {
		t.Errorf("GPU = true, esperado false por default (CPU-only)")
	}
	if len(opts.Mounts) != 1 {
		t.Fatalf("Mounts tem %d entradas, esperado 1 (socket Unix)", len(opts.Mounts))
	}
	m := opts.Mounts[0]
	if m.Source != "/run/faas-continuum/sockets/f-0" || m.Destination != containerSocketDir {
		t.Errorf("mount = %+v, esperado bind do dir do socket", m)
	}
}

// TestProvisionCreateOptionsGPU pins that the device flag crosses the pure
// helper up to the options of creation. The prestart hook itself is
// pinned below, without a daemon, by applying it to an empty
// specification.
func TestProvisionCreateOptionsGPU(t *testing.T) {
	opts := provisionCreateOptions("f-gpu-0", "/run/faas-continuum/sockets/f-gpu-0", true)
	if !opts.GPU {
		t.Fatalf("GPU = false, esperado true (opt-in da função)")
	}
	if opts.Env != nil {
		t.Errorf("Env = %v, esperado nil (seletores NVIDIA entram no Create, não aqui)", opts.Env)
	}
}

// TestWithNvidiaHook applies the hook to an empty specification and checks
// the prestart hook, its path and its arguments, without a container
// daemon.
func TestWithNvidiaHook(t *testing.T) {
	var s ocispec.Spec
	opt := withNvidiaHook()
	if err := opt(context.Background(), nil, nil, &s); err != nil {
		t.Fatalf("withNvidiaHook devolveu erro inesperado: %v", err)
	}
	if s.Hooks == nil || len(s.Hooks.Prestart) != 1 {
		t.Fatalf("Hooks.Prestart = %+v, esperada exatamente 1 entrada", s.Hooks)
	}
	h := s.Hooks.Prestart[0]
	if h.Path != nvidiaHookPath {
		t.Errorf("hook Path = %q, esperado %q", h.Path, nvidiaHookPath)
	}
	if len(h.Args) != 2 || h.Args[1] != "prestart" {
		t.Errorf("hook Args = %q, esperado [hook prestart]", h.Args)
	}
}

// ---------------------------------------------------------------------------
// Tests of the metric samples
// ---------------------------------------------------------------------------

// TestSampleMetricsDecodeCgroupV1 proves the conversion of the format of
// cgroup v1, whose accumulated CPU counter is in nanoseconds, and the
// reading of the memory in bytes.
func TestSampleMetricsDecodeCgroupV1(t *testing.T) {
	payload := synthAny(t, &statsv1.Metrics{
		CPU: &statsv1.CPUStat{
			Usage: &statsv1.CPUUsage{Total: 12_345_678_901}, // ns
		},
		Memory: &statsv1.MemoryStat{
			Usage: &statsv1.MemoryEntry{Usage: 24_117_248}, // bytes
		},
	})

	sample, err := decodeInvocationSample(payload)
	if err != nil {
		t.Fatalf("decodeInvocationSample() erro inesperado: %v", err)
	}
	if sample.CPUUsageUSec != 12_345_678 { // 12.345.678.901 ns / 1000
		t.Errorf("CPUUsageUSec = %d, esperado 12345678 (ns→µs)", sample.CPUUsageUSec)
	}
	if sample.MemoryBytes != 24_117_248 {
		t.Errorf("MemoryBytes = %d, esperado 24117248", sample.MemoryBytes)
	}
}

// TestSampleMetricsDecodeCgroupV2 proves that the format of cgroup v2
// passes through unchanged, since its counter is already in
// microseconds and must not be divided.
func TestSampleMetricsDecodeCgroupV2(t *testing.T) {
	payload := synthAny(t, &statsv2.Metrics{
		CPU:    &statsv2.CPUStat{UsageUsec: 987_654},
		Memory: &statsv2.MemoryStat{Usage: 51_380_224},
	})

	sample, err := decodeInvocationSample(payload)
	if err != nil {
		t.Fatalf("decodeInvocationSample() erro inesperado: %v", err)
	}
	if sample.CPUUsageUSec != 987_654 {
		t.Errorf("CPUUsageUSec = %d, esperado 987654 (µs sem conversão)", sample.CPUUsageUSec)
	}
	if sample.MemoryBytes != 51_380_224 {
		t.Errorf("MemoryBytes = %d, esperado 51380224", sample.MemoryBytes)
	}
}

// TestSampleMetricsDecodeNilInnerPointers proves that a partial payload,
// with absent components, returns zeroed fields instead of panicking: an
// incomplete sample is never a fatal error for the telemetry.
func TestSampleMetricsDecodeNilInnerPointers(t *testing.T) {
	cases := []struct {
		name    string
		payload *anypb.Any
	}{
		{
			name:    "cgroups v1 sem nenhum componente",
			payload: synthAny(t, &statsv1.Metrics{}),
		},
		{
			name:    "cgroups v1 só com CPU (memória nil)",
			payload: synthAny(t, &statsv1.Metrics{CPU: &statsv1.CPUStat{Usage: &statsv1.CPUUsage{Total: 2_000_000_000}}}),
		},
		{
			name:    "cgroups v2 sem nenhum componente",
			payload: synthAny(t, &statsv2.Metrics{}),
		},
		{
			name:    "cgroups v2 só com memória (CPU nil)",
			payload: synthAny(t, &statsv2.Metrics{Memory: &statsv2.MemoryStat{Usage: 4096}}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sample, err := decodeInvocationSample(tc.payload)
			if err != nil {
				t.Fatalf("decodeInvocationSample() erro inesperado: %v", err)
			}
			switch tc.name {
			case "cgroups v1 sem nenhum componente", "cgroups v2 sem nenhum componente":
				if sample != (telemetry.InvocationSample{}) {
					t.Errorf("amostra = %+v, esperada zerada", sample)
				}
			case "cgroups v1 só com CPU (memória nil)":
				if sample.CPUUsageUSec != 2_000_000 || sample.MemoryBytes != 0 {
					t.Errorf("amostra = %+v, esperada CPU=2000000 µs e memória 0", sample)
				}
			case "cgroups v2 só com memória (CPU nil)":
				if sample.CPUUsageUSec != 0 || sample.MemoryBytes != 4096 {
					t.Errorf("amostra = %+v, esperada CPU 0 e memória 4096 bytes", sample)
				}
			}
		})
	}
}

// TestSampleMetricsDecodeUnknownType proves that a payload with a type
// URL outside the two known formats becomes an explicit error, and not a
// silently zeroed sample that would contaminate the profile with false
// windows of zero percent.
func TestSampleMetricsDecodeUnknownType(t *testing.T) {
	payload := &anypb.Any{
		TypeUrl: "type.googleapis.com/com.example.tipo.inexistente",
		Value:   []byte{0xff, 0x01},
	}

	sample, err := decodeInvocationSample(payload)
	if err == nil {
		t.Fatal("esperava erro para formato de métrica desconhecido")
	}
	if !strings.Contains(err.Error(), "formato de métrica não suportado") {
		t.Errorf("erro = %q, esperado contendo \"formato de métrica não suportado\"", err.Error())
	}
	if sample != (telemetry.InvocationSample{}) {
		t.Errorf("amostra = %+v, esperada zerada no erro", sample)
	}
}

// TestSampleMetricsInstanceNotFound proves the public path of the metric
// sampling for an instance outside the pool, including a freshly created
// pool, without a daemon.
func TestSampleMetricsInstanceNotFound(t *testing.T) {
	adapter := NewPoolAdapterWithSocketRoot(nil, t.TempDir())

	sample, err := adapter.SampleMetrics(context.Background(), "instancia-inexistente")
	if err == nil {
		t.Fatal("esperava erro para instância inexistente no pool")
	}
	if !strings.Contains(err.Error(), `instância "instancia-inexistente" não encontrada`) {
		t.Errorf("erro = %q, esperado contendo instância não encontrada", err.Error())
	}
	if sample != (telemetry.InvocationSample{}) {
		t.Errorf("amostra = %+v, esperada zerada no erro", sample)
	}
}

// TestSampleMetricsContextHasNamespace proves that the sampling hands a
// context with the namespace to the task, which is a regression guard
// against the removal of that wrapper.
//
// The real container daemon rejects every call without the namespace in
// the context with a precondition error, while an in-memory fake does not
// enforce it, so the test forces the namespace to be present in the
// context received by the fake.
func TestSampleMetricsContextHasNamespace(t *testing.T) {
	payload := synthAny(t, &statsv2.Metrics{})
	ft := &metricsCapturingTask{
		metadata: &ctrdapi.Metric{Data: payload},
	}

	rt := &Runtime{namespace: DefaultNamespace}
	a := &PoolAdapter{
		runtime:        rt,
		hostSocketRoot: t.TempDir(),
		handles:        map[string]*Handle{"inst": {Task: ft}},
	}

	sample, err := a.SampleMetrics(context.Background(), "inst")
	if err != nil {
		t.Fatalf("SampleMetrics() erro inesperado: %v", err)
	}

	// It checks that the context received by the fake carries the
	// namespace of the runtime.
	ns, ok := namespaces.Namespace(ft.gotCtx)
	if !ok {
		t.Fatal("namespace não encontrado no contexto recebido por Task.Metrics —" +
			" wrap de Runtime.ctx removido? (bug de QA ao vivo: 'namespace is required')")
	}
	if ns != DefaultNamespace {
		t.Errorf("namespace = %q, esperado %q", ns, DefaultNamespace)
	}

	// A zeroed sample is valid, since the payload of the fake is empty.
	if sample != (telemetry.InvocationSample{}) {
		t.Errorf("amostra = %+v, esperada zerada (fake não produz métricas)", sample)
	}
}
