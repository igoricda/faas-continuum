package reconcile

import (
	"bytes"
	"context"
	"errors"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/igoricda/faas-continuum/internal/containerruntime"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// ---------------------------------------------------------------------------
// Helpers: fake runtime
// ---------------------------------------------------------------------------

// fakeRuntime simulates the container runtime through the pool adapter, so
// no real daemon is needed.
type fakeRuntime struct {
	mu          sync.Mutex
	running     map[string]bool // the instance identifier maps to a running task
	removed     []string
	adopted     []string
	listErr     error
	failInspect map[string]bool // instanceID -> AdoptExisting must fail
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{running: make(map[string]bool), failInspect: make(map[string]bool)}
}

func (f *fakeRuntime) ListInstanceIDs(context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.running))
	for id := range f.running {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (f *fakeRuntime) AdoptExisting(_ context.Context, instanceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failInspect[instanceID] {
		return errors.New("falha simulada ao inspecionar contêiner")
	}
	running, exists := f.running[instanceID]
	if !exists {
		return errors.New("contêiner não encontrado")
	}
	if running {
		f.adopted = append(f.adopted, instanceID)
	}
	if !running {
		return containerruntime.ErrNotRunning
	}
	return nil
}

func (f *fakeRuntime) StopAndRemoveExisting(_ context.Context, instanceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, instanceID)
	delete(f.running, instanceID)
	return nil
}

// ---------------------------------------------------------------------------
// Helpers: fake provisioner
// ---------------------------------------------------------------------------

// fakeProvisioner follows the same pattern as the pool tests.
type fakeProvisioner struct {
	mu          sync.Mutex
	provisioned []string
	failAll     bool
}

func (f *fakeProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAll {
		return errors.New("falha simulada de provisionamento")
	}
	f.provisioned = append(f.provisioned, instanceID)
	return nil
}

func (f *fakeProvisioner) Terminate(context.Context, string) error { return nil }

// ---------------------------------------------------------------------------
// Helpers: scale recording provisioner
// ---------------------------------------------------------------------------

// recordingProvisioner is used by the scale-down reconciliation tests and
// mirrors the recording provisioner of the pool package.
type scaleRecordingProvisioner struct {
	mu          sync.Mutex
	provisioned []string
	terminated  []string
	failOn      string
}

func (r *scaleRecordingProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID == r.failOn {
		return errors.New("fail provision")
	}
	r.provisioned = append(r.provisioned, instanceID)
	return nil
}

func (r *scaleRecordingProvisioner) Terminate(_ context.Context, instanceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminated = append(r.terminated, instanceID)
	for i, id := range r.provisioned {
		if id == instanceID {
			r.provisioned = append(r.provisioned[:i], r.provisioned[i+1:]...)
			break
		}
	}
	return nil
}

func (r *scaleRecordingProvisioner) terminatedCopy() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.terminated))
	copy(out, r.terminated)
	return out
}

// ---------------------------------------------------------------------------
// Helpers: pool that fails to scale down
// ---------------------------------------------------------------------------

// fakePoolWithError injects an error on scale down and delegates every
// other operation to a real pool.
type fakePoolWithError struct {
	inner          *pool.Manager
	scaleDownCalls []struct {
		fn  string
		min int
	}
	scaleDownErr error
	mu           sync.Mutex
}

func (f *fakePoolWithError) Adopt(functionName, instanceID string) {
	f.inner.Adopt(functionName, instanceID)
}

func (f *fakePoolWithError) EnsureMinInstances(ctx context.Context, functionName, imageRef string, minInstances int) error {
	return f.inner.EnsureMinInstances(ctx, functionName, imageRef, minInstances)
}

func (f *fakePoolWithError) ScaleDownToMin(ctx context.Context, functionName string, min int) (int, error) {
	f.mu.Lock()
	f.scaleDownCalls = append(f.scaleDownCalls, struct {
		fn  string
		min int
	}{functionName, min})
	f.mu.Unlock()
	if f.scaleDownErr != nil {
		return 0, f.scaleDownErr
	}
	return f.inner.ScaleDownToMin(ctx, functionName, min)
}

func (f *fakePoolWithError) scaleDownCallsCopy() []struct {
	fn  string
	min int
} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]struct {
		fn  string
		min int
	}, len(f.scaleDownCalls))
	copy(out, f.scaleDownCalls)
	return out
}

// ---------------------------------------------------------------------------
// Helpers: log capture
// ---------------------------------------------------------------------------

// capturarLog redirects the log output of the package to a buffer and
// restores the original destination at the end of the test. It is
// sequential by design: the tests that capture the log do not run in
// parallel.
func capturarLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	anterior := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(anterior) })
	return &buf
}

// ===========================================================================
// Reconciliation of the registered functions
// ===========================================================================

func TestRunAdoptsHealthyRunningContainer(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["alpr-yolo-0"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "alpr-yolo", "img:1.0", registry.RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}

	fp := &fakeProvisioner{}
	pl := pool.NewManagerWithQueueCap(fp, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}

	if got := pl.Count("alpr-yolo"); got != 1 {
		t.Errorf("Count() = %d, esperado 1 (a instância existente foi adotada)", got)
	}
	if got := len(fp.provisioned); got != 0 {
		t.Errorf("Provision() foi chamado %d vez(es); a instância adotada não deveria ser reprovisionada", got)
	}
	if len(rt.adopted) != 1 || rt.adopted[0] != "alpr-yolo-0" {
		t.Errorf("adopted = %v, esperado [alpr-yolo-0]", rt.adopted)
	}
}

func TestRunRemovesOrphanContainer(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["funcao-removida-0"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(&fakeProvisioner{}, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}

	if len(rt.removed) != 1 || rt.removed[0] != "funcao-removida-0" {
		t.Errorf("removed = %v, esperado [funcao-removida-0]", rt.removed)
	}
	if got := pl.Count("funcao-removida"); got != 0 {
		t.Errorf("Count() = %d, esperado 0 (órfão não deveria ser adotado)", got)
	}
}

func TestRunRemovesNonRunningContainer(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["alpr-yolo-0"] = false

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "alpr-yolo", "img:1.0", registry.RegisterOptions{MinInstances: 0}); err != nil {
		t.Fatal(err)
	}
	pl := pool.NewManagerWithQueueCap(&fakeProvisioner{}, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}

	if len(rt.removed) != 1 || rt.removed[0] != "alpr-yolo-0" {
		t.Errorf("removed = %v, esperado [alpr-yolo-0]", rt.removed)
	}
	if got := pl.Count("alpr-yolo"); got != 0 {
		t.Errorf("Count() = %d, esperado 0 (contêiner não saudável não deveria ser adotado)", got)
	}
}

func TestRunToppsUpDeficitAfterAdoption(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["alpr-yolo-0"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "alpr-yolo", "img:1.0", registry.RegisterOptions{MinInstances: 3}); err != nil {
		t.Fatal(err)
	}
	fp := &fakeProvisioner{}
	pl := pool.NewManagerWithQueueCap(fp, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}

	if got := pl.Count("alpr-yolo"); got != 3 {
		t.Errorf("Count() = %d, esperado 3 (1 adotada + 2 provisionadas para completar min_instances)", got)
	}
	if got := len(fp.provisioned); got != 2 {
		t.Errorf("Provision() foi chamado %d vez(es), esperado 2 (só o déficit, não as 3)", got)
	}
}

func TestRunMarksFunctionDegradedWhenProvisioningFails(t *testing.T) {
	rt := newFakeRuntime()

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "func-quebrada", "imagem-invalida:1.0", registry.RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	fp := &fakeProvisioner{failAll: true}
	pl := pool.NewManagerWithQueueCap(fp, nil)

	err := Run(context.Background(), rt, reg, pl)
	if err == nil {
		t.Fatal("esperava erro agregado quando o provisionamento falha")
	}

	fn, _ := reg.Get("func-quebrada")
	if !fn.Degraded() {
		t.Error("função deveria estar marcada como Degraded após falha de reconciliação")
	}
	if fn.LastError == "" {
		t.Error("LastError deveria estar preenchido")
	}
}

func TestRunClearsDegradedOnSuccessfulRestore(t *testing.T) {
	rt := newFakeRuntime()

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "alpr-yolo", "img:1.0", registry.RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	if err := reg.MarkDegraded("alpr-yolo", "falha de uma reconciliação anterior"); err != nil {
		t.Fatal(err)
	}

	pl := pool.NewManagerWithQueueCap(&fakeProvisioner{}, nil)
	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}

	fn, _ := reg.Get("alpr-yolo")
	if fn.Degraded() {
		t.Error("Degraded deveria ter sido limpo após restaurar min_instances com sucesso")
	}
}

func TestRunErrNotRunningTakesRemovePath(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["func-sem-task-0"] = false

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "func-sem-task", "img:1.0", registry.RegisterOptions{MinInstances: 0}); err != nil {
		t.Fatal(err)
	}
	pl := pool.NewManagerWithQueueCap(&fakeProvisioner{}, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}

	if got := len(rt.removed); got != 1 {
		t.Errorf("removed = %d, esperado 1", got)
	}
	if got := rt.removed[0]; got != "func-sem-task-0" {
		t.Errorf("removed = %q, esperado func-sem-task-0", got)
	}
	if got := pl.Count("func-sem-task"); got != 0 {
		t.Errorf("Count() = %d, esperado 0", got)
	}
}

func TestRunIgnoresContainerWithUnparsableID(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["nome-sem-indice"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(&fakeProvisioner{}, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}

	if len(rt.removed) != 0 {
		t.Errorf("removed = %v, esperado vazio", rt.removed)
	}
	if len(rt.adopted) != 0 {
		t.Errorf("adopted = %v, esperado vazio", rt.adopted)
	}
}

// ===========================================================================
// Reconciliation with scale down
// ===========================================================================

func TestRestoreScalesDownExcess(t *testing.T) {
	rt := newFakeRuntime()
	for i := 0; i < 4; i++ {
		id := "myfn-" + string(rune('0'+i))
		rt.running[id] = true
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "myfn", "img:1.0", registry.RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	fp := &scaleRecordingProvisioner{}
	pl := pool.NewManagerWithQueueCap(fp, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if got := pl.Count("myfn"); got != 1 {
		t.Fatalf("Count() = %d, want 1 after trimming 3 excess (4 adopted -> min 1)", got)
	}
	terminated := fp.terminatedCopy()
	if len(terminated) != 3 {
		t.Fatalf("terminated = %v, want 3 (excess newest)", terminated)
	}
	remaining := pl.Instances("myfn")
	if len(remaining) != 1 || remaining[0] != "myfn-0" {
		t.Errorf("remaining = %v, want [myfn-0]", remaining)
	}
	sort.Strings(terminated)
	expected := []string{"myfn-1", "myfn-2", "myfn-3"}
	for i, want := range expected {
		if terminated[i] != want {
			t.Errorf("terminated sorted[%d]=%q want %q", i, terminated[i], want)
		}
	}
	raw := fp.terminatedCopy()
	if len(raw) == 3 && (raw[0] != "myfn-3" || raw[1] != "myfn-2" || raw[2] != "myfn-1") {
		t.Errorf("raw terminated order = %v want [myfn-3 myfn-2 myfn-1] newest-first", raw)
	}
}

func TestRestoreScalesDownNotBelowMin(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["svc-0"] = true
	rt.running["svc-1"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "svc", "img:1.0", registry.RegisterOptions{MinInstances: 2}); err != nil {
		t.Fatal(err)
	}
	fp := &scaleRecordingProvisioner{}
	pl := pool.NewManagerWithQueueCap(fp, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if got := pl.Count("svc"); got != 2 {
		t.Errorf("Count() = %d, want 2 (no scale-down when at min)", got)
	}
	if got := len(fp.terminatedCopy()); got != 0 {
		t.Errorf("terminated count = %d, want 0 (should not scale below min)", got)
	}
	if got := len(fp.provisioned); got != 0 {
		t.Errorf("provisioned = %d, want 0 (Ensure should not provision when at min)", got)
	}
}

func TestRestoreStillEnsuresMin(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["api-0"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "api", "img:1.0", registry.RegisterOptions{MinInstances: 3}); err != nil {
		t.Fatal(err)
	}
	fp := &scaleRecordingProvisioner{}
	pl := pool.NewManagerWithQueueCap(fp, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if got := pl.Count("api"); got != 3 {
		t.Fatalf("Count() = %d, want 3 (ScaleDown 0 + Ensure adds 2)", got)
	}
	if got := len(fp.terminatedCopy()); got != 0 {
		t.Errorf("terminated = %v, want 0 (nothing excess to reap)", fp.terminatedCopy())
	}
	if got := len(fp.provisioned); got != 2 {
		t.Errorf("provisioned count = %d, want 2 (deficit after adopt)", got)
	}
	instances := pl.Instances("api")
	sort.Strings(instances)
	if len(instances) != 3 {
		t.Fatalf("instances = %v want 3", instances)
	}
	found0 := false
	for _, id := range instances {
		if id == "api-0" {
			found0 = true
		}
	}
	if !found0 {
		t.Errorf("instances = %v, want to contain adopted api-0", instances)
	}
}

func TestRestoreScaleDownErrorNotFatal(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["errfn-0"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "errfn", "img:1.0", registry.RegisterOptions{MinInstances: 2}); err != nil {
		t.Fatal(err)
	}
	fp := &scaleRecordingProvisioner{}
	inner := pool.NewManagerWithQueueCap(fp, nil)
	fpErr := &fakePoolWithError{
		inner:        inner,
		scaleDownErr: errors.New("simulated ScaleDown failure"),
	}

	err := Run(context.Background(), rt, reg, fpErr)
	if err != nil {
		t.Fatalf("Run() should not return error when only ScaleDown fails but Ensure succeeds, got %v", err)
	}
	calls := fpErr.scaleDownCallsCopy()
	if len(calls) != 1 {
		t.Fatalf("ScaleDownToMin calls = %v, want 1", calls)
	}
	if calls[0].fn != "errfn" || calls[0].min != 2 {
		t.Errorf("ScaleDownToMin call = %+v, want {errfn 2}", calls[0])
	}
	if got := inner.Count("errfn"); got != 2 {
		t.Errorf("Count() = %d, want 2 (Ensure must run despite ScaleDown error)", got)
	}
	fn, _ := reg.Get("errfn")
	if fn.Degraded() {
		t.Error("function should NOT be Degraded when ScaleDown fails but Ensure succeeds")
	}
	if fn.LastError != "" {
		t.Errorf("LastError = %q, want empty when Ensure succeeds", fn.LastError)
	}
}

// ===========================================================================
// Reconciliation of orphan containers
// ===========================================================================

func TestRunDistingueRamosDeOrfao(t *testing.T) {
	buf := capturarLog(t)

	rt := newFakeRuntime()
	rt.running["minha-funcao-3"] = true
	rt.running["outro-123"] = true
	rt.running["lixo-sem-padrao"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "minha-funcao", "img:1.0", registry.RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	pl := pool.NewManagerWithQueueCap(&fakeProvisioner{}, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}

	if got := pl.Count("minha-funcao"); got != 1 {
		t.Errorf("Count(minha-funcao) = %d, esperado 1 (adotada)", got)
	}
	if len(rt.removed) != 1 || rt.removed[0] != "outro-123" {
		t.Errorf("removed = %v, esperado [outro-123]", rt.removed)
	}
	if _, aindaLa := rt.running["lixo-sem-padrao"]; !aindaLa {
		t.Errorf("contêiner fora do padrão foi removido; deveria ser ignorado")
	}
	if !strings.Contains(buf.String(), "casa o padrão de instanceID") {
		t.Errorf("log novo do ramo estranho-no-padrão ausente; saída foi:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "não segue a convenção de nomenclatura esperada") {
		t.Errorf("log antigo do ramo fora-do-padrão ausente; saída foi:\n%s", buf.String())
	}
}
