package registry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// containsIndent reports whether the content has lines that start with two
// spaces of indentation followed by a quote, which is the pattern of the
// indented encoding.
func containsIndent(data []byte) bool {
	for _, line := range splitLines(data) {
		if len(line) >= 3 && line[0] == ' ' && line[1] == ' ' && line[2] == '"' {
			return true
		}
	}
	return false
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

// mustParseTime parses an RFC3339 timestamp for use in the tests.
func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic("invalid test time: " + err.Error())
	}
	return t
}

// ===========================================================================
// Core registry
// ===========================================================================

func TestRegisterBuildsResourceURL(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("xp4-borda-01:8080", "alpr-deteccao-placa", "registry.local/alpr:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register() erro inesperado: %v", err)
	}

	want := "xp4-borda-01:8080/function/alpr-deteccao-placa"
	if fn.ResourceURL != want {
		t.Errorf("ResourceURL = %q, esperado %q", fn.ResourceURL, want)
	}
}

func TestRegisterBlocksDuplicateName(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	if _, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatalf("primeiro Register() erro inesperado: %v", err)
	}

	_, _, err := r.Register("host", "minha-funcao", "img:2.0", RegisterOptions{MinInstances: 2})
	if !errors.Is(err, ErrDuplicateFunction) {
		t.Fatalf("esperava ErrDuplicateFunction, obtido: %v", err)
	}
}

func TestRegisterAcceptsZeroMinInstances(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 0})
	if err != nil {
		t.Fatalf("min_instances=0 deve ser aceito (scale-to-zero opcional): %v", err)
	}
	if fn.MinInstances != 0 {
		t.Errorf("MinInstances = %d, esperado 0", fn.MinInstances)
	}
}

func TestRegisterRejectsNegativeMinInstances(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	_, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: -1})
	if !errors.Is(err, ErrInvalidMinInstances) {
		t.Fatalf("esperava ErrInvalidMinInstances para min_instances=-1, obtido: %v", err)
	}
}

func TestRegisterKeepsOmittedMaxInstancesUnbounded(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 3})
	if err != nil {
		t.Fatalf("Register() erro inesperado: %v", err)
	}
	if fn.MaxInstances != 0 {
		t.Errorf("MaxInstances = %d, esperado 0 (sem teto, já que foi omitido)", fn.MaxInstances)
	}
}

func TestRegisterKeepsUnboundedMaxWithScaleToZero(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 0})
	if err != nil {
		t.Fatalf("Register() erro inesperado: %v", err)
	}
	if fn.MaxInstances != 0 || fn.MinInstances != 0 {
		t.Errorf("(min, max) = (%d, %d), esperado (0, 0) — scale-to-zero sem teto", fn.MinInstances, fn.MaxInstances)
	}
}

func TestRegisterRejectsNegativeMaxInstances(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	_, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 0, MaxInstances: -1})
	if !errors.Is(err, ErrInvalidMaxInstances) {
		t.Fatalf("esperava ErrInvalidMaxInstances para max_instances=-1, obtido: %v", err)
	}
}

func TestRegisterRejectsMaxInstancesBelowMinInstances(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	_, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 3, MaxInstances: 2})
	if !errors.Is(err, ErrInvalidMaxInstances) {
		t.Fatalf("esperava ErrInvalidMaxInstances, obtido: %v", err)
	}
}

func TestRegisterAcceptsExplicitMaxInstances(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 1, MaxInstances: 5})
	if err != nil {
		t.Fatalf("Register() erro inesperado: %v", err)
	}
	if fn.MaxInstances != 5 {
		t.Errorf("MaxInstances = %d, esperado 5", fn.MaxInstances)
	}
}

func TestGetAndList(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := r.Register("host", "func-a", "img:1.0", RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Register("host", "func-b", "img:1.0", RegisterOptions{MinInstances: 2}); err != nil {
		t.Fatal(err)
	}

	if _, ok := r.Get("func-a"); !ok {
		t.Error("esperava encontrar func-a")
	}
	if _, ok := r.Get("inexistente"); ok {
		t.Error("não esperava encontrar função inexistente")
	}

	if got := len(r.List()); got != 2 {
		t.Errorf("List() retornou %d funções, esperado 2", got)
	}
}

func TestUnregisterRemovesFunction(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := r.Register("host", "func-a", "img:1.0", RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}

	if err := r.Unregister("func-a"); err != nil {
		t.Fatalf("Unregister() erro inesperado: %v", err)
	}
	if _, ok := r.Get("func-a"); ok {
		t.Error("função ainda existe após Unregister()")
	}
}

func TestUnregisterNonExistentReturnsErrFunctionNotFound(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	err := r.Unregister("nao-existe")
	if !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("esperava ErrFunctionNotFound, obtido: %v", err)
	}
}

func TestUpdateRequiresExistingFunction(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	_, _, err := r.Update("host", "nao-existe", "img:1.0", RegisterOptions{MinInstances: 1}, false)
	if !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("esperava ErrFunctionNotFound, obtido: %v", err)
	}
}

func TestUpdatePreservesCreatedAtAndBumpsUpdatedAt(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	original, _, err := r.Register("host", "func-a", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}

	updated, _, err := r.Update("host", "func-a", "img:2.0", RegisterOptions{MinInstances: 2}, false)
	if err != nil {
		t.Fatalf("Update() erro inesperado: %v", err)
	}

	if !updated.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("CreatedAt = %v, esperado preservado de %v", updated.CreatedAt, original.CreatedAt)
	}
	if !updated.UpdatedAt.After(original.UpdatedAt) && !updated.UpdatedAt.Equal(original.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, esperado >= %v", updated.UpdatedAt, original.UpdatedAt)
	}
	if updated.ImageRef != "docker.io/library/img:2.0" {
		t.Errorf("ImageRef = %q, esperado %q", updated.ImageRef, "docker.io/library/img:2.0")
	}
	if updated.MinInstances != 2 {
		t.Errorf("MinInstances = %d, esperado 2", updated.MinInstances)
	}
}

func TestUpdateClearsDegradedState(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := r.Register("host", "func-a", "img:1.0", RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkDegraded("func-a", "falha simulada"); err != nil {
		t.Fatal(err)
	}

	updated, _, err := r.Update("host", "func-a", "img:2.0", RegisterOptions{MinInstances: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Degraded() {
		t.Error("Update() deveria limpar Degraded")
	}
	if updated.LastError != "" {
		t.Errorf("LastError = %q, esperado vazio após Update()", updated.LastError)
	}
}

func TestMarkAndClearDegraded(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := r.Register("host", "func-a", "img:1.0", RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}

	if err := r.MarkDegraded("func-a", "provisionamento falhou"); err != nil {
		t.Fatalf("MarkDegraded() erro inesperado: %v", err)
	}
	fn, _ := r.Get("func-a")
	if !fn.Degraded() || fn.LastError != "provisionamento falhou" {
		t.Errorf("função não ficou marcada como degraded corretamente: %+v", fn)
	}

	if err := r.ClearDegraded("func-a"); err != nil {
		t.Fatalf("ClearDegraded() erro inesperado: %v", err)
	}
	fn, _ = r.Get("func-a")
	if fn.Degraded() || fn.LastError != "" {
		t.Errorf("função ainda degraded após ClearDegraded(): %+v", fn)
	}
}

func TestMarkDegradedNonExistentReturnsError(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	err := r.MarkDegraded("nao-existe", "erro")
	if !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("esperava ErrFunctionNotFound, obtido: %v", err)
	}
}

func TestMarkDegradedEmptyLastError(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := r.Register("host", "func-a", "img:1.0", RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkDegraded("func-a", ""); err != nil {
		t.Fatalf("MarkDegraded(\"\") deveria ser permitido (policy): %v", err)
	}
	fn, _ := r.Get("func-a")
	if fn.Degraded() {
		t.Error("MarkDegraded(\"\") deveria deixar Degraded() = false (LastError = '')")
	}
	if fn.LastError != "" {
		t.Errorf("Expected LastError='' after MarkDegraded(\"\"), got %q", fn.LastError)
	}
}

func TestFaultInjectionRegistries(t *testing.T) {
	dir := t.TempDir()
	regPath := filepath.Join(dir, "ro-test.json")
	r, _ := NewPersistentRegistry(regPath)

	if _, _, err := r.Register("host", "func-nova", "img:1.0", RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatalf("Register preparatório: %v", err)
	}
	if err := r.MarkDegraded("func-nova", "erro inicial"); err != nil {
		t.Fatalf("MarkDegraded preparatório: %v", err)
	}

	// The persistence failure is injected in a way that works even as root:
	// the destination file is replaced by a directory. A read-only
	// directory does not work when the tests run as root, because the
	// capability to bypass file permissions applies, while writing a file
	// over a directory fails for every user, both with the temporary file
	// and rename and with a direct write.
	if err := os.Remove(regPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(regPath, 0o755); err != nil {
		t.Fatal(err)
	}

	_, _, err := r.Register("host", "func-outra", "img:1.0", RegisterOptions{MinInstances: 1})
	if err == nil {
		t.Error("Register() deveria falhar em diretório somente-leitura")
	}
	if _, ok := r.Get("func-outra"); ok {
		t.Error("Register() com falha de persistência não deveria adicionar função em memória")
	}

	antes, _ := r.Get("func-nova")
	_, _, err = r.Update("host", "func-nova", "img:2.0", RegisterOptions{MinInstances: 2}, false)
	if err == nil {
		t.Error("Update() deveria falhar em diretório somente-leitura")
	}
	depois, _ := r.Get("func-nova")
	if !reflect.DeepEqual(depois, antes) {
		t.Error("Update() com falha de persistência não deveria alterar função em memória")
	}

	err = r.MarkDegraded("func-nova", "outro erro")
	if err == nil {
		t.Error("MarkDegraded() deveria falhar em diretório somente-leitura")
	}
	if fn, _ := r.Get("func-nova"); fn.LastError != "erro inicial" {
		t.Error("MarkDegraded() com falha de persistência não deveria marcar função em memória")
	}

	err = r.ClearDegraded("func-nova")
	if err == nil {
		t.Error("ClearDegraded() deveria falhar em diretório somente-leitura")
	}
	if fn, _ := r.Get("func-nova"); !fn.Degraded() {
		t.Error("ClearDegraded() com falha de persistência não deveria limpar função em memória")
	}

	err = r.Unregister("func-nova")
	if err == nil {
		t.Error("Unregister() deveria falhar em diretório somente-leitura")
	}
	if _, ok := r.Get("func-nova"); !ok {
		t.Error("Unregister() com falha de persistência não deveria remover função em memória")
	}
}

func TestJSONTagsUseSnakeCase(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	fn, _, err := r.Register("host", "func-a", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(fn)
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{`"function_name"`, `"resource_url"`, `"image_ref"`, `"min_instances"`, `"max_instances"`, `"created_at"`, `"updated_at"`} {
		if !strings.Contains(string(data), key) {
			t.Errorf("JSON não contém a chave esperada %s; JSON: %s", key, data)
		}
	}
	if strings.Contains(string(data), `"Name"`) || strings.Contains(string(data), `"ResourceURL"`) {
		t.Errorf("JSON ainda usa PascalCase: %s", data)
	}
}

func TestRegistryQueueMax_Happy(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 0, QueueMax: 10})
	if err != nil {
		t.Fatalf("Register() erro inesperado: %v", err)
	}
	if fn.QueueMax != 10 {
		t.Errorf("QueueMax = %d, esperado 10", fn.QueueMax)
	}

	fnGot, ok := r.Get("minha-funcao")
	if !ok {
		t.Error("esperava encontrar função registrada")
	}
	if fnGot.QueueMax != 10 {
		t.Errorf("GET QueueMax = %d, esperado 10", fnGot.QueueMax)
	}
}

func TestRegistryQueueMax_RejectsWhenMaxSet(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	_, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 1, MaxInstances: 2, QueueMax: 5})
	if !errors.Is(err, ErrInvalidScalingParams) {
		t.Fatalf("esperava ErrInvalidScalingParams para max_instances=2, queue_max=5, obtido: %v", err)
	}
}

func TestRegistryQueueMax_UnboundedWithQueueMax(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 0, QueueMax: 5})
	if err != nil {
		t.Fatalf("Register() erro inesperado com max_instances=0 e queue_max=5: %v", err)
	}
	if fn.QueueMax != 5 {
		t.Errorf("QueueMax = %d, esperado 5", fn.QueueMax)
	}
}

func TestRegistryQueueMax_ZeroQueueMaxWithUnbounded(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 0})
	if err != nil {
		t.Fatalf("Register() erro inesperado com max_instances=0 e queue_max=0: %v", err)
	}
	if fn.QueueMax != 0 {
		t.Errorf("QueueMax = %d, esperado 0 (pool default 32)", fn.QueueMax)
	}
}

func TestRegistryQueueMax_RejectsQueueMaxOutOfRange(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	_, _, err := r.Register("host", "minha-funcao", "img:1.0", RegisterOptions{MinInstances: 0, QueueMax: 300})
	if !errors.Is(err, ErrInvalidScalingParams) {
		t.Fatalf("esperava ErrInvalidScalingParams para queue_max=300, obtido: %v", err)
	}
}

func TestJSONTagsIncludeQueueMax(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	fn, _, err := r.Register("host", "func-a", "img:1.0", RegisterOptions{MinInstances: 1, QueueMax: 10})
	if err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(fn)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(data), `"queue_max"`) {
		t.Errorf("JSON não contém a chave queue_max; JSON: %s", data)
	}
	if !strings.Contains(string(data), `"queue_max":10`) {
		t.Errorf("JSON não contém queue_max=10; JSON: %s", data)
	}
}

func TestRegisterNormalizesImageRef(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("host", "func-redis", "redis:7", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register(redis:7) erro inesperado: %v", err)
	}
	if fn.ImageRef != "docker.io/library/redis:7" {
		t.Errorf("ImageRef = %q, esperado %q", fn.ImageRef, "docker.io/library/redis:7")
	}

	r2, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	fn2, _, err := r2.Register("host", "func-user", "igoricda/alpr:latest", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register(igoricda/alpr:latest) erro inesperado: %v", err)
	}
	if fn2.ImageRef != "docker.io/igoricda/alpr:latest" {
		t.Errorf("ImageRef = %q, esperado %q", fn2.ImageRef, "docker.io/igoricda/alpr:latest")
	}

	r3, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	fn3, _, err := r3.Register("host", "func-explicit", "registry.local/alpr:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register(registry.local/alpr:1.0) erro inesperado: %v", err)
	}
	if fn3.ImageRef != "registry.local/alpr:1.0" {
		t.Errorf("ImageRef = %q, esperado %q", fn3.ImageRef, "registry.local/alpr:1.0")
	}
}

func TestRegisterRejectsInvalidImageRef(t *testing.T) {
	for _, input := range []string{"", "   ", "::::invalida"} {
		r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		_, _, err := r.Register("host", "func-x", input, RegisterOptions{MinInstances: 1})
		if err == nil {
			t.Fatalf("Register(%q): esperava erro, obtido nil", input)
		}
		if !errors.Is(err, ErrInvalidImageRef) {
			t.Errorf("Register(%q): erro %v não embrulha ErrInvalidImageRef", input, err)
		}
		if len(r.List()) != 0 {
			t.Errorf("Register(%q): função foi persistida apesar do erro (len=%d)", input, len(r.List()))
		}
	}
}

func TestRegisterAllowlist(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"), "example.com")

	fn, _, err := r.Register("host", "func-ok", "example.com/alpr:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register(example.com/alpr:1.0) com allowlist [example.com] erro inesperado: %v", err)
	}
	if fn.ImageRef != "example.com/alpr:1.0" {
		t.Errorf("ImageRef = %q, esperado %q", fn.ImageRef, "example.com/alpr:1.0")
	}

	r2, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"), "example.com")
	_, _, err = r2.Register("host", "func-blocked", "evil.io/x:latest", RegisterOptions{MinInstances: 1})
	if err == nil {
		t.Fatal("Register(evil.io/x:latest) com allowlist [example.com] deveria falhar")
	}
	if !errors.Is(err, ErrInvalidImageRef) {
		t.Errorf("Register(evil.io/x:latest): erro %v não embrulha ErrInvalidImageRef", err)
	}
	if len(r2.List()) != 0 {
		t.Errorf("Register(evil.io/x:latest): função foi persistida apesar do erro")
	}

	r3, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	fn3, _, err := r3.Register("host", "func-any", "usuario/imagem:tag", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register com allowlist vazia erro inesperado: %v", err)
	}
	if fn3.ImageRef != "docker.io/usuario/imagem:tag" {
		t.Errorf("ImageRef = %q, esperado normalizado para docker.io/...", fn3.ImageRef)
	}
}

func TestUpdateNormalizesAndValidatesImageRef(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"), "example.com")
	if _, _, err := r.Register("host", "func-upd", "example.com/original:1.0", RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}

	updated, _, err := r.Update("host", "func-upd", "example.com/nova:2.0", RegisterOptions{MinInstances: 2}, false)
	if err != nil {
		t.Fatalf("Update com image_ref válido erro inesperado: %v", err)
	}
	if updated.ImageRef != "example.com/nova:2.0" {
		t.Errorf("ImageRef após Update = %q, esperado %q", updated.ImageRef, "example.com/nova:2.0")
	}

	_, _, err = r.Update("host", "func-upd", "evil.io/bad:3.0", RegisterOptions{MinInstances: 1}, false)
	if err == nil {
		t.Fatal("Update com image_ref fora da allowlist deveria falhar")
	}
	if !errors.Is(err, ErrInvalidImageRef) {
		t.Errorf("Update: erro %v não embrulha ErrInvalidImageRef", err)
	}
	fn, _ := r.Get("func-upd")
	if fn.ImageRef != "example.com/nova:2.0" {
		t.Errorf("ImageRef após Update falho = %q, esperado preservado %q", fn.ImageRef, "example.com/nova:2.0")
	}
}

// ===========================================================================
// Idle timeout
// ===========================================================================

func TestRegistryIdleTimeout(t *testing.T) {
	t.Run("zero stores zero", func(t *testing.T) {
		r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		fn, _, err := r.Register("host", "func-zero", "img:1.0", RegisterOptions{IdleTimeout: config.Duration(0)})
		if err != nil {
			t.Fatalf("Register with IdleTimeout=0 failed: %v", err)
		}
		if time.Duration(fn.IdleTimeout) != 0 {
			t.Errorf("IdleTimeout = %v, want 0", time.Duration(fn.IdleTimeout))
		}
		got, ok := r.Get("func-zero")
		if !ok {
			t.Fatal("Get failed")
		}
		if time.Duration(got.IdleTimeout) != 0 {
			t.Errorf("Get IdleTimeout = %v, want 0", time.Duration(got.IdleTimeout))
		}
		list := r.List()
		found := false
		for _, f := range list {
			if f.Name == "func-zero" && time.Duration(f.IdleTimeout) == 0 {
				found = true
			}
		}
		if !found {
			t.Error("List did not return IdleTimeout=0")
		}
	})

	t.Run("five minutes stores and persists JSON", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "functions.json")
		r, err := NewPersistentRegistry(path)
		if err != nil {
			t.Fatal(err)
		}
		fn, _, err := r.Register("host", "func-5m", "img:1.0", RegisterOptions{IdleTimeout: config.Duration(5 * time.Minute)})
		if err != nil {
			t.Fatalf("Register with 5m failed: %v", err)
		}
		if time.Duration(fn.IdleTimeout) != 5*time.Minute {
			t.Errorf("IdleTimeout = %v, want 5m", time.Duration(fn.IdleTimeout))
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), `"idle_timeout"`) || !strings.Contains(string(data), `"5m0s"`) {
			t.Errorf("persisted JSON missing idle_timeout 5m0s; JSON: %s", string(data))
		}
		// The format on disk is compact, with a trailing newline. The
		// indentation is not asserted here: the assertion belongs to the
		// test that checks the compact encoding, the trailing newline and
		// the file permissions.
		got, ok := r.Get("func-5m")
		if !ok || time.Duration(got.IdleTimeout) != 5*time.Minute {
			t.Errorf("Get IdleTimeout = %v, want 5m", time.Duration(got.IdleTimeout))
		}
	})

	t.Run("negativo devolve ErrInvalidIdleTimeout", func(t *testing.T) {
		r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		_, _, err := r.Register("host", "func-neg", "img:1.0", RegisterOptions{IdleTimeout: config.Duration(-1 * time.Second)})
		if !errors.Is(err, ErrInvalidIdleTimeout) {
			t.Fatalf("expected ErrInvalidIdleTimeout for negative, got: %v", err)
		}
		if _, _, err := r.Register("host", "func-ok", "img:1.0", RegisterOptions{}); err != nil {
			t.Fatal(err)
		}
		_, _, err = r.Update("host", "func-ok", "img:2.0", RegisterOptions{IdleTimeout: config.Duration(-5 * time.Minute)}, false)
		if !errors.Is(err, ErrInvalidIdleTimeout) {
			t.Fatalf("expected ErrInvalidIdleTimeout for Update negative, got: %v", err)
		}
	})

	t.Run("acima de 24h devolve ErrInvalidIdleTimeout", func(t *testing.T) {
		r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		_, _, err := r.Register("host", "func-over", "img:1.0", RegisterOptions{IdleTimeout: config.Duration(25 * time.Hour)})
		if !errors.Is(err, ErrInvalidIdleTimeout) {
			t.Fatalf("expected ErrInvalidIdleTimeout for >24h, got: %v", err)
		}
		if _, _, err := r.Register("host", "func-ok2", "img:1.0", RegisterOptions{}); err != nil {
			t.Fatal(err)
		}
		_, _, err = r.Update("host", "func-ok2", "img:2.0", RegisterOptions{IdleTimeout: config.Duration(25 * time.Hour)}, false)
		if !errors.Is(err, ErrInvalidIdleTimeout) {
			t.Fatalf("expected ErrInvalidIdleTimeout for Update >24h, got: %v", err)
		}
	})

	t.Run("persistent round-trip", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "functions.json")
		r1, err := NewPersistentRegistry(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := r1.Register("host", "func-rt", "img:1.0", RegisterOptions{IdleTimeout: config.Duration(10 * time.Minute)}); err != nil {
			t.Fatal(err)
		}
		r2, err := NewPersistentRegistry(path)
		if err != nil {
			t.Fatal(err)
		}
		fn, ok := r2.Get("func-rt")
		if !ok {
			t.Fatal("func not found after reload")
		}
		if time.Duration(fn.IdleTimeout) != 10*time.Minute {
			t.Errorf("after reload IdleTimeout = %v, want 10m", time.Duration(fn.IdleTimeout))
		}
	})
	t.Run("Update stores idle_timeout", func(t *testing.T) {
		r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		if _, _, err := r.Register("host", "func-upd", "img:1.0", RegisterOptions{}); err != nil {
			t.Fatal(err)
		}
		updated, _, err := r.Update("host", "func-upd", "img:2.0", RegisterOptions{IdleTimeout: config.Duration(7 * time.Minute)}, false)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		if time.Duration(updated.IdleTimeout) != 7*time.Minute {
			t.Errorf("Update IdleTimeout = %v, want 7m", time.Duration(updated.IdleTimeout))
		}
		got, _ := r.Get("func-upd")
		if time.Duration(got.IdleTimeout) != 7*time.Minute {
			t.Errorf("Get after Update IdleTimeout = %v, want 7m", time.Duration(got.IdleTimeout))
		}
	})

	t.Run("List and Get return field", func(t *testing.T) {
		r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		if _, _, err := r.Register("host", "func-list", "img:1.0", RegisterOptions{IdleTimeout: config.Duration(3 * time.Minute)}); err != nil {
			t.Fatal(err)
		}
		got, ok := r.Get("func-list")
		if !ok {
			t.Fatal("Get failed")
		}
		if time.Duration(got.IdleTimeout) != 3*time.Minute {
			t.Errorf("Get IdleTimeout = %v, want 3m", time.Duration(got.IdleTimeout))
		}
		list := r.List()
		if len(list) != 1 || time.Duration(list[0].IdleTimeout) != 3*time.Minute {
			t.Errorf("List IdleTimeout = %v, want 3m", time.Duration(list[0].IdleTimeout))
		}
	})
}

// ===========================================================================
// Invoke tokens
// ===========================================================================

func TestInvokeTokenShowOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "functions.json")
	r, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	fn, plain, err := r.Register("host", "f", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}
	if plain == "" {
		t.Fatal("Register() devolveu plaintext vazio")
	}
	if fn.InvokeTokenSHA256 == "" {
		t.Fatal("Register() não preencheu InvokeTokenSHA256")
	}
	if !VerifyInvokeToken(plain, fn.InvokeTokenSHA256) {
		t.Error("token recém-emitido não verifica contra o hash armazenado")
	}

	data, err := json.Marshal(fn)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), plain) {
		t.Error("plaintext do token vazou no JSON da Function")
	}
	if !strings.Contains(string(data), `"invoke_token_sha256"`) {
		t.Error("JSON não contém invoke_token_sha256")
	}
	got, ok := r.Get("f")
	if !ok {
		t.Fatal("função sumiu do registro")
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), plain) {
		t.Error("plaintext do token vazou no GET")
	}

	r2, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	fn2, ok := r2.Get("f")
	if !ok {
		t.Fatal("função sumiu após reload")
	}
	if !VerifyInvokeToken(plain, fn2.InvokeTokenSHA256) {
		t.Error("token não verifica após reload do arquivo")
	}
}

func TestInvokeTokenLegacyDenies(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "functions.json")
	legacy := `{"f":{"function_name":"f","resource_url":"host/function/f","image_ref":"img:1.0","min_instances":1,"max_instances":0,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	fn, ok := r.Get("f")
	if !ok {
		t.Fatal("função legada não carregou")
	}
	if VerifyInvokeToken("qualquer-coisa", fn.InvokeTokenSHA256) {
		t.Error("função legada (hash vazio) aceitou token; esperado fail-closed")
	}
}

func TestUpdateRotateToken(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	fn, plain, err := r.Register("host", "f", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}

	updated, plain2, err := r.Update("host", "f", "img:2.0", RegisterOptions{MinInstances: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	if plain2 != "" {
		t.Errorf("Update sem rotate devolveu plaintext %q, esperado vazio", plain2)
	}
	if updated.InvokeTokenSHA256 != fn.InvokeTokenSHA256 {
		t.Error("Update sem rotate trocou o hash")
	}
	if !VerifyInvokeToken(plain, updated.InvokeTokenSHA256) {
		t.Error("token antigo parou de verificar após Update sem rotate")
	}

	updated2, plain3, err := r.Update("host", "f", "img:3.0", RegisterOptions{MinInstances: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	if plain3 == "" || plain3 == plain {
		t.Errorf("Update com rotate devolveu plaintext inválido %q", plain3)
	}
	if !VerifyInvokeToken(plain3, updated2.InvokeTokenSHA256) {
		t.Error("token novo não verifica após rotate")
	}
	if VerifyInvokeToken(plain, updated2.InvokeTokenSHA256) {
		t.Error("token antigo continua válido após rotate")
	}
}

func TestVerifyInvokeTokenTotal(t *testing.T) {
	_, sumHex, err := generateInvokeToken()
	if err != nil {
		t.Fatal(err)
	}
	if VerifyInvokeToken("", sumHex) {
		t.Error("token vazio aceito")
	}
	if VerifyInvokeToken(strings.Repeat("x", MaxPresentedInvokeTokenBytes+1), sumHex) {
		t.Error("token gigante aceito")
	}
	if VerifyInvokeToken("nao-e-o-token", sumHex) {
		t.Error("token errado aceito")
	}
	if VerifyInvokeToken("qualquer", "") {
		t.Error("hash vazio aceitou token")
	}
	if VerifyInvokeToken("qualquer", "hex-invalido!!!") {
		t.Error("hash malformado aceitou token")
	}
}

// ===========================================================================
// Invoke token cache
// ===========================================================================

func TestInvokeTokenCacheHit(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	_, plain, err := r.Register("host", "f", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}
	fn, ok := r.Get("f")
	if !ok {
		t.Fatal("função sumiu do registro")
	}
	if !fn.invokeTokenSumValid {
		t.Fatal("cache não populado no Register")
	}
	if !fn.VerifyPresentedToken(plain) {
		t.Error("token válido negado pelo caminho com cache")
	}
	if fn.VerifyPresentedToken("deadbeef") {
		t.Error("token inválido aceito pelo caminho com cache")
	}
	if fn.VerifyPresentedToken("") {
		t.Error("token vazio aceito pelo caminho com cache")
	}
	if fn.VerifyPresentedToken(strings.Repeat("x", MaxPresentedInvokeTokenBytes+1)) {
		t.Error("token gigante aceito pelo caminho com cache")
	}
}

func TestInvokeTokenCacheRotate(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	_, plain, err := r.Register("host", "f", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, plain2, err := r.Update("host", "f", "img:2.0", RegisterOptions{MinInstances: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	fn, ok := r.Get("f")
	if !ok {
		t.Fatal("função sumiu do registro")
	}
	if !fn.VerifyPresentedToken(plain2) {
		t.Error("token novo negado após rotate")
	}
	if fn.VerifyPresentedToken(plain) {
		t.Error("token antigo aceito após rotate")
	}

	if _, _, err := r.Update("host", "f", "img:3.0", RegisterOptions{MinInstances: 1}, false); err != nil {
		t.Fatal(err)
	}
	fn2, ok := r.Get("f")
	if !ok {
		t.Fatal("função sumiu do registro")
	}
	if !fn2.VerifyPresentedToken(plain2) {
		t.Error("token parou de verificar após Update sem rotate")
	}
}

func TestInvokeTokenCacheLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "functions.json")
	r, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	_, plain, err := r.Register("host", "f", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	fn, ok := r2.Get("f")
	if !ok {
		t.Fatal("função sumiu após reload")
	}
	if !fn.invokeTokenSumValid {
		t.Fatal("cache não populado no load")
	}
	if !fn.VerifyPresentedToken(plain) {
		t.Error("token não verifica após reload pelo caminho com cache")
	}
	if fn.VerifyPresentedToken("deadbeef") {
		t.Error("token inválido aceito após reload")
	}
}

func TestInvokeTokenCacheLegacyDenies(t *testing.T) {
	var fn Function
	fn.setInvokeTokenCache("")
	if fn.invokeTokenSumValid {
		t.Error("hash vazio marcou cache como válido")
	}
	if fn.VerifyPresentedToken("qualquer-coisa") {
		t.Error("hash vazio aceitou token pelo caminho com cache")
	}
	fn.setInvokeTokenCache("hex-invalido!!!")
	if fn.VerifyPresentedToken("qualquer-coisa") {
		t.Error("hash malformado aceitou token pelo caminho com cache")
	}
}

func TestInvokeTokenCacheLiteralFallback(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	_, plain, err := r.Register("host", "f", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}
	stored, ok := r.Get("f")
	if !ok {
		t.Fatal("função sumiu do registro")
	}
	literal := Function{Name: "f", InvokeTokenSHA256: stored.InvokeTokenSHA256}
	if literal.invokeTokenSumValid {
		t.Fatal("literal construído à mão não deveria ter cache válido")
	}
	if !literal.VerifyPresentedToken(plain) {
		t.Error("literal com hash válido negou o token certo no fallback")
	}
	if literal.VerifyPresentedToken("deadbeef") {
		t.Error("literal aceitou token errado no fallback")
	}
}

// ===========================================================================
// Durability of the persisted file
// ===========================================================================

func TestDurabilityKill9Restart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "functions.json")

	{
		r, err := NewPersistentRegistry(path)
		if err != nil {
			t.Fatalf("NewPersistentRegistry failed: %v", err)
		}

		funcs := []struct {
			name    string
			image   string
			minInst int
			maxInst int
		}{
			{"alpr-deteccao-placa", "docker.io/library/alpr:1.0", 1, 3},
			{"face-recognition", "docker.io/library/face-rec:2.0", 0, 0},
			{"object-detection", "docker.io/library/yolo:3.0", 2, 5},
		}

		for _, f := range funcs {
			_, _, err := r.Register("xp4-borda-01:8080", f.name, f.image, RegisterOptions{
				MinInstances: f.minInst,
				MaxInstances: f.maxInst,
			})
			if err != nil {
				t.Fatalf("Register %s failed: %v", f.name, err)
			}
		}

		if len(r.List()) != 3 {
			t.Fatalf("Expected 3 functions, got %d", len(r.List()))
		}
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Read file after 'crash': %v", err)
	}

	var onDisk map[string]Function
	if err := json.Unmarshal(content, &onDisk); err != nil {
		t.Fatalf("On-disk JSON invalid: %v", err)
	}
	if len(onDisk) != 3 {
		t.Fatalf("Expected 3 functions on disk, got %d", len(onDisk))
	}

	{
		r, err := NewPersistentRegistry(path)
		if err != nil {
			t.Fatalf("Restart NewPersistentRegistry failed: %v", err)
		}

		if len(r.List()) != 3 {
			t.Fatalf("After restart: expected 3 functions, got %d", len(r.List()))
		}

		expected := map[string]struct{ min, max int }{
			"alpr-deteccao-placa": {1, 3},
			"face-recognition":    {0, 0},
			"object-detection":    {2, 5},
		}

		for name, exp := range expected {
			fn, ok := r.Get(name)
			if !ok {
				t.Errorf("MISSING after restart: %s", name)
				continue
			}
			if fn.MinInstances != exp.min || fn.MaxInstances != exp.max {
				t.Errorf("MISMATCH %s: got min=%d max=%d, expected min=%d max=%d",
					name, fn.MinInstances, fn.MaxInstances, exp.min, exp.max)
			}
		}
	}

	{
		r, err := NewPersistentRegistry(path)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = r.Register("xp4-borda-01:8080", "new-function", "docker.io/library/new:1.0", RegisterOptions{MinInstances: 1})
		if err != nil {
			t.Fatalf("Register after restart failed: %v", err)
		}

		r2, err := NewPersistentRegistry(path)
		if err != nil {
			t.Fatal(err)
		}
		fn, ok := r2.Get("new-function")
		if !ok {
			t.Fatal("new-function not persisted after restart")
		}
		if fn.Name != "new-function" {
			t.Errorf("Expected new-function, got %s", fn.Name)
		}
	}
}

func TestConcurrentWritesDurability(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "functions.json")

	r, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 50)
	for i := 0; i < 50; i++ {
		go func(id int) {
			name := "func-" + string(rune('0'+id%10)) + string(rune('0'+id/10))
			name = "func-" + string(rune(id))
			if id >= 100 {
				name = "func-" + string(rune(id/100)) + string(rune((id/10)%10)) + string(rune(id%10))
			}
			_, _, err := r.Register("host", name, "img:1.0", RegisterOptions{MinInstances: 1})
			done <- err
		}(i)
	}

	for i := 0; i < 50; i++ {
		if err := <-done; err != nil {
			t.Logf("Concurrent register error (may be duplicate): %v", err)
		}
	}

	countBefore := len(r.List())
	t.Logf("Function count before restart: %d", countBefore)

	r2, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatal(err)
	}

	countAfter := len(r2.List())
	t.Logf("Function count after restart: %d", countAfter)

	if countBefore != countAfter {
		t.Errorf("Data loss: %d before, %d after restart", countBefore, countAfter)
	}
}

// ===========================================================================
// Compact persistence
// ===========================================================================

func TestLoadLegacyIndentedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "functions.json")

	legacyData := map[string]Function{
		"alpr-deteccao-placa": {
			Name:              "alpr-deteccao-placa",
			ResourceURL:       "xp4-borda-01:8080/function/alpr-deteccao-placa",
			ImageRef:          "docker.io/library/alpr:1.0",
			MinInstances:      1,
			MaxInstances:      3,
			QueueMax:          10,
			TimeoutMS:         5000,
			IdleTimeout:       0,
			InvokeTokenSHA256: "a1b2c3d4e5f6",
			CreatedAt:         mustParseTime("2024-01-15T10:30:00Z"),
			UpdatedAt:         mustParseTime("2024-01-15T10:30:00Z"),
			LastError:         "",
		},
		"face-recognition": {
			Name:              "face-recognition",
			ResourceURL:       "xp4-borda-01:8080/function/face-recognition",
			ImageRef:          "docker.io/library/face-rec:2.0",
			MinInstances:      0,
			MaxInstances:      0,
			QueueMax:          5,
			TimeoutMS:         10000,
			IdleTimeout:       0,
			InvokeTokenSHA256: "f6e5d4c3b2a1",
			CreatedAt:         mustParseTime("2024-02-20T14:45:00Z"),
			UpdatedAt:         mustParseTime("2024-02-20T14:45:00Z"),
			LastError:         "provisionamento falhou",
		},
	}

	indentedBytes, err := json.MarshalIndent(legacyData, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent falhou: %v", err)
	}
	indentedBytes = append(indentedBytes, '\n')

	if err := os.WriteFile(path, indentedBytes, 0o600); err != nil {
		t.Fatalf("os.WriteFile falhou: %v", err)
	}

	content, _ := os.ReadFile(path)
	if !containsIndent(content) {
		t.Fatal("fixture não contém indentação esperada — teste inválido")
	}

	r, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatalf("NewPersistentRegistry falhou ao carregar arquivo legado: %v", err)
	}

	fn1, ok := r.Get("alpr-deteccao-placa")
	if !ok {
		t.Fatal("função alpr-deteccao-placa não encontrada após load")
	}
	if fn1.Name != "alpr-deteccao-placa" || fn1.MinInstances != 1 || fn1.MaxInstances != 3 {
		t.Errorf("alpr-deteccao-placa: dados incorretos: %+v", fn1)
	}
	if !fn1.CreatedAt.Equal(mustParseTime("2024-01-15T10:30:00Z")) {
		t.Errorf("alpr-deteccao-placa CreatedAt: %v", fn1.CreatedAt)
	}
	if fn1.Degraded() {
		t.Error("alpr-deteccao-placa não deveria estar degraded")
	}

	fn2, ok := r.Get("face-recognition")
	if !ok {
		t.Fatal("função face-recognition não encontrada após load")
	}
	if fn2.Name != "face-recognition" || fn2.MinInstances != 0 || fn2.MaxInstances != 0 {
		t.Errorf("face-recognition: dados incorretos: %+v", fn2)
	}
	if !fn2.CreatedAt.Equal(mustParseTime("2024-02-20T14:45:00Z")) {
		t.Errorf("face-recognition CreatedAt: %v", fn2.CreatedAt)
	}
	if !fn2.Degraded() || fn2.LastError != "provisionamento falhou" {
		t.Errorf("face-recognition deveria estar degraded com LastError: %+v", fn2)
	}

	if len(r.List()) != 2 {
		t.Errorf("List() retornou %d funções, esperado 2", len(r.List()))
	}
}

func TestSaveProducesCompactJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "functions.json")

	r, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatalf("NewPersistentRegistry falhou: %v", err)
	}

	_, _, err = r.Register("host", "test-func", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register falhou: %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile falhou: %v", err)
	}

	if containsIndent(content) {
		t.Errorf("arquivo gravado contém indentação (esperado JSON compacto):\n%s", string(content))
	}

	if len(content) == 0 || content[len(content)-1] != '\n' {
		t.Errorf("arquivo não termina com newline: %q", string(content))
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat falhou: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("permissão do arquivo = %o, esperado 0600", info.Mode().Perm())
	}

	var loaded map[string]Function
	if err := json.Unmarshal(content, &loaded); err != nil {
		t.Errorf("JSON gravado não é válido: %v", err)
	}
	if len(loaded) != 1 {
		t.Errorf("round-trip carregou %d funções, esperado 1", len(loaded))
	}
}
