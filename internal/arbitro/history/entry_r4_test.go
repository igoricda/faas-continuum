package history

// Cobertura focada da quebra autorizada r4 (selflearning-telemetry-seeded-history,
// todo 5): Entry carrega PSICpuSomeAvg10/PSIMemFullAvg10 (float64, omitempty),
// o arquivo carrega a sentinela historySchemaSentinelKey, e NewHistory rejeita
// r3 sem marcador com erro fail-closed que nomeia o migrador (todo 6).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEntryR4(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket25_50}

	t.Run("r4-with-PSI round-trips (save+load preserva valores)", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "history.json")
		h, err := NewHistory(path, 100)
		if err != nil {
			t.Fatal(err)
		}
		e := Entry{
			FunctionName:      "f",
			NodeID:            "n1",
			Timestamp:         now,
			ScenarioContext:   sc,
			ObservedLatencyMS: 120,
			Outcome:           OutcomeSucesso,
			PSICpuSomeAvg10:   12.5,
			PSIMemFullAvg10:   8.25,
		}
		if err := h.AppendWithGroup(e, "f"); err != nil {
			t.Fatal(err)
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}

		// O arquivo em disco carrega o marcador r4.
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var disk map[string][]Entry
		if err := json.Unmarshal(raw, &disk); err != nil {
			t.Fatal(err)
		}
		if _, ok := disk[historySchemaSentinelKey]; !ok {
			t.Fatalf("arquivo r4 sem chave sentinela %q", historySchemaSentinelKey)
		}

		h2, err := NewHistory(path, 100)
		if err != nil {
			t.Fatalf("NewHistory sobre r4: %v", err)
		}
		defer h2.Close()
		key := SegmentKey("f", "n1", sc)
		entries := h2.m[key]
		if len(entries) != 1 {
			t.Fatalf("entries=%d, want 1", len(entries))
		}
		if entries[0].PSICpuSomeAvg10 != 12.5 || entries[0].PSIMemFullAvg10 != 8.25 {
			t.Fatalf("PSI=(%v,%v), want (12.5,8.25)",
				entries[0].PSICpuSomeAvg10, entries[0].PSIMemFullAvg10)
		}
		// A sentinela não vaza para o mapa em memória.
		for _, k := range h2.Keys() {
			if k == historySchemaSentinelKey {
				t.Fatalf("sentinela vazou para Keys()")
			}
		}
	})

	t.Run("zero-Value Entry marshaliza SEM as chaves PSI (r3 byte-clean)", func(t *testing.T) {
		var e Entry
		wire, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(wire, &m); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"psi_cpu_some_avg10", "psi_mem_full_avg10"} {
			if _, ok := m[k]; ok {
				t.Errorf("zero Entry marshalizou %q (omitempty quebrado)", k)
			}
		}
	})

	t.Run("r3-without-fields decodifica PSI==0 (tolerante, ausente=>unknown)", func(t *testing.T) {
		// JSON em forma r3: sem chaves PSI, sem sentinela — decodifica no
		// nível de Entry (a REJEIÇÃO de arquivo r3 é no NewHistory, abaixo).
		wire := `{"timestamp":"2026-09-23T21:52:00Z","function_name":"f","node_id":"n1",` +
			`"outcome":"sucesso","scenario_context":{"load_bucket":"25-50%"},` +
			`"observed_latency_ms":120,"slo_met":true,"cold_start":false}`
		var e Entry
		if err := json.Unmarshal([]byte(wire), &e); err != nil {
			t.Fatal(err)
		}
		if e.PSICpuSomeAvg10 != 0 || e.PSIMemFullAvg10 != 0 {
			t.Fatalf("PSI=(%v,%v), want (0,0)", e.PSICpuSomeAvg10, e.PSIMemFullAvg10)
		}
	})

	// Este caso carregava um falso-verde antes da auditoria de 2026-09-28:
	// (1) escrevia bytes NUL CRUOS na chave, o que torna o JSON invalido --
	// o loader falhava no ramo "corrompido", nunca no de "sem marcador r4";
	// (2) asseverava Contains(err, "migrador"), mas o caminho do TempDir
	// carrega o NOME do subtesto, que contem a palavra "migrador" -- a
	// assercao era satisfeita pelo path, nao pela mensagem. Ou seja: o
	// caminho fail-closed do r3 nunca foi exercitado de fato.
	//
	// Aqui o blob usa o escape JSON \u0000 (como a fixture pinada real), o
	// que de fato chega ao ramo de formato obsoleto, e as asercoes usam
	// literais que NAO podem aparecer no path.
	t.Run("NewHistory sobre r3 sem marcador => erro fail-closed orientando remocao", func(t *testing.T) {
		blob := "{\"fn\\u0000n1\\u000025-50%\":[{\"timestamp\":\"2026-09-23T21:52:00Z\"," +
			"\"function_name\":\"f\",\"node_id\":\"n1\",\"outcome\":\"sucesso\"," +
			"\"scenario_context\":{\"load_bucket\":\"25-50%\"},\"observed_latency_ms\":120," +
			"\"slo_met\":true,\"cold_start\":false}]}\n"
		path := filepath.Join(t.TempDir(), "history-sem-marcador.json")
		if err := os.WriteFile(path, []byte(blob), 0o600); err != nil {
			t.Fatal(err)
		}
		// Sanidade do fixture: tem de ser JSON VALIDO, senao o teste
		// viraria de novo um "corrompido" disfarçado de r3.
		var probe map[string][]Entry
		if err := json.Unmarshal([]byte(blob), &probe); err != nil {
			t.Fatalf("fixture r3 invalida: %v", err)
		}
		h, err := NewHistory(path, 100)
		if err == nil {
			if h != nil {
				_ = h.Close()
			}
			t.Fatal("NewHistory sobre r3 sem marcador deveria retornar erro")
		}
		// Deve ser o ramo de formato obsoleto, e NAO o de JSON corrompido.
		if strings.Contains(err.Error(), "corrompido") {
			t.Fatalf("caiu no ramo de corrompido em vez do de formato r3: %v", err)
		}
		if !strings.Contains(err.Error(), "sem marcador r4") {
			t.Fatalf("erro nao nomeia o formato r3 sem marcador: %v", err)
		}
		// O r3 foi aposentado (2026-09-28) e o migrador removido: a
		// orientacao correta ao operador e remover o arquivo, e a mensagem
		// nao pode mandar rodar um comando que nao existe mais.
		if !strings.Contains(err.Error(), "aposentado") {
			t.Fatalf("erro nao orienta a remocao do arquivo r3: %v", err)
		}
		if strings.Contains(err.Error(), "rode o migrador") {
			t.Fatalf("erro ainda manda rodar migrador inexistente: %v", err)
		}
	})

	t.Run("arquivo corrompido ainda erra como antes", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "history-bad.json")
		if err := os.WriteFile(path, []byte(`{"k": [INVALID`), 0o600); err != nil {
			t.Fatal(err)
		}
		h, err := NewHistory(path, 100)
		if err == nil {
			if h != nil {
				_ = h.Close()
			}
			t.Fatal("NewHistory com JSON corrompido deveria retornar erro")
		}
		if !strings.Contains(err.Error(), "corrompido") {
			t.Fatalf("erro de corrompido mudou: %v", err)
		}
	})
}
