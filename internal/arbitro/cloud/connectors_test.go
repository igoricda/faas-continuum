package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Config por função válida (ADR-0011, item 3): dois endpoints distintos,
// anônimos (auth_scheme none, sem token — AuthLevel.ANONYMOUS).
func validPerFunctionConfig(urlA, urlB string) CloudFallbackConfig {
	return CloudFallbackConfig{
		Connector: ConnectorAzureFunctions,
		Functions: map[string]FunctionEndpoint{
			"alpr-deteccao-placa": {EndpointURL: urlA, InputContentType: "application/json", AuthScheme: "none"},
			"ocr":                 {EndpointURL: urlB, InputContentType: "application/json", AuthScheme: "none"},
		},
	}
}

func mustSet(t *testing.T, s *CloudStore, cfg CloudFallbackConfig) {
	t.Helper()
	if err := s.Set(cfg); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func newMemStoreWith(t *testing.T, cfg CloudFallbackConfig) *CloudStore {
	t.Helper()
	s, err := NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	mustSet(t, s, cfg)
	return s
}

// Validação do formato por função (ADR-0011, item 3): mesma regra por
// entrada do mapa; misturas sem connector são rejeitadas (fail-closed).
func TestCloudConfig_PerFunctionValidation(t *testing.T) {
	good := func() FunctionEndpoint {
		return FunctionEndpoint{EndpointURL: "https://example.com/api/f", InputContentType: "application/json", AuthScheme: "none"}
	}
	cases := []struct {
		name    string
		cfg     CloudFallbackConfig
		wantErr bool
	}{
		{"válido por função", CloudFallbackConfig{Connector: ConnectorAzureFunctions, Functions: map[string]FunctionEndpoint{"a": good(), "b": good()}}, false},
		{"connector desconhecido", CloudFallbackConfig{Connector: "lambda", Functions: map[string]FunctionEndpoint{"a": good()}}, true},
		{"azure sem functions", CloudFallbackConfig{Connector: ConnectorAzureFunctions}, true},
		{"azure com mapa vazio", CloudFallbackConfig{Connector: ConnectorAzureFunctions, Functions: map[string]FunctionEndpoint{}}, true},
		{"nome de função vazio", CloudFallbackConfig{Connector: ConnectorAzureFunctions, Functions: map[string]FunctionEndpoint{"": good()}}, true},
		{"endpoint ruim numa entrada", CloudFallbackConfig{Connector: ConnectorAzureFunctions, Functions: map[string]FunctionEndpoint{"a": {EndpointURL: "ftp://x", InputContentType: "application/json", AuthScheme: "none"}}}, true},
		{"mapa sem connector", CloudFallbackConfig{Functions: map[string]FunctionEndpoint{"a": good()}}, true},
		{"generic-http com mapa", CloudFallbackConfig{Connector: ConnectorGenericHTTP, Functions: map[string]FunctionEndpoint{"a": good()}, EndpointURL: "https://example.com/i", InputContentType: "application/json", AuthScheme: "none"}, true},
		{"generic-http plano explícito", CloudFallbackConfig{Connector: ConnectorGenericHTTP, EndpointURL: "https://example.com/i", InputContentType: "application/json", AuthScheme: "none"}, false},
		{"plano legado intacto", CloudFallbackConfig{EndpointURL: "https://example.com/i", InputContentType: "application/json", AuthScheme: "Bearer"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewCloudStore("")
			if err != nil {
				t.Fatalf("NewCloudStore: %v", err)
			}
			err = s.Set(tc.cfg)
			if tc.wantErr && !errors.Is(err, ErrInvalidCloudConfig) {
				t.Fatalf("Set err=%v, esperado ErrInvalidCloudConfig", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Set válido err=%v", err)
			}
			if tc.wantErr {
				if _, ok := s.Get(); ok {
					t.Fatalf("Get após Set inválido deveria ser vazio")
				}
			}
		})
	}
}

// Persistência do formato por função (ADR-0011, item 6): tmp+rename+
// 0600+fsync e JSON compacto como o legado; migração lazy no próximo Set
// (o que é Set é o que é persistido, nos dois sentidos).
func TestCloudStore_PerFunctionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cloud.json")

	s, err := NewCloudStore(path)
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	mustSet(t, s, validPerFunctionConfig("https://a.example/api/alpr", "https://b.example/api/ocr"))

	got, ok := s.Get()
	if !ok || got.Connector != ConnectorAzureFunctions || len(got.Functions) != 2 {
		t.Fatalf("Get = %+v,%v, esperado formato por função com 2 entradas", got, ok)
	}
	if got.Functions["ocr"].EndpointURL != "https://b.example/api/ocr" {
		t.Fatalf("endpoint ocr = %q", got.Functions["ocr"].EndpointURL)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perm=%o, esperado 0600", perm)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), `"connector":"azure-functions"`) {
		t.Fatalf("arquivo sem connector: %s", data)
	}
	for i := 0; i+2 < len(data); i++ {
		if data[i] == '\n' && data[i+1] == ' ' && data[i+2] == ' ' {
			t.Fatalf("arquivo com MarshalIndent, esperado Marshal compacto")
		}
	}

	// Reabrir carrega o formato novo (load valida igual ao Set).
	s2, err := NewCloudStore(path)
	if err != nil {
		t.Fatalf("reabrir: %v", err)
	}
	if _, ok := s2.Get(); !ok {
		t.Fatalf("Get após reabrir = false")
	}

	// Migração lazy reversa: Set legado sobrescreve o arquivo no formato plano.
	mustSet(t, s2, CloudFallbackConfig{EndpointURL: "https://x.example/i", InputContentType: "application/json", AuthScheme: "none"})
	raw, _ := os.ReadFile(path)
	if strings.Contains(string(raw), `"connector"`) {
		t.Fatalf("arquivo ainda com connector após Set legado: %s", raw)
	}
	got2, _ := s2.Get()
	if got2.EndpointURL != "https://x.example/i" || got2.Functions != nil {
		t.Fatalf("Get após migração reversa = %+v", got2)
	}
}

// Arquivo legado em disco continua lido (ADR-0011, item 5).
func TestCloudStore_LoadLegacyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cloud.json")
	legacy := `{"endpoint_url":"https://nuvem.example.com/invoke","input_content_type":"application/json","auth_scheme":"Bearer","auth_token":"tok"}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	s, err := NewCloudStore(path)
	if err != nil {
		t.Fatalf("NewCloudStore(legado): %v", err)
	}
	got, ok := s.Get()
	if !ok || got.EndpointURL != "https://nuvem.example.com/invoke" || got.AuthToken != "tok" {
		t.Fatalf("Get legado = %+v,%v", got, ok)
	}
}

// Sucesso do AzureFunctionsConnector (ADR-0011, itens 2-3): POST do input
// CRU (sem envelope), Content-Type configurado, sem Authorization quando
// anônimo; corpo 2xx devolvido bruto.
func TestAzureFunctionsConnector_Success(t *testing.T) {
	var gotMethod, gotCT, gotAuth, gotBody, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotCT = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Client-Id")
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","plate":"ABC1D22","region":"Brazil"}`))
	}))
	defer srv.Close()

	s := newMemStoreWith(t, CloudFallbackConfig{
		Connector: ConnectorAzureFunctions,
		Functions: map[string]FunctionEndpoint{
			"ocr": {EndpointURL: srv.URL, InputContentType: "application/json", AuthScheme: "none",
				RequiredHeaders: []string{"X-Client-Id"}, HeaderValues: map[string]string{"X-Client-Id": "abc"}},
		},
	})
	c := NewAzureFunctionsConnector(s, nil)
	if !c.Configured() {
		t.Fatalf("Configured = false com config por função")
	}
	body, err := c.Invoke(context.Background(), "ocr", json.RawMessage(`{"image_base64":"xyz"}`))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(body) != `{"status":"success","plate":"ABC1D22","region":"Brazil"}` {
		t.Fatalf("corpo não verbatim: %s", body)
	}
	if gotMethod != http.MethodPost || gotCT != "application/json" {
		t.Fatalf("method=%q ct=%q", gotMethod, gotCT)
	}
	if gotBody != `{"image_base64":"xyz"}` {
		t.Fatalf("input com envelope ou alterado: %q", gotBody)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization com token omitido deveria ser vazio: %q", gotAuth)
	}
	if gotCustom != "abc" {
		t.Fatalf("header custom = %q, esperado abc", gotCustom)
	}
}

// Auth e headers (ADR-0011, item 2): Authorization só com token;
// required sem valor não é enviado.
func TestAzureFunctionsConnector_Auth(t *testing.T) {
	var gotAuth string
	var gotMissing string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotMissing = r.Header.Get("X-Missing")
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	s := newMemStoreWith(t, CloudFallbackConfig{
		Connector: ConnectorAzureFunctions,
		Functions: map[string]FunctionEndpoint{
			"f": {EndpointURL: srv.URL, InputContentType: "application/json", AuthScheme: "Bearer", AuthToken: "segredo",
				RequiredHeaders: []string{"X-Missing"}, HeaderValues: map[string]string{}},
		},
	})
	c := NewAzureFunctionsConnector(s, nil)
	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if gotAuth != "Bearer segredo" {
		t.Fatalf("Authorization = %q, esperado 'Bearer segredo'", gotAuth)
	}
	if gotMissing != "" {
		t.Fatalf("header sem valor deveria ser omitido: %q", gotMissing)
	}
}

// Roteamento por função (ADR-0011, item 2): cada função no seu endpoint;
// função sem entrada => ErrFunctionNotMapped.
func TestAzureFunctionsConnector_Routing(t *testing.T) {
	mk := func(tag string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(tag))
		}))
	}
	srvA, srvB := mk("A"), mk("B")
	defer srvA.Close()
	defer srvB.Close()

	s := newMemStoreWith(t, validPerFunctionConfig(srvA.URL, srvB.URL))
	c := NewAzureFunctionsConnector(s, nil)
	for fn, want := range map[string]string{"alpr-deteccao-placa": "A", "ocr": "B"} {
		body, err := c.Invoke(context.Background(), fn, json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("Invoke(%s): %v", fn, err)
		}
		if string(body) != want {
			t.Fatalf("Invoke(%s) = %q, esperado %q (endpoint errado)", fn, body, want)
		}
	}
	if _, err := c.Invoke(context.Background(), "inexistente", json.RawMessage(`{}`)); !errors.Is(err, ErrFunctionNotMapped) {
		t.Fatalf("Invoke sem mapeamento err=%v, esperado ErrFunctionNotMapped", err)
	}
}

// Falhas de transporte e tamanho (ADR-0011, item 2): não-2xx e corpo
// >8 MiB viram erro, como no Forward.
func TestAzureFunctionsConnector_TransportFailures(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("cloud down"))
	}))
	defer bad.Close()
	big := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, (8<<20)+1))
	}))
	defer big.Close()

	s := newMemStoreWith(t, validPerFunctionConfig(bad.URL, big.URL))
	c := NewAzureFunctionsConnector(s, nil)
	if _, err := c.Invoke(context.Background(), "alpr-deteccao-placa", json.RawMessage(`{}`)); err == nil {
		t.Fatalf("não-2xx deveria falhar")
	}
	if _, err := c.Invoke(context.Background(), "ocr", json.RawMessage(`{}`)); err == nil {
		t.Fatalf("corpo >8 MiB deveria falhar")
	}
}

// Tradução de erro-200 (ADR-0011, item 4): {"status":"error"} vira erro
// para o gateway reportar CLOUD_FALLBACK_FAILED; desligável por conector.
func TestAzureFunctionsConnector_AppError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"error","message":"imagem inválida"}`))
	}))
	defer srv.Close()

	s := newMemStoreWith(t, validPerFunctionConfig(srv.URL, srv.URL))
	c := NewAzureFunctionsConnector(s, nil)
	_, err := c.Invoke(context.Background(), "ocr", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "imagem inválida") {
		t.Fatalf("erro-200 deveria virar erro com a mensagem: %v", err)
	}

	c.TranslateAppError = false
	if _, err := c.Invoke(context.Background(), "ocr", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("com tradução desligada, erro-200 passa como sucesso: %v", err)
	}
}

// Corpo 2xx não-JSON ou sem status error segue como sucesso (o gateway
// embrulha não-JSON como string, como no Forward).
func TestAzureFunctionsConnector_NonJSONSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`texto cru`))
	}))
	defer srv.Close()

	s := newMemStoreWith(t, validPerFunctionConfig(srv.URL, srv.URL))
	c := NewAzureFunctionsConnector(s, nil)
	body, err := c.Invoke(context.Background(), "ocr", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(body) != "texto cru" {
		t.Fatalf("corpo = %q", body)
	}
}

// Paridade do GenericHTTPConnector com Forward (ADR-0011, item 5):
// mesmo input bruto, headers, auth, 2xx bruto; erro-200 do app PASSA
// como sucesso (sem tradução, como o Forward atual).
func TestGenericHTTPConnector_Parity(t *testing.T) {
	var gotBody, gotCT, gotAuth, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Client-Id")
		raw := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(raw)
		gotBody = string(raw)
		_, _ = w.Write([]byte(`{"status":"error","message":"do app"}`))
	}))
	defer srv.Close()

	s := newMemStoreWith(t, CloudFallbackConfig{
		EndpointURL: srv.URL, InputContentType: "application/json", AuthScheme: "Bearer", AuthToken: "tok123",
		RequiredHeaders: []string{"X-Client-Id"}, HeaderValues: map[string]string{"X-Client-Id": "abc"},
	})
	c := NewGenericHTTPConnector(s, nil)
	if !c.Configured() {
		t.Fatalf("Configured = false com config legada")
	}
	body, err := c.Invoke(context.Background(), "qualquer-fn", json.RawMessage(`{"image_base64":"xyz"}`))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(body) != `{"status":"error","message":"do app"}` {
		t.Fatalf("erro-200 deveria passar como sucesso no genérico: %s", body)
	}
	if gotBody != `{"image_base64":"xyz"}` || gotCT != "application/json" || gotAuth != "Bearer tok123" || gotCustom != "abc" {
		t.Fatalf("paridade quebrada: body=%q ct=%q auth=%q custom=%q", gotBody, gotCT, gotAuth, gotCustom)
	}

	// Com formato por função vigente, o genérico recusa (despacho é do Connector).
	mustSet(t, s, validPerFunctionConfig(srv.URL, srv.URL))
	if c.Configured() {
		t.Fatalf("Configured genérico com formato por função deveria ser false")
	}
	if _, err := c.Invoke(context.Background(), "ocr", json.RawMessage(`{}`)); err == nil {
		t.Fatalf("genérico com formato por função deveria falhar")
	}
}

// Despacho por formato (ADR-0011, item 6): o Connector lê o vigente a
// cada Invoke — PUT troca o caminho sem rewiring.
func TestConnector_Dispatch(t *testing.T) {
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("legacy"))
	}))
	defer legacy.Close()
	perFn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	defer perFn.Close()

	s, err := NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	c := NewConnector(s)
	if c.Configured() {
		t.Fatalf("Configured sem config deveria ser false")
	}
	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("Invoke sem config err=%v, esperado ErrNotConfigured", err)
	}

	mustSet(t, s, CloudFallbackConfig{EndpointURL: legacy.URL, InputContentType: "application/json", AuthScheme: "none"})
	if !c.Configured() {
		t.Fatalf("Configured legado = false")
	}
	body, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`))
	if err != nil || string(body) != "legacy" {
		t.Fatalf("caminho legado = %q,%v", body, err)
	}

	mustSet(t, s, validPerFunctionConfig(perFn.URL, perFn.URL))
	body, err = c.Invoke(context.Background(), "ocr", json.RawMessage(`{}`))
	if err != nil || string(body) != `{"status":"success"}` {
		t.Fatalf("caminho por função = %q,%v", body, err)
	}

	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if c.Configured() {
		t.Fatalf("Configured após Clear deveria ser false")
	}
}
