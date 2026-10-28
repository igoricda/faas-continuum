package auth

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"
)

// Shared helpers.

// capturarLog redirects the log package output to a buffer and restores the
// original writer when the test ends.
//
// It mutates global state (log.SetOutput), so no test in this package may call
// t.Parallel: concurrent captures would interleave and corrupt each other.
func capturarLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	anterior := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(anterior) })
	return &buf
}

// credencialParaHash returns a valid credential whose plaintext token is
// token; the Argon2id hash is actually derived.
func credencialParaHash(t *testing.T, token string) Credential {
	t.Helper()
	hash, err := HashToken(token)
	if err != nil {
		t.Fatalf("HashToken() erro inesperado: %v", err)
	}
	return Credential{HeaderName: DefaultHeaderName, TokenHash: hash}
}

// handlerSentinela returns a handler that records the call; if it runs, the
// request passed the middleware.
func handlerSentinela(chamado *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*chamado = true
		w.WriteHeader(http.StatusOK)
	})
}

// requisicaoProtegida builds a request against the management route with
// RemoteAddr=ip and the credential header set to valor (empty means the header
// is absent).
func requisicaoProtegida(ip, valor string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/function/minha-funcao", nil)
	req.RemoteAddr = ip
	if valor != "" {
		req.Header.Set(DefaultHeaderName, valor)
	}
	return req
}

// truncar shortens long strings for readable error messages.
func truncar(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Credential generation, persistence and loading.

func TestGenerateProducesDistinctTokensAndHashes(t *testing.T) {
	g1, err := Generate()
	if err != nil {
		t.Fatalf("Generate() erro inesperado: %v", err)
	}
	g2, err := Generate()
	if err != nil {
		t.Fatalf("Generate() erro inesperado: %v", err)
	}

	if g1.PlainToken == g2.PlainToken {
		t.Error("dois Generate() sucessivos produziram o mesmo token em texto plano")
	}
	if g1.Cred.TokenHash == g2.Cred.TokenHash {
		t.Error("dois Generate() sucessivos produziram o mesmo token_hash (salt aleatório esperado)")
	}
	if len(g1.PlainToken) != plainTokenBytes*2 { // hex: 2 chars per byte
		t.Errorf("len(PlainToken) = %d, esperado %d", len(g1.PlainToken), plainTokenBytes*2)
	}
	if g1.Cred.HeaderName != DefaultHeaderName {
		t.Errorf("HeaderName = %q, esperado %q", g1.Cred.HeaderName, DefaultHeaderName)
	}
}

func TestPlainTokenVerifiesAgainstStoredHash(t *testing.T) {
	g, err := Generate()
	if err != nil {
		t.Fatalf("Generate() erro inesperado: %v", err)
	}

	if !VerifyToken(g.PlainToken, g.Cred.TokenHash) {
		t.Error("PlainToken gerado não verifica contra Cred.TokenHash")
	}
	if VerifyToken("token-errado", g.Cred.TokenHash) {
		t.Error("token errado verificou contra o token_hash armazenado")
	}
}

func TestSaveAndLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.json")

	g, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveToFile(path, g.Cred); err != nil {
		t.Fatalf("SaveToFile() erro inesperado: %v", err)
	}

	got, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() erro inesperado: %v", err)
	}
	if got != g.Cred {
		t.Errorf("credencial lida = %+v, esperado %+v", got, g.Cred)
	}
}

func TestLoadFromFileRejectsPlainTokenField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.json")
	rejeitado := `{"header_name":"Authorization","token":"x"}`
	if err := os.WriteFile(path, []byte(rejeitado), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadFromFile(path)
	if err == nil {
		t.Fatal("LoadFromFile() aceitou arquivo com campo \"token\" em texto plano; esperava erro fail-closed")
	}
	if !errors.Is(err, errFormatoInvalido) {
		t.Errorf("erro sem a mensagem acionável de reprovisionamento:\n obtido: %v\n esperado errFormatoInvalido via errors.Is", err)
	}

	// A rejected file must not be rewritten or removed by the load attempt;
	// the operator decides what to do with it.
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("arquivo rejeitado foi removido pela tentativa de carga: %v", readErr)
	}
	if string(data) != rejeitado {
		t.Errorf("arquivo rejeitado foi reescrito;\n antes: %s\n depois: %s", rejeitado, data)
	}
}

func TestLoadFromFileRejectsCorruptJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.json")
	if err := os.WriteFile(path, []byte("{isto não é json"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadFromFile(path)
	if err == nil {
		t.Fatal("LoadFromFile() aceitou JSON corrompido; esperava erro")
	}
	if !errors.Is(err, errFormatoInvalido) {
		t.Errorf("erro sem a mensagem acionável de reprovisionamento: %v", err)
	}
}

func TestLoadFromFileRejectsMissingOrEmptyTokenHash(t *testing.T) {
	cases := []struct {
		name     string
		conteudo string
	}{
		{"sem_token_hash", `{"header_name":"Authorization"}`},
		{"token_hash_vazio", `{"header_name":"Authorization","token_hash":""}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credential.json")
			if err := os.WriteFile(path, []byte(tc.conteudo), 0o600); err != nil {
				t.Fatal(err)
			}

			_, err := LoadFromFile(path)
			if err == nil {
				t.Fatalf("LoadFromFile() aceitou %q; esperava erro", tc.name)
			}
			if !errors.Is(err, errFormatoInvalido) {
				t.Errorf("erro sem a mensagem acionável de reprovisionamento: %v", err)
			}
		})
	}
}

func TestLoadOrGenerateCreatesOnFirstRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.json")

	first, plainFirst, err := LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("LoadOrGenerate() (1ª chamada) erro inesperado: %v", err)
	}
	if plainFirst == "" {
		t.Error("1ª chamada de LoadOrGenerate() retornou plainToken vazio; esperava show-once do provisionamento")
	}
	if !VerifyToken(plainFirst, first.TokenHash) {
		t.Error("plainToken da 1ª chamada não verifica contra o token_hash persistido")
	}

	second, plainSecond, err := LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("LoadOrGenerate() (2ª chamada) erro inesperado: %v", err)
	}
	if plainSecond != "" {
		t.Errorf("2ª chamada de LoadOrGenerate() retornou plainToken %q; esperava vazio (token só é mostrado uma vez)", plainSecond)
	}
	if second != first {
		t.Error("LoadOrGenerate() gerou uma credencial nova na 2ª chamada, esperava reaproveitar a persistida")
	}
}

func TestSaveToFileLeavesNoTempBehind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credential.json")

	g, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveToFile(path, g.Cred); err != nil {
		t.Fatalf("SaveToFile() erro inesperado: %v", err)
	}

	sobras, err := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sobras) != 0 {
		t.Errorf("SaveToFile() deixou arquivos temporários para trás: %v", sobras)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("arquivo final da credencial não existe após SaveToFile(): %v", err)
	}
}

func TestPersistedBytesContainOnlyTokenHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.json")

	g, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveToFile(path, g.Cred); err != nil {
		t.Fatalf("SaveToFile() erro inesperado: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"header_name"`)) {
		t.Error("bytes persistidos não contêm a chave header_name")
	}
	if !bytes.Contains(data, []byte(`"token_hash"`)) {
		t.Error("bytes persistidos não contêm a chave token_hash")
	}
	if bytes.Contains(data, []byte(`"token":`)) {
		t.Errorf("bytes persistidos contêm a chave legada \"token\":\n%s", data)
	}
	if bytes.Contains(data, []byte(g.PlainToken)) {
		t.Error("bytes persistidos contêm o token em texto plano — vazamento do segredo em repouso")
	}
}

// TestRateLimiterLimitOne checks the allow, deny (transition), deny (no
// transition) sequence with a limiter initialized to limit=1.
func TestRateLimiterLimitOne(t *testing.T) {
	agora := time.Unix(1700000000, 0)
	rl := NewRateLimiter(1, time.Minute)
	rl.now = func() time.Time { return agora }

	registrar := func(permitido, transicao bool) {
		if !permitido && transicao {
			t.Logf("limite de requisições (transicao=true)")
		}
	}

	// i=0: first request, count becomes 1, permitido=true, transicao=false
	p, tr := rl.allow("10.0.0.1")
	registrar(p, tr)
	if p != true || tr != false {
		t.Errorf("iteração 0: esperado (true, false), got (%v, %v)", p, tr)
	}

	// i=1: second request, count becomes 2 > limit=1, permitido=false,
	// transicao=true
	p, tr = rl.allow("10.0.0.1")
	registrar(p, tr)
	if p != false || tr != true {
		t.Errorf("iteração 1: esperado (false, true), got (%v, %v)", p, tr)
	}

	// i=2: third request, count becomes 3, permitido=false, transicao=false
	p, tr = rl.allow("10.0.0.1")
	registrar(p, tr)
	if p != false || tr != false {
		t.Errorf("iteração 2: esperado (false, false), got (%v, %v)", p, tr)
	}
}

// Argon2id hashing and verification.

// TestConstantesParametrosArgon2 pins the public values of the Argon2id
// parameters; any change to them must be made explicit here.
func TestConstantesParametrosArgon2(t *testing.T) {
	if DefaultMemoryKiB != 19456 {
		t.Errorf("DefaultMemoryKiB = %d, esperado 19456", DefaultMemoryKiB)
	}
	if DefaultTimeCost != 1 {
		t.Errorf("DefaultTimeCost = %d, esperado 1", DefaultTimeCost)
	}
	if DefaultParallelism != 1 {
		t.Errorf("DefaultParallelism = %d, esperado 1", DefaultParallelism)
	}
	if SaltLengthBytes != 16 {
		t.Errorf("SaltLengthBytes = %d, esperado 16", SaltLengthBytes)
	}
	if KeyLengthBytes != 32 {
		t.Errorf("KeyLengthBytes = %d, esperado 32", KeyLengthBytes)
	}
	if MaxPresentedTokenBytes != 4096 {
		t.Errorf("MaxPresentedTokenBytes = %d, esperado 4096", MaxPresentedTokenBytes)
	}
}

// TestHashTokenFormatoPHC checks the HashToken output format: the PHC string
// must follow $argon2id$v=19$m=...,t=...,p=...$<salt>$<hash>, with fields in
// base64 RawStdEncoding and the expected salt and key sizes.
func TestHashTokenFormatoPHC(t *testing.T) {
	const token = "token-de-provisionamento"

	phc, err := HashToken(token)
	if err != nil {
		t.Fatalf("HashToken() erro inesperado: %v", err)
	}

	partes := strings.Split(phc, "$")
	if len(partes) != 6 {
		t.Fatalf("PHC tem %d partes ao dividir por '$', esperado 6: %q", len(partes), phc)
	}
	if partes[0] != "" {
		t.Errorf("parte 0 = %q, esperado vazio (PHC começa com '$')", partes[0])
	}
	if partes[1] != "argon2id" {
		t.Errorf("algoritmo = %q, esperado %q", partes[1], "argon2id")
	}
	if partes[2] != "v=19" {
		t.Errorf("versão = %q, esperado %q", partes[2], "v=19")
	}
	wantParams := fmt.Sprintf("m=%d,t=%d,p=%d", DefaultMemoryKiB, DefaultTimeCost, DefaultParallelism)
	if partes[3] != wantParams {
		t.Errorf("parâmetros = %q, esperado %q (round-trip exato)", partes[3], wantParams)
	}

	salt, err := base64.RawStdEncoding.DecodeString(partes[4])
	if err != nil {
		t.Fatalf("salt não decodifica em base64 RawStdEncoding: %v", err)
	}
	if len(salt) != SaltLengthBytes {
		t.Errorf("len(salt) = %d, esperado %d", len(salt), SaltLengthBytes)
	}

	chave, err := base64.RawStdEncoding.DecodeString(partes[5])
	if err != nil {
		t.Fatalf("hash não decodifica em base64 RawStdEncoding: %v", err)
	}
	if len(chave) != KeyLengthBytes {
		t.Errorf("len(hash) = %d, esperado %d", len(chave), KeyLengthBytes)
	}
}

// TestHashTokenSaltsAleatorios checks that two hashes of the same token differ,
// since each salt comes from crypto/rand; this defeats precomputed tables.
func TestHashTokenSaltsAleatorios(t *testing.T) {
	phc1, err := HashToken("mesmo-token")
	if err != nil {
		t.Fatalf("HashToken() (1ª chamada) erro inesperado: %v", err)
	}
	phc2, err := HashToken("mesmo-token")
	if err != nil {
		t.Fatalf("HashToken() (2ª chamada) erro inesperado: %v", err)
	}
	if phc1 == phc2 {
		t.Error("dois HashToken() do mesmo token produziram PHC idêntico; salts deveriam ser aleatórios")
	}
}

// TestHashTokenRejeitaVazio covers the only documented error of HashToken: an
// empty token.
func TestHashTokenRejeitaVazio(t *testing.T) {
	if _, err := HashToken(""); err == nil {
		t.Error("HashToken(\"\") deveria retornar erro para token vazio")
	}
}

// TestVerifyTokenRoundTrip is the happy path: the original token verifies
// against its own freshly generated PHC string.
func TestVerifyTokenRoundTrip(t *testing.T) {
	const token = "credencial-do-no-borda"

	phc, err := HashToken(token)
	if err != nil {
		t.Fatalf("HashToken() erro inesperado: %v", err)
	}
	if !VerifyToken(token, phc) {
		t.Error("VerifyToken(token, phc) = false para o par gerado por HashToken; esperado true")
	}
}

// TestVerifyTokenFalhas is a table of negative cases: any malformed, tampered
// or oversized input must return false without panicking.
func TestVerifyTokenFalhas(t *testing.T) {
	const token = "credencial-do-no-borda"

	phcValido, err := HashToken(token)
	if err != nil {
		t.Fatalf("HashToken() erro inesperado: %v", err)
	}

	// Derive tampered PHC strings from the valid one.
	partes := strings.Split(phcValido, "$")
	saltOriginal, _ := base64.RawStdEncoding.DecodeString(partes[4])
	hashOriginal, _ := base64.RawStdEncoding.DecodeString(partes[5])

	saltTrocado := make([]byte, len(saltOriginal))
	if _, err := rand.Read(saltTrocado); err != nil {
		t.Fatal(err)
	}
	hashTrocado := make([]byte, len(hashOriginal))
	for i := range hashTrocado {
		hashTrocado[i] = hashOriginal[i] ^ 0xFF
	}

	cases := []struct {
		name   string
		token  string
		phc    string
		wantOK bool
	}{
		{"token errado", "outro-token", phcValido, false},
		{"token apresentado vazio", "", phcValido, false},
		{"token apresentado acima do limite", strings.Repeat("a", MaxPresentedTokenBytes+1), phcValido, false},
		{"salt adulterado", token, "$argon2id$v=19$" + partes[3] + "$" + base64.RawStdEncoding.EncodeToString(saltTrocado) + "$" + partes[5], false},
		{"hash adulterado", token, "$argon2id$v=19$" + partes[3] + "$" + partes[4] + "$" + base64.RawStdEncoding.EncodeToString(hashTrocado), false},
		{"phc vazio", token, "", false},
		{"phc só separadores", token, "$$", false},
		{"phc truncado sem hash", token, "$argon2id$v=19$" + partes[3] + "$" + partes[4], false},
		{"phc truncado sem salt e hash", token, "$argon2id$v=19$" + partes[3], false},
		{"phc truncado só algoritmo", token, "$argon2id", false},
		{"algoritmo errado (argon2i)", token, "$argon2i$v=19$" + partes[3] + "$" + partes[4] + "$" + partes[5], false},
		{"versão errada (v=16)", token, "$argon2id$v=16$" + partes[3] + "$" + partes[4] + "$" + partes[5], false},
		{"m=0 (IDKey panica se não validar antes)", token, "$argon2id$v=19$m=0,t=1,p=1$" + partes[4] + "$" + partes[5], false},
		{"t=0 (IDKey panica se não validar antes)", token, "$argon2id$v=19$m=19456,t=0,p=1$" + partes[4] + "$" + partes[5], false},
		{"p=0 (IDKey panica se não validar antes)", token, "$argon2id$v=19$m=19456,t=1,p=0$" + partes[4] + "$" + partes[5], false},
		{"todos parâmetros zerados", token, "$argon2id$v=19$m=0,t=0,p=0$AA$BB", false},
		{"campo de parâmetros malformado", token, "$argon2id$v=19$lixo$" + partes[4] + "$" + partes[5], false},
		{"versão malformada", token, "$argon2id$v=dezenove$" + partes[3] + "$" + partes[4] + "$" + partes[5], false},
		{"salt com base64 inválido", token, "$argon2id$v=19$" + partes[3] + "$@@@não é base64@@@$" + partes[5], false},
		{"hash com base64 inválido", token, "$argon2id$v=19$" + partes[3] + "$" + partes[4] + "$@@@não é base64@@@", false},
		{"partes demais", token, phcValido + "$extra", false},
		{"sem '$' inicial", token, strings.TrimPrefix(phcValido, "$"), false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := VerifyToken(tc.token, tc.phc) // must not panic
			if got != tc.wantOK {
				t.Errorf("VerifyToken(%q, %q...) = %v, esperado %v", tc.token, truncar(tc.phc, 40), got, tc.wantOK)
			}
		})
	}
}

// TestVerifyTokenChaveDeTamanhoDiferente checks that a stored key of a size
// different from the recomputed one returns false instead of panicking, a case
// possible with a hand-written PHC string.
func TestVerifyTokenChaveDeTamanhoDiferente(t *testing.T) {
	partes := []string{"", "argon2id", "v=19",
		fmt.Sprintf("m=%d,t=%d,p=%d", DefaultMemoryKiB, DefaultTimeCost, DefaultParallelism),
		base64.RawStdEncoding.EncodeToString(make([]byte, SaltLengthBytes)),
		base64.RawStdEncoding.EncodeToString(make([]byte, KeyLengthBytes+8)), // larger key
	}
	phc := strings.Join(partes, "$")

	if VerifyToken("qualquer-token", phc) {
		t.Error("VerifyToken retornou true com chave armazenada de tamanho diferente da recomputada")
	}
}

// TestVerifyTokenHonoraParametrosDoPHC checks that verification uses the
// parameters recorded in the PHC string and not the package defaults: a valid
// PHC with m=8192,t=2,p=1 must verify.
func TestVerifyTokenHonoraParametrosDoPHC(t *testing.T) {
	const token = "credencial-do-no-borda"

	salt := make([]byte, SaltLengthBytes)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	const (
		memoria     = uint32(8192)
		custo       = uint32(2)
		paralelismo = uint32(1)
	)
	chave := argon2.IDKey([]byte(token), salt, custo, memoria, uint8(paralelismo), KeyLengthBytes)
	phc := fmt.Sprintf("$%s$v=%d$m=%d,t=%d,p=%d$%s$%s",
		"argon2id", 19, memoria, custo, paralelismo,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(chave),
	)

	if !VerifyToken(token, phc) {
		t.Error("VerifyToken = false para PHC com parâmetros não-padrão válidos (m=8192,t=2,p=1); parâmetros do PHC deveriam ser honrados")
	}
}

// Middleware authentication and rate limiting.

// TestRequireAuthAllowsValidToken checks that a valid token reaches the handler
// and produces no log output; successful requests are silent by contract.
func TestRequireAuthAllowsValidToken(t *testing.T) {
	cred := credencialParaHash(t, "segredo")
	chamado := false
	buf := capturarLog(t)

	req := requisicaoProtegida("192.0.2.1:1234", "segredo")
	rec := httptest.NewRecorder()

	RequireAuthWithLimiter(cred, NewRateLimiter(30, time.Minute), handlerSentinela(&chamado)).ServeHTTP(rec, req)

	if !chamado {
		t.Error("handler protegido não foi chamado com token válido")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, esperado %d", rec.Code, http.StatusOK)
	}
	if buf.Len() != 0 {
		t.Errorf("requisição válida produziu log; esperava silêncio:\n%s", buf.String())
	}
}

// TestRequireAuthRejectsMissingOrWrongToken checks that a missing or wrong
// token yields 401 with the exact envelope and an audit line containing the IP
// and route.
func TestRequireAuthRejectsMissingOrWrongToken(t *testing.T) {
	cred := credencialParaHash(t, "segredo")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler protegido não deveria ser chamado sem credencial válida")
	})

	cases := []struct {
		name   string
		header string
	}{
		{"ausente", ""},
		{"errado", "token-errado"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := capturarLog(t)
			req := requisicaoProtegida("192.0.2.1:1234", tc.header)
			rec := httptest.NewRecorder()

			RequireAuthWithLimiter(cred, NewRateLimiter(30, time.Minute), next).ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, esperado %d", rec.Code, http.StatusUnauthorized)
			}
			if corpo := strings.TrimSpace(rec.Body.String()); corpo != msgUnauthorized {
				t.Errorf("corpo = %q, esperado envelope exato %q", corpo, msgUnauthorized)
			}
			linha := buf.String()
			if !strings.Contains(linha, "auth: falha de autenticação") {
				t.Errorf("401 sem linha de auditoria; log capturado:\n%s", linha)
			}
			if !strings.Contains(linha, "ip=192.0.2.1") || !strings.Contains(linha, "rota=/function/minha-funcao") {
				t.Errorf("linha de auditoria sem ip/rota esperados:\n%s", linha)
			}
		})
	}
}

// TestRequireAuthRejectsOversizedTokenFast checks that a header above
// MaxPresentedTokenBytes is rejected before hashing, with 401, an audit line,
// and the handler never called.
func TestRequireAuthRejectsOversizedTokenFast(t *testing.T) {
	cred := credencialParaHash(t, "segredo")
	chamado := false
	buf := capturarLog(t)

	enorme := strings.Repeat("x", MaxPresentedTokenBytes+1)
	req := requisicaoProtegida("192.0.2.1:1234", enorme)
	rec := httptest.NewRecorder()

	RequireAuthWithLimiter(cred, NewRateLimiter(30, time.Minute), handlerSentinela(&chamado)).ServeHTTP(rec, req)

	if chamado {
		t.Error("handler protegido foi chamado com cabeçalho oversized")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d", rec.Code, http.StatusUnauthorized)
	}
	if !strings.Contains(buf.String(), "auth: falha de autenticação") {
		t.Errorf("rejeição oversized sem linha de auditoria:\n%s", buf.String())
	}
}

// TestRequireAuthRateLimitReturns429 checks that the 31st request from one IP
// within the window gets a retryable 429 before the credential check, while a
// different IP at the same count is unaffected.
func TestRequireAuthRateLimitReturns429(t *testing.T) {
	cred := credencialParaHash(t, "segredo")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler protegido não deveria ser chamado neste teste (cabeçalho sempre ausente)")
	})

	const limite = 30
	handler := RequireAuthWithLimiter(cred, NewRateLimiter(30, time.Minute), next)
	codigos := make([]int, 0, limite+1)
	for i := 0; i <= limite; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, requisicaoProtegida("192.0.2.1:1234", ""))
		codigos = append(codigos, rec.Code)
	}

	for i, codigo := range codigos[:limite] {
		if codigo != http.StatusUnauthorized {
			t.Errorf("requisição %d: status = %d, esperado %d (dentro do limite)", i+1, codigo, http.StatusUnauthorized)
		}
	}
	if codigos[limite] != http.StatusTooManyRequests {
		t.Fatalf("requisição %d: status = %d, esperado %d", limite+1, codigos[limite], http.StatusTooManyRequests)
	}

	// Body of the 429: retryable envelope, same shape as the 401.
	rec429 := httptest.NewRecorder()
	handler.ServeHTTP(rec429, requisicaoProtegida("192.0.2.1:1234", ""))
	if corpo := strings.TrimSpace(rec429.Body.String()); corpo != msgTooManyReqs {
		t.Errorf("corpo do 429 = %q, esperado envelope exato %q", corpo, msgTooManyReqs)
	}
	if !strings.Contains(rec429.Body.String(), "TOO_MANY_REQUESTS") || !strings.Contains(rec429.Body.String(), `"retryable":true`) {
		t.Errorf("corpo do 429 sem TOO_MANY_REQUESTS/retryable:true:\n%s", rec429.Body.String())
	}

	// A different IP with the same count: its first request must not be 429
	recOutro := httptest.NewRecorder()
	handler.ServeHTTP(recOutro, requisicaoProtegida("203.0.113.7:9999", ""))
	if recOutro.Code == http.StatusTooManyRequests {
		t.Error("IP diferente recebeu 429 na primeira requisição; limites deveriam ser independentes por IP")
	}
	if recOutro.Code != http.StatusUnauthorized {
		t.Errorf("IP diferente: status = %d, esperado %d (falha de credencial, não de throttle)", recOutro.Code, http.StatusUnauthorized)
	}
}

// TestRequireAuthThrottleLoggedOncePerBurst checks that a denied burst logs the
// throttle line exactly once (the allow to deny transition), while
// authentication failures are logged once per request accepted by the limiter
// and not at all after throttling starts.
func TestRequireAuthThrottleLoggedOncePerBurst(t *testing.T) {
	cred := credencialParaHash(t, "segredo")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	buf := capturarLog(t)

	const total = 35
	handler := RequireAuthWithLimiter(cred, NewRateLimiter(30, time.Minute), next)
	negados429 := 0
	for i := 0; i < total; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, requisicaoProtegida("192.0.2.1:1234", ""))
		if rec.Code == http.StatusTooManyRequests {
			negados429++
		}
	}

	logTexto := buf.String()
	if got := strings.Count(logTexto, "limite de requisições"); got != 1 {
		t.Errorf("linhas de throttle = %d, esperado exatamente 1 (uma por transição allow→deny)\nlog:\n%s", got, logTexto)
	}
	if got := strings.Count(logTexto, "falha de autenticação"); got != 30 {
		t.Errorf("linhas de auditoria de auth = %d, esperado 30 (uma por requisição pré-throttle)\nlog:\n%s", got, logTexto)
	}
	if negados429 != total-30 {
		t.Errorf("respostas 429 = %d, esperado %d", negados429, total-30)
	}
}

// TestRequireAuthNeverLogsPresentedToken checks that neither the presented
// token nor the stored hash leaks into the log on any failure.
func TestRequireAuthNeverLogsPresentedToken(t *testing.T) {
	tokenApresentado := "TOKEN-SUPER-SECRETO-APRESENTADO"
	cred := credencialParaHash(t, "segredo-armazenado-diferente")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler não deveria ser chamado")
	})

	buf := capturarLog(t)

	rec := httptest.NewRecorder()
	RequireAuthWithLimiter(cred, NewRateLimiter(30, time.Minute), next).ServeHTTP(rec, requisicaoProtegida("192.0.2.1:1234", tokenApresentado))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, esperado %d", rec.Code, http.StatusUnauthorized)
	}
	logTexto := buf.String()
	if strings.Contains(logTexto, tokenApresentado) {
		t.Errorf("LOG VAZOU o token apresentado:\n%s", logTexto)
	}
	if strings.Contains(logTexto, cred.TokenHash) {
		t.Errorf("LOG VAZOU o hash armazenado:\n%s", logTexto)
	}
	if !strings.Contains(logTexto, "auth: falha de autenticação") {
		t.Errorf("esperava a linha de auditoria no log:\n%s", logTexto)
	}
}

// TestRequireAuthMalformedStoredHash checks that a malformed stored PHC never
// panics and yields a normal 401 with an audit line.
func TestRequireAuthMalformedStoredHash(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler não deveria ser chamado com token_hash malformado")
	})

	cases := []struct {
		name string
		hash string
	}{
		{"vazio", ""},
		{"lixo", "não-é-um-phc"},
		{"parametros_zerados", "$argon2id$v=19$m=0,t=0,p=0$AA$BB"},
		{"truncado", "$argon2id$v=19$m=19456,t=1,p=1$c2FsdA"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := capturarLog(t)
			cred := Credential{HeaderName: DefaultHeaderName, TokenHash: tc.hash}

			rec := httptest.NewRecorder()
			RequireAuthWithLimiter(cred, NewRateLimiter(30, time.Minute), next).ServeHTTP(rec, requisicaoProtegida("192.0.2.1:1234", "qualquer-token"))

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, esperado %d (sem panic)", rec.Code, http.StatusUnauthorized)
			}
			if !strings.Contains(buf.String(), "auth: falha de autenticação") {
				t.Errorf("falha com hash malformado sem linha de auditoria:\n%s", buf.String())
			}
		})
	}
}

// TestIPDeRemoteAddr covers IP extraction from r.RemoteAddr: the port is
// stripped by net.SplitHostPort, with a fallback to the raw string for
// unexpected formats.
func TestIPDeRemoteAddr(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		esperado   string
	}{
		{"ipv4_com_porta", "192.0.2.1:1234", "192.0.2.1"},
		{"ipv6_com_porta", "[2001:db8::1]:9090", "2001:db8::1"},
		{"sem_porta_fallback_bruto", "10.0.0.9", "10.0.0.9"},
		{"socket_unix_fallback_bruto", "@containerd.sock", "@containerd.sock"},
		{"vazio", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ipDeRemoteAddr(tc.remoteAddr); got != tc.esperado {
				t.Errorf("ipDeRemoteAddr(%q) = %q, esperado %q", tc.remoteAddr, got, tc.esperado)
			}
		})
	}
}

// TestRateLimiterBlocksThirtyFirstRequestSameIP checks the limiter unit: 30
// requests allowed, the 31st denied, and a different IP unaffected.
func TestRateLimiterBlocksThirtyFirstRequestSameIP(t *testing.T) {
	rl := NewRateLimiter(30, time.Minute)

	for i := 0; i < 30; i++ {
		if !rl.Allow("10.0.0.1") {
			t.Fatalf("requisição %d do IP A negada dentro do limite", i+1)
		}
	}
	if rl.Allow("10.0.0.1") {
		t.Error("31ª requisição do mesmo IP permitida; esperava negação")
	}
	if !rl.Allow("10.0.0.2") {
		t.Error("primeira requisição de IP diferente negada; janelas deveriam ser independentes")
	}
}

// TestRateLimiterReopensAfterWindow uses an injected clock (no sleeps) to
// exhaust a window, then advances past it and checks that a fresh window starts
// with a zero count.
func TestRateLimiterReopensAfterWindow(t *testing.T) {
	agora := time.Unix(1700000000, 0)
	rl := NewRateLimiter(30, time.Minute)
	rl.now = func() time.Time { return agora }

	for i := 0; i < 30; i++ {
		if !rl.Allow("10.0.0.1") {
			t.Fatalf("requisição %d negada dentro do limite", i+1)
		}
	}
	if rl.Allow("10.0.0.1") {
		t.Fatal("31ª requisição permitida na janela cheia")
	}

	agora = agora.Add(time.Minute + time.Second) // new window, no sleeping

	if !rl.Allow("10.0.0.1") {
		t.Error("primeira requisição após expirar a janela foi negada; esperava janela nova")
	}
	for i := 0; i < 29; i++ { // complete the new window's 30 requests
		if !rl.Allow("10.0.0.1") {
			t.Fatalf("requisição %d da janela nova negada dentro do limite", i+2)
		}
	}
	if rl.Allow("10.0.0.1") {
		t.Error("31ª requisição da janela nova permitida; contagem deveria ter reiniciado")
	}
}

// TestRateLimiterPruneRemovesOnlyExpired exercises pruning directly with
// synthetic timestamps.
func TestRateLimiterPruneRemovesOnlyExpired(t *testing.T) {
	rl := NewRateLimiter(30, time.Minute)
	base := time.Unix(1700000000, 0)

	rl.buckets["expirado-a"] = &rateBucket{inicio: base.Add(-2 * time.Minute), count: 5}
	rl.buckets["expirado-b"] = &rateBucket{inicio: base.Add(-time.Minute - time.Nanosecond), count: 7}
	rl.buckets["na-borda"] = &rateBucket{inicio: base.Add(-time.Minute), count: 3} // expired
	rl.buckets["fresco"] = &rateBucket{inicio: base.Add(-time.Second), count: 2}

	rl.pruneLocked(base)

	if _, ficou := rl.buckets["expirado-a"]; ficou {
		t.Error("bucket expirado-a sobreviveu à poda")
	}
	if _, ficou := rl.buckets["expirado-b"]; ficou {
		t.Error("bucket expirado-b sobreviveu à poda")
	}
	if _, ficou := rl.buckets["na-borda"]; ficou {
		t.Error("bucket com idade == window deveria ser considerado expirado (janela fechada)")
	}
	b, ficou := rl.buckets["fresco"]
	if !ficou {
		t.Fatal("bucket fresco foi podado indevidamente")
	}
	if b.count != 2 {
		t.Errorf("count do bucket fresco = %d, esperado 2 (poda não mexe em conteúdo)", b.count)
	}
}

// TestRateLimiterEvictsArbitraryWhenOverCap checks that with no expired buckets
// the map stays bounded at maxRateBuckets plus the current entry, never growing
// with the number of forged IPs.
func TestRateLimiterEvictsArbitraryWhenOverCap(t *testing.T) {
	agora := time.Unix(1700000000, 0)
	rl := NewRateLimiter(30, time.Minute)
	rl.now = func() time.Time { return agora } // frozen: nothing expires

	const totalIPs = maxRateBuckets + 2000
	for i := 0; i < totalIPs; i++ {
		ip := fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xff, (i>>8)&0xff, i&0xff)
		rl.Allow(ip)
	}

	if len(rl.buckets) > maxRateBuckets+1 {
		t.Errorf("mapa do limiter cresceu até %d entradas; esperado teto duro de %d (+1 corrente)", len(rl.buckets), maxRateBuckets+1)
	}
}

// TestRateLimiterDenyTransitionOncePerWindow checks that the allow to deny
// transition is reported only on the first denial of each burst: two bursts in
// separate windows produce two transitions, and further denials produce none.
func TestRateLimiterDenyTransitionOncePerWindow(t *testing.T) {
	agora := time.Unix(1700000000, 0)
	rl := NewRateLimiter(30, time.Minute)
	rl.now = func() time.Time { return agora }

	transicoes := 0
	registrar := func(permitido, transicao bool) {
		if !permitido && transicao {
			transicoes++
		}
	}

	for i := 0; i < 35; i++ { // burst 1: transition on the 31st
		p, tr := rl.allow("10.0.0.1")
		registrar(p, tr)
	}
	agora = agora.Add(time.Minute + time.Second) // window 2, no sleeping
	for i := 0; i < 35; i++ {                    // burst 2: transition on the 31st again
		p, tr := rl.allow("10.0.0.1")
		registrar(p, tr)
	}

	if transicoes != 2 {
		t.Errorf("transições allow→deny = %d, esperado 2 (uma por rajada/janela)", transicoes)
	}
}

// TestLimiterCompartilhado checks that RequireAuthWithLimiter with an injected
// limiter shares the per-IP count across routes: 20 requests on route A plus 10
// on route B fill the 30-request window, so the 31st (cross-route) gets 429.
func TestLimiterCompartilhado(t *testing.T) {
	cred := credencialParaHash(t, "segredo")
	limiter := NewRateLimiter(30, time.Minute)

	chamadoA := false
	chamadoB := false
	handlerA := RequireAuthWithLimiter(cred, limiter, handlerSentinela(&chamadoA))
	handlerB := RequireAuthWithLimiter(cred, limiter, handlerSentinela(&chamadoB))

	// 20 requests on route A (all without credentials, so 401, but counted)
	for i := 0; i < 20; i++ {
		rec := httptest.NewRecorder()
		req := requisicaoProtegida("192.0.2.1:1234", "")
		handlerA.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("rota A requisição %d: status = %d, esperado %d", i+1, rec.Code, http.StatusUnauthorized)
		}
	}

	// 10 requests on route B (still within the limit: 20+10=30)
	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		req := requisicaoProtegida("192.0.2.1:1234", "")
		handlerB.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("rota B requisição %d: status = %d, esperado %d", i+1, rec.Code, http.StatusUnauthorized)
		}
	}

	// The 31st request (cross-route, on route A) must get 429
	rec31 := httptest.NewRecorder()
	req31 := requisicaoProtegida("192.0.2.1:1234", "")
	handlerA.ServeHTTP(rec31, req31)
	if rec31.Code != http.StatusTooManyRequests {
		t.Fatalf("31ª requisição cross-rota: status = %d, esperado %d", rec31.Code, http.StatusTooManyRequests)
	}
	if !strings.Contains(rec31.Body.String(), "TOO_MANY_REQUESTS") || !strings.Contains(rec31.Body.String(), `"retryable":true`) {
		t.Errorf("corpo do 429 não tem envelope esperado: %s", rec31.Body.String())
	}

	// A different IP is unaffected
	recOutro := httptest.NewRecorder()
	reqOutro := requisicaoProtegida("203.0.113.7:9999", "")
	handlerA.ServeHTTP(recOutro, reqOutro)
	if recOutro.Code == http.StatusTooManyRequests {
		t.Error("IP diferente recebeu 429 na primeira requisição; limites deveriam ser independentes por IP")
	}
	if recOutro.Code != http.StatusUnauthorized {
		t.Errorf("IP diferente: status = %d, esperado %d (falha de credencial, não de throttle)", recOutro.Code, http.StatusUnauthorized)
	}
}
