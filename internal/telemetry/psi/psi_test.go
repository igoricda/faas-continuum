package psi

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFakeCgroup(t *testing.T, cpu, mem string) string {
	t.Helper()
	dir := t.TempDir()
	if cpu != "" {
		if err := os.WriteFile(filepath.Join(dir, "cpu.pressure"), []byte(cpu), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if mem != "" {
		if err := os.WriteFile(filepath.Join(dir, "memory.pressure"), []byte(mem), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestSupported(t *testing.T) {
	dir := writeFakeCgroup(t,
		"some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
		"some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
	)
	r := NewReader(dir)
	if !r.Supported() {
		t.Fatal("esperava Supported()==true quando cpu.pressure existe")
	}
}

func TestNotSupported(t *testing.T) {
	dir := t.TempDir() // no cpu.pressure file: simulates a kernel without CONFIG_PSI or with cgroups v1
	r := NewReader(dir)
	if r.Supported() {
		t.Fatal("esperava Supported()==false quando cpu.pressure não existe")
	}
}

func TestReadParsesValues(t *testing.T) {
	dir := writeFakeCgroup(t,
		"some avg10=5.20 avg60=3.10 avg300=1.80 total=128400\nfull avg10=1.10 avg60=0.00 avg300=0.00 total=0\n",
		"some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=2.30 avg60=0.00 avg300=0.00 total=0\n",
	)
	r := NewReader(dir)

	snap, err := r.Read()
	if err != nil {
		t.Fatalf("Read() retornou erro inesperado: %v", err)
	}

	if snap.CPUSome.Avg10 != 5.20 {
		t.Errorf("CPUSome.Avg10 = %v, esperado 5.20", snap.CPUSome.Avg10)
	}
	if snap.MemorySome.Avg10 != 0.00 {
		t.Errorf("MemorySome.Avg10 = %v, esperado 0.00", snap.MemorySome.Avg10)
	}
	if snap.MemoryFull.Avg10 != 2.30 {
		t.Errorf("MemoryFull.Avg10 = %v, esperado 2.30", snap.MemoryFull.Avg10)
	}
}

func TestReadErrorsWhenUnsupported(t *testing.T) {
	dir := t.TempDir()
	r := NewReader(dir)

	if _, err := r.Read(); err == nil {
		t.Fatal("esperava erro ao chamar Read() sem suporte a PSI")
	}
}
