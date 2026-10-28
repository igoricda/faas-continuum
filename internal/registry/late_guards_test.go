package registry

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestLateGuards(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	fn, _, err := r.Register("test", "f-default", "img:1.0", RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatal(err)
	}
	if fn.LateReportable {
		t.Fatal("LateReportable default = true, esperado false (opt-in)")
	}

	fnOpt, _, err := r.Register("test", "f-opt", "img:1.0", RegisterOptions{MinInstances: 1, LateReportable: true})
	if err != nil {
		t.Fatal(err)
	}
	if !fnOpt.LateReportable {
		t.Fatal("Register com LateReportable:true não aplicou o flag")
	}

	updated, _, err := r.Update("test", "f-default", "img:1.0", RegisterOptions{MinInstances: 1, LateReportable: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.LateReportable {
		t.Fatal("Update com LateReportable:true não aplicou o flag")
	}
	got, ok := r.Get("f-default")
	if !ok || !got.LateReportable {
		t.Fatal("Get após Update não reflete LateReportable:true")
	}

	raw, err := json.Marshal(fnOpt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"late_reportable":true`) {
		t.Fatalf("JSON sem late_reportable:true, obtido %s", string(raw))
	}
}
