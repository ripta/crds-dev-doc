package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	yaml "gopkg.in/yaml.v3"
)

const minimalCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
spec:
  group: example.com
  names:
    kind: Widget
    plural: widgets
  scope: Namespaced
`

func TestSplitYAML(t *testing.T) {
	t.Run("single CRD document", func(t *testing.T) {
		got, err := splitYAML([]byte(minimalCRD), "crd.yaml")
		if err != nil {
			t.Fatalf("splitYAML() error = %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("splitYAML() returned %d docs, want 1", len(got))
		}
		assertKind(t, got[0], "CustomResourceDefinition")
	})

	t.Run("multiple CRD documents", func(t *testing.T) {
		in := minimalCRD + "---\n" + strings.Replace(minimalCRD, "Widget", "Gadget", 1)
		got, err := splitYAML([]byte(in), "crds.yaml")
		if err != nil {
			t.Fatalf("splitYAML() error = %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("splitYAML() returned %d docs, want 2", len(got))
		}
	})

	t.Run("leading document separator", func(t *testing.T) {
		got, err := splitYAML([]byte("---\n"+minimalCRD), "crd.yaml")
		if err != nil {
			t.Fatalf("splitYAML() error = %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("splitYAML() returned %d docs, want 1", len(got))
		}
	})

	t.Run("non-CRD documents are skipped", func(t *testing.T) {
		in := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n---\n" + minimalCRD
		got, err := splitYAML([]byte(in), "mixed.yaml")
		if err != nil {
			t.Fatalf("splitYAML() error = %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("splitYAML() returned %d docs, want 1 (the ConfigMap must be dropped)", len(got))
		}
		assertKind(t, got[0], "CustomResourceDefinition")
	})

	t.Run("document without a kind is skipped", func(t *testing.T) {
		in := "foo: bar\n---\n" + minimalCRD
		got, err := splitYAML([]byte(in), "nokind.yaml")
		if err != nil {
			t.Fatalf("splitYAML() error = %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("splitYAML() returned %d docs, want 1", len(got))
		}
	})

	t.Run("non-string kind is skipped", func(t *testing.T) {
		got, err := splitYAML([]byte("kind: 42\n"), "badkind.yaml")
		if err != nil {
			t.Fatalf("splitYAML() error = %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("splitYAML() returned %d docs, want 0", len(got))
		}
	})

	t.Run("empty input yields no documents", func(t *testing.T) {
		got, err := splitYAML(nil, "empty.yaml")
		if err != nil {
			t.Fatalf("splitYAML() error = %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("splitYAML() returned %d docs, want 0", len(got))
		}
	})

	// Each failed Decode bumps errCount; past 10 the function gives up.
	t.Run("malformed yaml errors out rather than looping", func(t *testing.T) {
		got, err := splitYAML([]byte("\tthis: is not: valid: yaml\n["), "broken.yaml")
		if err == nil {
			t.Fatalf("splitYAML() error = nil, want an error; got %d docs", len(got))
		}
		if !strings.Contains(err.Error(), "broken.yaml") {
			t.Errorf("splitYAML() error = %q, want it to name the file", err)
		}
	})
}

// splitYAML re-encodes rather than slicing the input, so formatting and key
// order are not preserved.
func TestSplitYAMLReencodes(t *testing.T) {
	got, err := splitYAML([]byte(minimalCRD), "crd.yaml")
	if err != nil {
		t.Fatalf("splitYAML() error = %v", err)
	}

	var out map[string]any
	if err := yaml.Unmarshal(got[0], &out); err != nil {
		t.Fatalf("re-encoded doc is not valid yaml: %v", err)
	}
	spec, ok := out["spec"].(map[string]any)
	if !ok {
		t.Fatalf("re-encoded doc lost its spec: %#v", out)
	}
	if spec["group"] != "example.com" {
		t.Errorf("spec.group = %v, want example.com", spec["group"])
	}
}

// panickingDecoder stands in for yaml.Decoder to reach splitYAML's recover.
type panickingDecoder struct{}

func (panickingDecoder) Decode(any) error { panic("boom") }

// Regression: with unnamed results, a recovered panic returned (nil, nil),
// which getYAMLs read as "no CRDs in this file".
func TestSplitYAMLPanicReturnsError(t *testing.T) {
	orig := newYAMLDecoder
	t.Cleanup(func() { newYAMLDecoder = orig })
	newYAMLDecoder = func(io.Reader) yamlDecoder { return panickingDecoder{} }

	yamls, err := splitYAML([]byte(minimalCRD), "panic.yaml")

	if err == nil {
		t.Fatal("splitYAML() error = nil after a panic, want a non-nil error")
	}
	if !strings.Contains(err.Error(), "panic while processing yaml file") {
		t.Errorf("splitYAML() error = %q, want it to mention the panic", err)
	}
	if !strings.Contains(err.Error(), "panic.yaml") {
		t.Errorf("splitYAML() error = %q, want it to name the file", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("splitYAML() error = %q, want it to carry the panic value", err)
	}
	if yamls != nil {
		t.Errorf("splitYAML() docs = %v, want nil after a panic", yamls)
	}
}

func TestGetYAMLsSkipsPanickingFile(t *testing.T) {
	orig := newYAMLDecoder
	t.Cleanup(func() { newYAMLDecoder = orig })
	newYAMLDecoder = func(io.Reader) yamlDecoder { return panickingDecoder{} }

	dir := t.TempDir()
	writeFile(t, dir, "crd.yaml", minimalCRD)

	got := getYAMLs([]git.GrepResult{{FileName: "crd.yaml"}}, dir)

	if _, ok := got["crd.yaml"]; ok {
		t.Errorf("getYAMLs() = %v, want no entry for a file that panicked", got)
	}
}

func TestGetYAMLs(t *testing.T) {
	t.Run("reads and splits matched files", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "crd.yaml", minimalCRD)

		got := getYAMLs([]git.GrepResult{{FileName: "crd.yaml"}}, dir)

		docs, ok := got["crd.yaml"]
		if !ok {
			t.Fatalf("getYAMLs() = %v, want an entry for crd.yaml", got)
		}
		if len(docs) != 1 {
			t.Errorf("getYAMLs() returned %d docs for crd.yaml, want 1", len(docs))
		}
	})

	// Valid YAML, but past maxFileSize so it is never parsed.
	t.Run("oversized files produce an empty entry", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "big.yaml", minimalCRD+"# "+strings.Repeat("x", maxFileSize))

		got := getYAMLs([]git.GrepResult{{FileName: "big.yaml"}}, dir)

		docs, ok := got["big.yaml"]
		if !ok {
			t.Fatal("getYAMLs() dropped the oversized file entirely, want an empty entry")
		}
		if len(docs) != 0 {
			t.Errorf("getYAMLs() returned %d docs for an oversized file, want 0", len(docs))
		}
	})

	t.Run("missing files are skipped", func(t *testing.T) {
		got := getYAMLs([]git.GrepResult{{FileName: "gone.yaml"}}, t.TempDir())

		if _, ok := got["gone.yaml"]; ok {
			t.Errorf("getYAMLs() = %v, want no entry for a missing file", got)
		}
	})

	t.Run("unparseable files are skipped", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "broken.yaml", "\tbad: yaml: here\n[")

		got := getYAMLs([]git.GrepResult{{FileName: "broken.yaml"}}, dir)

		if _, ok := got["broken.yaml"]; ok {
			t.Errorf("getYAMLs() = %v, want no entry for an unparseable file", got)
		}
	})

	t.Run("handles several files at once", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "a.yaml", minimalCRD)
		writeFile(t, dir, "nested/b.yaml", minimalCRD)

		got := getYAMLs([]git.GrepResult{
			{FileName: "a.yaml"},
			{FileName: "nested/b.yaml"},
			{FileName: "missing.yaml"},
		}, dir)

		if len(got) != 2 {
			t.Errorf("getYAMLs() returned %d entries, want 2", len(got))
		}
	})
}

func assertKind(t *testing.T, doc []byte, want string) {
	t.Helper()

	var out map[string]any
	if err := yaml.Unmarshal(doc, &out); err != nil {
		t.Fatalf("doc is not valid yaml: %v", err)
	}
	if got := out["kind"]; got != want {
		t.Errorf("doc kind = %v, want %v", got, want)
	}
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()

	full := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("failed to create dir for %s: %v", name, err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write %s: %v", name, err)
	}
}
