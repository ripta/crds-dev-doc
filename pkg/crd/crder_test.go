package crd

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// noStorageCRD marks no version as the storage version, so no GVK can be
// derived from it.
var noStorageCRD = []byte(`
apiVersion: apiextensions.k8s.io/v1beta1
kind: CustomResourceDefinition
metadata:
  name: crontabs.example.com
spec:
  group: example.com
  scope: Namespaced
  names:
    plural: crontabs
    singular: crontab
    kind: CronTab
  versions:
  - name: v1
    served: true
    storage: false
`)

// unknownAPIVersionCRD is v1beta1-shaped but carries an apiVersion matching
// neither known group version, so NewCRDer must fall back.
var unknownAPIVersionCRD = []byte(`
apiVersion: apiextensions.k8s.io/v1alpha9
kind: CustomResourceDefinition
metadata:
  name: crontabs.example.com
spec:
  group: example.com
  scope: Namespaced
  names:
    plural: crontabs
    singular: crontab
    kind: CronTab
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          host:
            type: string
`)

func TestNewCRDer(t *testing.T) {
	tests := []struct {
		name string
		crd  []byte
		want schema.GroupVersionKind
	}{
		{
			name: "v1 uses the version marked storage",
			crd:  v1crd,
			want: schema.GroupVersionKind{Group: "example.com", Version: "v1beta1", Kind: "CronTab"},
		},
		{
			name: "v1beta1 uses the version marked storage",
			crd:  v1beta1crd,
			want: schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "CronTab"},
		},
		{
			name: "crossplane fixture",
			crd:  crossplane,
			want: schema.GroupVersionKind{Group: "cache.gcp.crossplane.io", Version: "v1alpha2", Kind: "CloudMemorystoreInstanceClass"},
		},
		{
			name: "unknown apiVersion falls back",
			crd:  unknownAPIVersionCRD,
			want: schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "CronTab"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCRDer(tt.crd)
			if err != nil {
				t.Fatalf("NewCRDer() error = %v", err)
			}
			if c.CRD == nil {
				t.Fatal("NewCRDer() returned a nil CRD")
			}
			if c.GVK == nil {
				t.Fatal("NewCRDer() returned a nil GVK")
			}
			if *c.GVK != tt.want {
				t.Errorf("NewCRDer() GVK = %v, want %v", *c.GVK, tt.want)
			}
		})
	}
}

func TestNewCRDerErrors(t *testing.T) {
	tests := []struct {
		name    string
		crd     []byte
		wantErr string
	}{
		{
			name:    "type metadata is not an object",
			crd:     []byte("- one\n- two\n"),
			wantErr: "could not unmarshal crd type metadata",
		},
		{
			name:    "no storage version",
			crd:     noStorageCRD,
			wantErr: getStoredGVKErr,
		},
		{
			name:    "not a crd at all",
			crd:     []byte("apiVersion: v1\nkind: ConfigMap\ndata:\n  a: b\n"),
			wantErr: getStoredGVKErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCRDer(tt.crd)
			if err == nil {
				t.Fatalf("NewCRDer() error = nil, want %q (got %v)", tt.wantErr, c)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("NewCRDer() error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateRejectsWrongGVK(t *testing.T) {
	c, err := NewCRDer(v1beta1crd)
	if err != nil {
		t.Fatalf("NewCRDer() error = %v", err)
	}

	instance := []byte("apiVersion: other.com/v1\nkind: NotACronTab\nhost: a\n")
	err = c.Validate(instance)

	if err == nil {
		t.Fatal("Validate() error = nil, want a GVK mismatch")
	}
	if !strings.Contains(err.Error(), wrongGVKErr) {
		t.Errorf("Validate() error = %q, want it to contain %q", err, wrongGVKErr)
	}
}

func TestValidateRejectsMalformedInstance(t *testing.T) {
	c, err := NewCRDer(v1beta1crd)
	if err != nil {
		t.Fatalf("NewCRDer() error = %v", err)
	}

	err = c.Validate([]byte("\tnot: valid: yaml\n["))

	if err == nil {
		t.Fatal("Validate() error = nil, want a conversion error")
	}
	if !strings.Contains(err.Error(), yamlToJSONErr) {
		t.Errorf("Validate() error = %q, want it to contain %q", err, yamlToJSONErr)
	}
}
