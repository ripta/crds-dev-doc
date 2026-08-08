package crd

import (
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func crdWithVersions(group, kind string, versions ...apiextensions.CustomResourceDefinitionVersion) *apiextensions.CustomResourceDefinition {
	return &apiextensions.CustomResourceDefinition{
		Spec: apiextensions.CustomResourceDefinitionSpec{
			Group:    group,
			Names:    apiextensions.CustomResourceDefinitionNames{Kind: kind},
			Versions: versions,
		},
	}
}

func TestGetStoredGVK(t *testing.T) {
	t.Run("returns the storage version", func(t *testing.T) {
		crd := crdWithVersions("example.com", "CronTab",
			apiextensions.CustomResourceDefinitionVersion{Name: "v1alpha1", Storage: false},
			apiextensions.CustomResourceDefinitionVersion{Name: "v1beta1", Storage: true},
			apiextensions.CustomResourceDefinitionVersion{Name: "v1", Storage: false},
		)

		got := GetStoredGVK(crd)
		if got == nil {
			t.Fatal("GetStoredGVK() = nil, want a GVK")
		}

		want := schema.GroupVersionKind{Group: "example.com", Version: "v1beta1", Kind: "CronTab"}
		if *got != want {
			t.Errorf("GetStoredGVK() = %v, want %v", *got, want)
		}
	})

	t.Run("nil when no version is marked storage", func(t *testing.T) {
		crd := crdWithVersions("example.com", "CronTab",
			apiextensions.CustomResourceDefinitionVersion{Name: "v1", Storage: false},
		)

		if got := GetStoredGVK(crd); got != nil {
			t.Errorf("GetStoredGVK() = %v, want nil", got)
		}
	})

	t.Run("nil when there are no versions", func(t *testing.T) {
		if got := GetStoredGVK(crdWithVersions("example.com", "CronTab")); got != nil {
			t.Errorf("GetStoredGVK() = %v, want nil", got)
		}
	})

	t.Run("returns the first storage version", func(t *testing.T) {
		crd := crdWithVersions("example.com", "CronTab",
			apiextensions.CustomResourceDefinitionVersion{Name: "v1beta1", Storage: true},
			apiextensions.CustomResourceDefinitionVersion{Name: "v1", Storage: true},
		)

		got := GetStoredGVK(crd)
		if got == nil {
			t.Fatal("GetStoredGVK() = nil, want a GVK")
		}
		if got.Version != "v1beta1" {
			t.Errorf("GetStoredGVK().Version = %q, want v1beta1", got.Version)
		}
	})
}

// PrettyGVK orders by specificity, which is group/kind/version rather than
// the group/version/kind the name might suggest.
func TestPrettyGVK(t *testing.T) {
	tests := []struct {
		name string
		gvk  schema.GroupVersionKind
		want string
	}{
		{
			name: "typical gvk",
			gvk:  schema.GroupVersionKind{Group: "example.com", Version: "v1beta1", Kind: "CronTab"},
			want: "example.com/CronTab/v1beta1",
		},
		{
			name: "empty group",
			gvk:  schema.GroupVersionKind{Version: "v1", Kind: "Pod"},
			want: "/Pod/v1",
		},
		{
			name: "zero value",
			gvk:  schema.GroupVersionKind{},
			want: "//",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PrettyGVK(&tt.gvk); got != tt.want {
				t.Errorf("PrettyGVK() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetStoredSchema(t *testing.T) {
	topLevel := &apiextensions.CustomResourceValidation{
		OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Description: "top level"},
	}
	perVersion := &apiextensions.CustomResourceValidation{
		OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Description: "per version"},
	}

	t.Run("prefers the top-level validation", func(t *testing.T) {
		spec := apiextensions.CustomResourceDefinitionSpec{
			Validation: topLevel,
			Versions: []apiextensions.CustomResourceDefinitionVersion{
				{Name: "v1", Storage: true, Schema: perVersion},
			},
		}

		got := getStoredSchema(spec)
		if got == nil || got.OpenAPIV3Schema.Description != "top level" {
			t.Errorf("getStoredSchema() = %v, want the top-level validation", got)
		}
	})

	t.Run("falls back to the storage version schema", func(t *testing.T) {
		spec := apiextensions.CustomResourceDefinitionSpec{
			Versions: []apiextensions.CustomResourceDefinitionVersion{
				{Name: "v1beta1", Storage: false, Schema: topLevel},
				{Name: "v1", Storage: true, Schema: perVersion},
			},
		}

		got := getStoredSchema(spec)
		if got == nil || got.OpenAPIV3Schema.Description != "per version" {
			t.Errorf("getStoredSchema() = %v, want the storage version schema", got)
		}
	})

	t.Run("nil when nothing is stored", func(t *testing.T) {
		spec := apiextensions.CustomResourceDefinitionSpec{
			Versions: []apiextensions.CustomResourceDefinitionVersion{
				{Name: "v1", Storage: false, Schema: perVersion},
			},
		}

		if got := getStoredSchema(spec); got != nil {
			t.Errorf("getStoredSchema() = %v, want nil", got)
		}
	})
}

func TestIsStoredGVK(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "CronTab"}

	tests := []struct {
		name string
		meta metav1.TypeMeta
		want bool
	}{
		{
			name: "matching",
			meta: metav1.TypeMeta{APIVersion: "example.com/v1", Kind: "CronTab"},
			want: true,
		},
		{
			name: "wrong kind",
			meta: metav1.TypeMeta{APIVersion: "example.com/v1", Kind: "Other"},
			want: false,
		},
		{
			name: "wrong version",
			meta: metav1.TypeMeta{APIVersion: "example.com/v2", Kind: "CronTab"},
			want: false,
		},
		{
			name: "wrong group",
			meta: metav1.TypeMeta{APIVersion: "other.com/v1", Kind: "CronTab"},
			want: false,
		},
		{
			name: "empty",
			meta: metav1.TypeMeta{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStoredGVK(&tt.meta, &gvk); got != tt.want {
				t.Errorf("isStoredGVK() = %v, want %v", got, tt.want)
			}
		})
	}
}
