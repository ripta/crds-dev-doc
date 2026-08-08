package crd

import (
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestStripLabels(t *testing.T) {
	crd := &apiextensions.CustomResourceDefinition{}
	crd.SetLabels(map[string]string{"app": "widget", "team": "infra"})

	StripLabels()(crd)

	if got := crd.GetLabels(); len(got) != 0 {
		t.Errorf("labels = %v, want empty", got)
	}
}

func TestStripLabelsOnCRDWithoutLabels(t *testing.T) {
	crd := &apiextensions.CustomResourceDefinition{}

	StripLabels()(crd)

	if got := crd.GetLabels(); len(got) != 0 {
		t.Errorf("labels = %v, want empty", got)
	}
}

// v1 validation rejects CRDs in *.k8s.io groups that lack the api-approved
// annotation, so stripping it would stop those CRDs indexing.
func TestStripAnnotationsKeepsAPIApproved(t *testing.T) {
	const approval = "https://github.com/kubernetes/kubernetes/pull/78458"

	crd := &apiextensions.CustomResourceDefinition{}
	crd.SetAnnotations(map[string]string{
		apiextensionsv1.KubeAPIApprovedAnnotation: approval,
		"kubectl.kubernetes.io/last-applied-configuration": "{}",
		"helm.sh/hook": "crd-install",
	})

	StripAnnotations()(crd)

	got := crd.GetAnnotations()
	if len(got) != 1 {
		t.Errorf("annotations = %v, want only the api-approved annotation", got)
	}
	if got[apiextensionsv1.KubeAPIApprovedAnnotation] != approval {
		t.Errorf("api-approved annotation = %q, want %q", got[apiextensionsv1.KubeAPIApprovedAnnotation], approval)
	}
}

func TestStripAnnotationsWithoutAPIApproved(t *testing.T) {
	crd := &apiextensions.CustomResourceDefinition{}
	crd.SetAnnotations(map[string]string{"helm.sh/hook": "crd-install"})

	StripAnnotations()(crd)

	if got := crd.GetAnnotations(); len(got) != 0 {
		t.Errorf("annotations = %v, want empty", got)
	}
}

func TestStripAnnotationsOnCRDWithoutAnnotations(t *testing.T) {
	crd := &apiextensions.CustomResourceDefinition{}

	StripAnnotations()(crd)

	if got := crd.GetAnnotations(); len(got) != 0 {
		t.Errorf("annotations = %v, want empty", got)
	}
}

func TestStripConversion(t *testing.T) {
	crd := &apiextensions.CustomResourceDefinition{
		Spec: apiextensions.CustomResourceDefinitionSpec{
			Conversion: &apiextensions.CustomResourceConversion{
				Strategy: apiextensions.WebhookConverter,
			},
		},
	}

	StripConversion()(crd)

	if crd.Spec.Conversion != nil {
		t.Errorf("conversion = %v, want nil", crd.Spec.Conversion)
	}
}

func TestStripConversionWhenAlreadyNil(t *testing.T) {
	crd := &apiextensions.CustomResourceDefinition{}

	StripConversion()(crd)

	if crd.Spec.Conversion != nil {
		t.Errorf("conversion = %v, want nil", crd.Spec.Conversion)
	}
}

// The v1beta1 fixture carries a webhook conversion strategy, which v1
// validation would reject without StripConversion.
func TestModifiersRunDuringConversion(t *testing.T) {
	c, err := NewCRDer(v1beta1crd, StripLabels(), StripAnnotations(), StripConversion())
	if err != nil {
		t.Fatalf("NewCRDer() error = %v", err)
	}

	if c.CRD.Spec.Conversion != nil {
		t.Errorf("conversion = %v, want nil", c.CRD.Spec.Conversion)
	}
	if got := c.CRD.GetLabels(); len(got) != 0 {
		t.Errorf("labels = %v, want empty", got)
	}
	if got := c.CRD.GetAnnotations(); len(got) != 0 {
		t.Errorf("annotations = %v, want empty", got)
	}
}
