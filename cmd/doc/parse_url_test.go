package main

import (
	"testing"
)

func TestParseGHURL(t *testing.T) {
	tests := []struct {
		name        string
		uPath       string
		wantOrg     string
		wantRepo    string
		wantGroup   string
		wantVersion string
		wantKind    string
		wantTag     string
		wantErr     error
	}{
		{
			name:        "basic path without tag",
			uPath:       "/prefix/myorg/myrepo/apps/v1/Deployment",
			wantOrg:     "myorg",
			wantRepo:    "myrepo",
			wantGroup:   "apps",
			wantVersion: "v1",
			wantKind:    "Deployment",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "path with hyphens and underscores",
			uPath:       "/api/my-org/my_repo/cert-manager.io/v1alpha1/Certificate",
			wantOrg:     "my-org",
			wantRepo:    "my_repo",
			wantGroup:   "cert-manager.io",
			wantVersion: "v1alpha1",
			wantKind:    "Certificate",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "path with dots in group",
			uPath:       "/repo/kubernetes/kubernetes/networking.k8s.io/v1/Ingress",
			wantOrg:     "kubernetes",
			wantRepo:    "kubernetes",
			wantGroup:   "networking.k8s.io",
			wantVersion: "v1",
			wantKind:    "Ingress",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "minimal valid path - exactly 6 elements",
			uPath:       "/a/b/c/d/e/f",
			wantOrg:     "b",
			wantRepo:    "c",
			wantGroup:   "d",
			wantVersion: "e",
			wantKind:    "f",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "path with more than 6 elements",
			uPath:       "/prefix/org/repo/group/version/kind/extra/elements",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "version",
			wantKind:    "kind",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "path with trailing slash",
			uPath:       "/prefix/org/repo/group/version/kind/",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "version",
			wantKind:    "kind",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "path without leading slash",
			uPath:       "prefix/org/repo/group/version/kind",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "version",
			wantKind:    "kind",
			wantTag:     "",
			wantErr:     nil,
		},

		{
			name:        "path with simple tag",
			uPath:       "/prefix/org/repo/group/v1/Kind@v1.0.0",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "v1.0.0",
			wantErr:     nil,
		},
		{
			name:        "path with semantic version tag",
			uPath:       "/repo/kubernetes/api/apps/v1/Deployment@v0.29.0",
			wantOrg:     "kubernetes",
			wantRepo:    "api",
			wantGroup:   "apps",
			wantVersion: "v1",
			wantKind:    "Deployment",
			wantTag:     "v0.29.0",
			wantErr:     nil,
		},
		{
			name:        "path with tag containing dashes",
			uPath:       "/prefix/org/repo/group/v1/Kind@release-1.2.3",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "release-1.2.3",
			wantErr:     nil,
		},
		{
			name:        "path with tag containing slashes",
			uPath:       "/prefix/org/repo/group/v1/Kind@feature/x/y",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "feature/x/y",
			wantErr:     nil,
		},
		{
			name:        "path with alpha tag",
			uPath:       "/prefix/org/repo/group/v1alpha1/CustomResource@v1.0.0-alpha.1",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1alpha1",
			wantKind:    "CustomResource",
			wantTag:     "v1.0.0-alpha.1",
			wantErr:     nil,
		},
		{
			name:        "path with short tag",
			uPath:       "/prefix/org/repo/group/v1/Kind@v1",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "v1",
			wantErr:     nil,
		},
		{
			name:        "path with tag at exactly position 6",
			uPath:       "/a/b/c/d/e/f@tag",
			wantOrg:     "b",
			wantRepo:    "c",
			wantGroup:   "d",
			wantVersion: "e",
			wantKind:    "f",
			wantTag:     "tag",
			wantErr:     nil,
		},

		{
			name:        "multiple @ symbols - first one wins for tag",
			uPath:       "/prefix/org/repo/group/v1/Kind@tag1@tag2",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "tag1",
			wantErr:     nil,
		},
		{
			name:        "@ in middle of path before kind",
			uPath:       "/prefix/org@something/repo/group/v1/Kind",
			wantOrg:     "org@something",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "something/repo/group/v1/Kind",
			wantErr:     nil,
		},
		{
			name:        "@ in org position",
			uPath:       "/prefix/@org/repo/group/v1/Kind",
			wantOrg:     "@org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "org/repo/group/v1/Kind",
			wantErr:     nil,
		},
		{
			name:        "empty tag after @",
			uPath:       "/prefix/org/repo/group/v1/Kind@",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "",
			wantErr:     nil,
		},

		{
			name:        "empty path",
			uPath:       "",
			wantOrg:     "",
			wantRepo:    "",
			wantGroup:   "",
			wantVersion: "",
			wantKind:    "",
			wantTag:     "",
			wantErr:     ErrInvalidPath,
		},
		{
			name:        "only slash",
			uPath:       "/",
			wantOrg:     "",
			wantRepo:    "",
			wantGroup:   "",
			wantVersion: "",
			wantKind:    "",
			wantTag:     "",
			wantErr:     ErrInvalidPath,
		},
		{
			name:        "one element",
			uPath:       "/prefix",
			wantOrg:     "",
			wantRepo:    "",
			wantGroup:   "",
			wantVersion: "",
			wantKind:    "",
			wantTag:     "",
			wantErr:     ErrInvalidPath,
		},
		{
			name:        "two elements",
			uPath:       "/prefix/org",
			wantOrg:     "",
			wantRepo:    "",
			wantGroup:   "",
			wantVersion: "",
			wantKind:    "",
			wantTag:     "",
			wantErr:     ErrInvalidPath,
		},
		{
			name:        "three elements",
			uPath:       "/prefix/org/repo",
			wantOrg:     "",
			wantRepo:    "",
			wantGroup:   "",
			wantVersion: "",
			wantKind:    "",
			wantTag:     "",
			wantErr:     ErrInvalidPath,
		},
		{
			name:        "four elements",
			uPath:       "/prefix/org/repo/group",
			wantOrg:     "",
			wantRepo:    "",
			wantGroup:   "",
			wantVersion: "",
			wantKind:    "",
			wantTag:     "",
			wantErr:     ErrInvalidPath,
		},
		{
			name:        "five elements",
			uPath:       "/prefix/org/repo/group/version",
			wantOrg:     "",
			wantRepo:    "",
			wantGroup:   "",
			wantVersion: "",
			wantKind:    "",
			wantTag:     "",
			wantErr:     ErrInvalidPath,
		},

		{
			name:        "empty segment in middle",
			uPath:       "/prefix/org//group/version/kind",
			wantOrg:     "org",
			wantRepo:    "",
			wantGroup:   "group",
			wantVersion: "version",
			wantKind:    "kind",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "multiple empty segments",
			uPath:       "/prefix///org/repo/group/version/kind",
			wantOrg:     "",
			wantRepo:    "",
			wantGroup:   "org",
			wantVersion: "repo",
			wantKind:    "group",
			wantTag:     "",
			wantErr:     nil,
		},

		{
			name:        "path with query string",
			uPath:       "/prefix/org/repo/group/v1/Kind?foo=bar",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "path with fragment",
			uPath:       "/prefix/org/repo/group/v1/Kind#section",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "path with query and fragment",
			uPath:       "/prefix/org/repo/group/v1/Kind?foo=bar#section",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "path with tag and query string",
			uPath:       "/prefix/org/repo/group/v1/Kind@v1.0.0?foo=bar",
			wantOrg:     "org",
			wantRepo:    "repo",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "v1.0.0",
			wantErr:     nil,
		},

		{
			name:        "numeric org and repo",
			uPath:       "/prefix/123/456/group/v1/Kind",
			wantOrg:     "123",
			wantRepo:    "456",
			wantGroup:   "group",
			wantVersion: "v1",
			wantKind:    "Kind",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "camelCase components",
			uPath:       "/prefix/MyOrg/MyRepo/myGroup/v1beta1/MyKind",
			wantOrg:     "MyOrg",
			wantRepo:    "MyRepo",
			wantGroup:   "myGroup",
			wantVersion: "v1beta1",
			wantKind:    "MyKind",
			wantTag:     "",
			wantErr:     nil,
		},
		{
			name:        "long path components",
			uPath:       "/prefix/very-long-organization-name/very-long-repository-name/very.long.group.name/v1alpha1/VeryLongKindName",
			wantOrg:     "very-long-organization-name",
			wantRepo:    "very-long-repository-name",
			wantGroup:   "very.long.group.name",
			wantVersion: "v1alpha1",
			wantKind:    "VeryLongKindName",
			wantTag:     "",
			wantErr:     nil,
		},

		{
			name:        "kubernetes deployment",
			uPath:       "/repo/kubernetes/kubernetes/apps/v1/Deployment@v1.29.0",
			wantOrg:     "kubernetes",
			wantRepo:    "kubernetes",
			wantGroup:   "apps",
			wantVersion: "v1",
			wantKind:    "Deployment",
			wantTag:     "v1.29.0",
			wantErr:     nil,
		},
		{
			name:        "cert-manager certificate",
			uPath:       "/repo/cert-manager/cert-manager/cert-manager.io/v1/Certificate@v1.13.0",
			wantOrg:     "cert-manager",
			wantRepo:    "cert-manager",
			wantGroup:   "cert-manager.io",
			wantVersion: "v1",
			wantKind:    "Certificate",
			wantTag:     "v1.13.0",
			wantErr:     nil,
		},
		{
			name:        "istio virtual service",
			uPath:       "/repo/istio/istio/networking.istio.io/v1beta1/VirtualService@1.20.0",
			wantOrg:     "istio",
			wantRepo:    "istio",
			wantGroup:   "networking.istio.io",
			wantVersion: "v1beta1",
			wantKind:    "VirtualService",
			wantTag:     "1.20.0",
			wantErr:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOrg, gotRepo, gotGroup, gotVersion, gotKind, gotTag, err := parseGHURL(tt.uPath)

			if err != tt.wantErr {
				t.Errorf("parseGHURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr != nil {
				return
			}

			if gotOrg != tt.wantOrg {
				t.Errorf("parseGHURL() org = %q, want %q", gotOrg, tt.wantOrg)
			}
			if gotRepo != tt.wantRepo {
				t.Errorf("parseGHURL() repo = %q, want %q", gotRepo, tt.wantRepo)
			}
			if gotGroup != tt.wantGroup {
				t.Errorf("parseGHURL() group = %q, want %q", gotGroup, tt.wantGroup)
			}
			if gotVersion != tt.wantVersion {
				t.Errorf("parseGHURL() version = %q, want %q", gotVersion, tt.wantVersion)
			}
			if gotKind != tt.wantKind {
				t.Errorf("parseGHURL() kind = %q, want %q", gotKind, tt.wantKind)
			}
			if gotTag != tt.wantTag {
				t.Errorf("parseGHURL() tag = %q, want %q", gotTag, tt.wantTag)
			}
		})
	}
}
