package models

import "testing"

// FullName is the canonical repo key. It is what gets written to tags.repo,
// what CatchUp reads back out, and what Index locks on, so the normalization
// has to be stable across all three.
func TestGitterRepoFullName(t *testing.T) {
	tests := []struct {
		name string
		repo GitterRepo
		want string
	}{
		{
			name: "already lowercase",
			repo: GitterRepo{Org: "crossplane", Repo: "crossplane"},
			want: "github.com/crossplane/crossplane",
		},
		{
			name: "lowercases the org",
			repo: GitterRepo{Org: "Crossplane", Repo: "crossplane"},
			want: "github.com/crossplane/crossplane",
		},
		{
			name: "lowercases the repo",
			repo: GitterRepo{Org: "crossplane", Repo: "Crossplane"},
			want: "github.com/crossplane/crossplane",
		},
		{
			name: "lowercases both",
			repo: GitterRepo{Org: "CrossPlane", Repo: "CrossPlane"},
			want: "github.com/crossplane/crossplane",
		},
		{
			name: "keeps hyphens and dots",
			repo: GitterRepo{Org: "cert-manager", Repo: "cert-manager.io"},
			want: "github.com/cert-manager/cert-manager.io",
		},
		{
			name: "keeps digits and underscores",
			repo: GitterRepo{Org: "org_1", Repo: "repo_2"},
			want: "github.com/org_1/repo_2",
		},
		{
			name: "tag is not part of the name",
			repo: GitterRepo{Org: "Org", Repo: "Repo", Tag: "v1.0.0"},
			want: "github.com/org/repo",
		},
		{
			name: "empty fields",
			repo: GitterRepo{},
			want: "github.com//",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.repo.FullName(); got != tt.want {
				t.Errorf("FullName() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Regression: Index built its lock key without lowercasing while CatchUp
// locked on the database name, so the two could index one repo concurrently.
func TestGitterRepoFullNameIsCaseInsensitive(t *testing.T) {
	variants := []GitterRepo{
		{Org: "Crossplane", Repo: "Crossplane"},
		{Org: "crossplane", Repo: "CROSSPLANE"},
		{Org: "CROSSPLANE", Repo: "crossplane"},
		{Org: "crossplane", Repo: "crossplane"},
	}

	want := variants[len(variants)-1].FullName()
	for _, v := range variants {
		if got := v.FullName(); got != want {
			t.Errorf("FullName() for %+v = %q, want %q", v, got, want)
		}
	}
}
