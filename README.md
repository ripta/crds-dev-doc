# crds-dev-doc

> A Kubernetes Custom Resource Definition (CRD) documentation browser.

https://crds.r8y.page/

crds-dev-doc (né `doc`) is a web service that indexes and displays
documentation for Kubernetes CRDs from public GitHub repositories. Search and
browse CRDs by repository, version tag, or Group/Version/Kind (GVK).

This is a fork of [crdsdev/doc](https://github.com/crdsdev/doc), originally
powering doc.crds.dev. I'd happily contribute the changes back should the
original author resume accepting PRs.

## Features

- Find CRDs and search by GVK from any public GitHub repository
- Browse versions at specific git tags or the latest version
- Automatic indexing discovers CRDs from GitHub repositories

## Repository Requirements

For a repository to be successfully indexed, it must meet the following criteria:

- Publicly hosted on GitHub, which is inherited from the original implementation
- CRDs in YAML format (i.e., not JSON, not Helm chart templates)
- Files must have `.yaml` extension
- Must have `kind` of `CustomResourceDefinition`
- Must pass the [Kubernetes CRD validator](https://pkg.go.dev/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation)

## API Reference

### Doc Server Endpoints

The doc web server (`cmd/doc`) exposes the following HTTP endpoints, which is the primary interface for users:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Home page with search interface |
| GET | `/gvk` | Browse all indexed Groups |
| GET | `/gvk/{group}` | List all versions in the specified Group |
| GET | `/gvk/{group}/{version}` | List all Kinds in the specified Group-Version |
| GET | `/gvk/{group}/{version}/{kind}` | List all repos containing the specified GVK |
| GET | `/raw/github.com/{org}/{repo}` | Get raw JSON data for latest repository version |
| GET | `/raw/github.com/{org}/{repo}@{tag}` | Get raw JSON data for a repository/tag |
| GET | `/recent` | List recently indexed repositories |
| GET | `/repo/github.com/{org}/{repo}` | List all indexed tags for a repository |
| GET | `/repo/github.com/{org}/{repo}@{tag}` | View CRDs from a specific repository and tag |

### Gitter RPC Methods

The gitter indexer service (`cmd/gitter`), which `doc` talks to, exposes the following RPC methods:

#### `Ping() -> string`

Health check method that just returns `pong`.

#### `Index(GitterRepo) -> string`

Indexes a GitHub repository and extracts CRDs.

**Parameters:**
```go
type GitterRepo struct {
    Org  string // GitHub organization/user
    Repo string // Repository name
    Tag  string // Git tag/version to index
}
```

## Local Development

See [DEVELOPING.md](DEVELOPING.md) for additional local development steps.
