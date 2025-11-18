/*
Copyright 2020 The CRDS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync/atomic"
	"time"

	crdutil "github.com/crdsdev/doc/pkg/crd"
	"github.com/crdsdev/doc/pkg/models"
	"github.com/crdsdev/doc/pkg/validation"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/unrolled/render"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

var db *pgxpool.Pool

// redis connection
var (
	envDevelopment = "IS_DEV"

	userEnv     = "PG_USER"
	passwordEnv = "PG_PASS"
	hostEnv     = "PG_HOST"
	portEnv     = "PG_PORT"
	dbEnv       = "PG_DB"

	listenAddrEnv     = "DOC_LISTEN_ADDR"
	defaultListenAddr = ":5001"

	gitterAddrEnv     = "GITTER_ADDR"
	defaultGitterAddr = "127.0.0.1:5002"

	gitterAddr        string
	gitterSemaphore   chan struct{}
	gitterPingTime    atomic.Int64
	gitterLastHealthy atomic.Bool

	longCacheDuration  = 4 * time.Hour
	shortCacheDuration = 5 * time.Minute
)

var logger *slog.Logger

func parseLogLevel() slog.Level {
	level := strings.ToLower(os.Getenv("LOG_LEVEL"))
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SchemaPlusParent is a JSON schema plus the name of the parent field.
type SchemaPlusParent struct {
	Parent string
	Schema map[string]apiextensions.JSONSchemaProps
}

var page = render.New(render.Options{
	Extensions:    []string{".html"},
	Directory:     "template",
	Layout:        "layout",
	IsDevelopment: os.Getenv(envDevelopment) == "true",
	Funcs: []template.FuncMap{
		{
			"plusParent": func(p string, s map[string]apiextensions.JSONSchemaProps) *SchemaPlusParent {
				return &SchemaPlusParent{
					Parent: p,
					Schema: s,
				}
			},
		},
	},
})

type pageData struct {
	DisableNavBar bool
	IsDarkMode    bool
	Title         string
	IndexerAlive  bool
}

type baseData struct {
	Page          pageData
	IndexingReply string
	IndexingError string
}

type docData struct {
	Page        pageData
	Repo        string
	Tag         string
	At          string
	Group       string
	Version     string
	Kind        string
	Description string
	Schema      apiextensions.JSONSchemaProps
}

type listGVKData struct {
	Page    pageData
	Group   string
	Version string
	Kind    string

	Total    int
	Repotags map[string][]tagInfo
}

type listGroupVersionData struct {
	Page    pageData
	Group   string
	Version string

	Total int
	Kinds map[string]int
}

type listGroupsData struct {
	Page  pageData
	Group string

	Total    int
	Versions map[string]int
}

type listAllGroupsData struct {
	Page pageData

	Total  int
	Groups map[string]listAllGroupStats
}

type listAllGroupStats struct {
	VersionCount int
	KindCount    int
}

type listTagsData struct {
	Page  pageData
	Repo  string
	Tags  []tagInfo
	Total int
}

type orgData struct {
	Page  pageData
	Repo  string
	Tag   string
	At    string
	Tags  []tagInfo
	CRDs  map[string]models.RepoCRD
	Total int
}

type homeData struct {
	Page  pageData
	Repos []string
}

func gitterPinger(gitterAddr string) {
	ping := func() {
		client, err := rpc.DialHTTP("tcp", gitterAddr)
		if err != nil {
			if wasHealthy := gitterLastHealthy.Load(); wasHealthy {
				logger.Warn("gitter became unhealthy: dialing error", "err", err)
				gitterLastHealthy.Store(false)
			}
			return
		}

		reply := ""
		if err := client.Call("Gitter.Ping", struct{}{}, &reply); err != nil {
			if wasHealthy := gitterLastHealthy.Load(); wasHealthy {
				logger.Warn("gitter became unhealthy: ping error", "err", err)
				gitterLastHealthy.Store(false)
			}
		} else {
			gitterPingTime.Store(time.Now().Unix())
			if wasHealthy := gitterLastHealthy.Load(); !wasHealthy {
				logger.Info("gitter became healthy", "reply", reply)
				gitterLastHealthy.Store(true)
			}
		}
	}

	ping()

	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			ping()
		}
	}
}

func gitterIsAlive() bool {
	lastPing := gitterPingTime.Load()
	return lastPing > 0 && time.Now().Unix()-lastPing <= 120
}

// callGitterIndex makes a synchronous RPC call to Gitter.Index with timeout and semaphore limiting.
// It returns the reply message and any error that occurred.
func callGitterIndex(ctx context.Context, repo models.GitterRepo, gitterAddr string) (string, error) {
	// Try to acquire semaphore slot (non-blocking)
	select {
	case gitterSemaphore <- struct{}{}:
		defer func() { <-gitterSemaphore }() // Release on return
	default:
		return "", fmt.Errorf("indexing queue is full; try again in a few minutes")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dialDone := make(chan struct{})
	var client *rpc.Client
	var dialErr error
	go func() {
		client, dialErr = rpc.DialHTTP("tcp", gitterAddr)
		close(dialDone)
	}()

	select {
	case <-dialDone:
		if dialErr != nil {
			logger.Error("dialing gitter failed", "err", dialErr)
			return "", fmt.Errorf("unable to connect to indexing service")
		}
	case <-ctx.Done():
		return "", fmt.Errorf("timeout connecting to indexing service")
	}
	defer client.Close()

	// Make RPC call with timeout
	callDone := make(chan struct{})
	var callErr error
	var callReply string
	go func() {
		callErr = client.Call("Gitter.Index", repo, &callReply)
		close(callDone)
	}()

	select {
	case <-callDone:
		if callErr != nil {
			logger.Error("gitter.Index error", "org", repo.Org, "repo", repo.Repo, "tag", repo.Tag, "reply", callReply, "err", callErr)
			return callReply, callErr
		}

		logger.Info("gitter.Index succeeded", "org", repo.Org, "repo", repo.Repo, "tag", repo.Tag, "reply", callReply)
		return callReply, nil
	case <-ctx.Done():
		return "", fmt.Errorf("timeout waiting for indexing service response")
	}
}

func main() {
	logLevel := parseLogLevel()
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))

	dsn := os.Getenv("CRDS_DEV_STORAGE_DSN")
	if dsn == "" {
		dsn = fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", os.Getenv(userEnv), os.Getenv(passwordEnv), os.Getenv(hostEnv), os.Getenv(portEnv), os.Getenv(dbEnv))
	}

	conn, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		logger.Error("failed to parse database config", "err", err)
		os.Exit(1)
	}
	db, err = pgxpool.ConnectConfig(context.Background(), conn)
	if err != nil {
		logger.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}

	gitterAddr = defaultGitterAddr
	if value, ok := os.LookupEnv(gitterAddrEnv); ok && value != "" {
		gitterAddr = value
	}

	logger.Info("gitter address", "addr", gitterAddr)

	// Initialize semaphore for limiting concurrent RPC calls
	gitterSemaphore = make(chan struct{}, 4)

	go gitterPinger(gitterAddr)

	start()
}

func getPageData(r *http.Request, title string, disableNavBar bool) pageData {
	return pageData{
		DisableNavBar: disableNavBar,
		Title:         title,
		IndexerAlive:  gitterIsAlive(),
	}
}

func start() {
	listenAddr := defaultListenAddr
	if value, ok := os.LookupEnv(listenAddrEnv); ok && value != "" {
		listenAddr = value
	}

	logger.Info("starting doc server", "addr", listenAddr)
	r := mux.NewRouter().StrictSlash(true)
	r.Use(loggingMiddleware)
	staticHandler := http.StripPrefix("/static/", http.FileServer(http.Dir("./static/")))
	r.HandleFunc("/", home)
	r.PathPrefix("/static/").Handler(staticHandler)
	r.HandleFunc("/gvk/{group}/{version}/{kind}", listGVK)
	r.HandleFunc("/gvk/{group}/{version}", listGroupVersion)
	r.HandleFunc("/gvk/{group}", listGroups)
	r.HandleFunc("/gvk", listAllGroups)
	r.HandleFunc("/repo/github.com/{org}/{repo}@{tag:[A-Za-z0-9._/+-]+}", org)
	r.HandleFunc("/repo/github.com/{org}/{repo}", listTags)
	r.HandleFunc("/recent", listRecentlyIndexedRepos)
	r.HandleFunc("/raw/github.com/{org}/{repo}@{tag:[A-Za-z0-9._/+-]+}", raw)
	r.HandleFunc("/raw/github.com/{org}/{repo}", raw)
	r.PathPrefix("/").HandlerFunc(doc)
	if err := http.ListenAndServe(listenAddr, r); err != nil {
		logger.Error("failed to serve", "addr", listenAddr, "err", err)
		os.Exit(1)
	}
}

func home(w http.ResponseWriter, r *http.Request) {
	emitCacheControl(w, longCacheDuration)

	data := homeData{Page: getPageData(r, "Doc", true)}
	if err := page.HTML(w, http.StatusOK, "home", data); err != nil {
		logger.Error("failed to render home template", "err", err)
		fmt.Fprint(w, "Unable to render home template.")
		return
	}
	logger.Info("rendered home page")
}

func listGVK(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	group := parameters["group"]
	version := parameters["version"]
	kind := parameters["kind"]

	rows, err := db.Query(r.Context(), "SELECT t.repo, t.name, t.time, encode(t.hash_sha1, 'hex'), t.alias_tag_id FROM tags t INNER JOIN crds c ON (c.tag_id = t.id) WHERE c.group=$1 AND c.version=$2 AND c.kind=$3 ORDER BY t.time DESC;", group, version, kind)
	if err != nil {
		logger.Error("failed to get repos for GVK", "group", group, "version", version, "kind", kind, "err", err)
		http.Error(w, "Unable to get repositories for supplied GVK.", http.StatusInternalServerError)
		return
	}

	data := listGVKData{
		Page:     getPageData(r, fmt.Sprintf("%s/%s.%s", group, version, kind), false),
		Group:    group,
		Version:  version,
		Kind:     kind,
		Repotags: map[string][]tagInfo{},
	}

	for rows.Next() {
		var repo, tag string
		var timestamp time.Time
		var hashSHA1 string
		var aliasTagID *int
		if err := rows.Scan(&repo, &tag, &timestamp, &hashSHA1, &aliasTagID); err != nil {
			logger.Error("failed to scan repo row for GVK", "group", group, "version", version, "kind", kind, "err", err)
			fmt.Fprint(w, "Unable to get repositories for supplied GVK.")
			return
		}

		data.Repotags[repo] = append(data.Repotags[repo], tagInfo{
			Name:       tag,
			Timestamp:  timestamp,
			HashSHA1:   hashSHA1,
			AliasTagID: aliasTagID,
		})
		data.Total++
	}

	if data.Total == 0 {
		http.Error(w, "GVK not found.", http.StatusNotFound)
		return
	}

	if err := page.HTML(w, http.StatusOK, "list_gvk", data); err != nil {
		logger.Error("failed to render list GVK template", "group", group, "version", version, "kind", kind, "err", err)
		fmt.Fprint(w, "Unable to render list GVK template.")
		return
	}
	logger.Info("rendered list GVK template", "group", group, "version", version, "kind", kind)
}

func listGroupVersion(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	group := parameters["group"]
	version := parameters["version"]

	rows, err := db.Query(r.Context(), "SELECT c.kind, COUNT(1) FROM crds c WHERE c.group=$1 AND c.version=$2 GROUP BY c.kind;", group, version)
	if err != nil {
		logger.Error("failed to get repos for group-version", "group", group, "version", version, "err", err)
		http.Error(w, "Unable to get repositories for supplied group-version.", http.StatusInternalServerError)
		return
	}

	data := listGroupVersionData{
		Page:    getPageData(r, fmt.Sprintf("%s/%s", group, version), false),
		Group:   group,
		Version: version,
		Kinds:   map[string]int{},
	}

	for rows.Next() {
		var kind string
		var count int
		if err := rows.Scan(&kind, &count); err != nil {
			logger.Error("failed to scan repo row for group-version", "group", group, "version", version, "err", err)
			fmt.Fprint(w, "Unable to get repositories for supplied group-version.")
			return
		}

		data.Kinds[kind] = count
		data.Total++
	}

	if data.Total == 0 {
		http.Error(w, "Group-Version not found.", http.StatusNotFound)
		return
	}

	if err := page.HTML(w, http.StatusOK, "list_group_version", data); err != nil {
		logger.Error("failed to render list group-version template", "group", group, "version", version, "err", err)
		fmt.Fprint(w, "Unable to render list group-version template.")
		return
	}
	logger.Info("rendered list group-version template", "group", group, "version", version)
}

func listGroups(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	group := parameters["group"]

	rows, err := db.Query(r.Context(), "SELECT c.version, COUNT(DISTINCT c.kind) FROM crds c WHERE c.group=$1 GROUP BY c.version;", group)
	if err != nil {
		logger.Error("failed to get versions for group", "group", group, "err", err)
		http.Error(w, "Unable to get versions for supplied group.", http.StatusInternalServerError)
		return
	}

	data := listGroupsData{
		Page:     getPageData(r, group, false),
		Group:    group,
		Versions: map[string]int{},
	}

	for rows.Next() {
		var version string
		var count int
		if err := rows.Scan(&version, &count); err != nil {
			logger.Error("failed to scan version row for group", "group", group, "err", err)
			fmt.Fprint(w, "Unable to get versions for supplied group.")
			return
		}

		data.Versions[version] = count
		data.Total++
	}

	if data.Total == 0 {
		http.Error(w, "Group not found.", http.StatusNotFound)
		return
	}

	if err := page.HTML(w, http.StatusOK, "list_groups", data); err != nil {
		logger.Error("failed to render list groups template", "group", group, "err", err)
		fmt.Fprint(w, "Unable to render list groups template.")
		return
	}
	logger.Info("rendered list groups template", "group", group)
}

func listAllGroups(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(r.Context(), "SELECT c.group, COUNT(DISTINCT c.version), COUNT(DISTINCT c.kind) FROM crds c GROUP BY c.group ORDER BY c.group;")
	if err != nil {
		logger.Error("failed to get all groups", "err", err)
		http.Error(w, "Unable to get all groups.", http.StatusInternalServerError)
		return
	}

	data := listAllGroupsData{
		Page:   getPageData(r, "All Groups", false),
		Groups: map[string]listAllGroupStats{},
	}

	for rows.Next() {
		var group string
		var versionCount, kindCount int
		if err := rows.Scan(&group, &versionCount, &kindCount); err != nil {
			logger.Error("failed to scan all groups row", "err", err)
			fmt.Fprint(w, "Unable to get all groups.")
			return
		}

		data.Groups[group] = listAllGroupStats{
			VersionCount: versionCount,
			KindCount:    kindCount,
		}
		data.Total++
	}

	if err := page.HTML(w, http.StatusOK, "list_all_groups", data); err != nil {
		logger.Error("failed to render list all groups template", "err", err)
		fmt.Fprint(w, "Unable to render list all groups template.")
		return
	}
	logger.Info("rendered list all groups template")
}

func raw(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	org := parameters["org"]
	repo := parameters["repo"]
	tag := parameters["tag"]

	// Validate tag if present
	if tag != "" {
		if err := validation.ValidateTag(tag); err != nil {
			logger.Warn("invalid tag format in raw handler", "tag", tag, "remote_addr", r.RemoteAddr, "err", err)
			http.Error(w, "Invalid tag format.", http.StatusBadRequest)
			return
		}
	}

	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", org, repo)
	var rows pgx.Rows
	var err error
	if tag == "" {
		rows, err = db.Query(r.Context(), "SELECT c.data::jsonb FROM tags t INNER JOIN crds c ON (c.tag_id = COALESCE(t.alias_tag_id, t.id)) WHERE LOWER(t.repo)=LOWER($1) AND t.id = (SELECT id FROM tags WHERE LOWER(repo) = LOWER($1) ORDER BY time DESC LIMIT 1);", fullRepo)
	} else {
		rows, err = db.Query(r.Context(), "SELECT c.data::jsonb FROM tags t INNER JOIN crds c ON (c.tag_id = COALESCE(t.alias_tag_id, t.id)) WHERE LOWER(t.repo)=LOWER($1) AND t.name=$2;", fullRepo, tag)
	}

	var res []byte
	var total []byte
	for err == nil && rows.Next() {
		if err := rows.Scan(&res); err != nil {
			break
		}
		crd := &apiextensions.CustomResourceDefinition{}
		if err := yaml.Unmarshal(res, crd); err != nil {
			break
		}
		crdv1 := &apiextensionsv1.CustomResourceDefinition{}
		if err := apiextensionsv1.Convert_apiextensions_CustomResourceDefinition_To_v1_CustomResourceDefinition(crd, crdv1, nil); err != nil {
			break
		}
		crdv1.SetGroupVersionKind(apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinition"))
		y, err := yaml.Marshal(crdv1)
		if err != nil {
			break
		}
		total = append(total, y...)
		total = append(total, []byte("\n---\n")...)
	}

	if err != nil {
		fmt.Fprint(w, "Unable to render raw CRDs.")
		logger.Error("failed to get raw CRDs", "repo", repo, "full_repo", fullRepo, "err", err)
	} else {
		w.Write([]byte(total))
		logger.Info("rendered raw CRDs", "repo", repo)
	}
}

type tagInfo struct {
	Name       string
	Timestamp  time.Time
	HashSHA1   string
	AliasTagID *int
}

func listTags(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	org := parameters["org"]
	repo := parameters["repo"]
	pageData := getPageData(r, fmt.Sprintf("%s/%s Tags", org, repo), false)
	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", org, repo)

	rows, err := db.Query(r.Context(), "SELECT name, time, encode(hash_sha1, 'hex'), alias_tag_id FROM tags WHERE LOWER(repo)=LOWER($1) ORDER BY time DESC;", fullRepo)
	if err != nil {
		logger.Error("failed to get tags for repo", "repo", repo, "err", err)
		http.Error(w, "Unable to get tags.", http.StatusInternalServerError)
		return
	}

	tags := []tagInfo{}
	for rows.Next() {
		var t string
		var ts time.Time
		var hashSHA1 string
		var aliasTagID *int
		if err := rows.Scan(&t, &ts, &hashSHA1, &aliasTagID); err != nil {
			logger.Error("failed to scan tag row", "err", err)
			fmt.Fprint(w, "Unable to render tags.")
			return
		}

		tags = append(tags, tagInfo{
			Name:       t,
			Timestamp:  ts,
			HashSHA1:   hashSHA1,
			AliasTagID: aliasTagID,
		})
	}

	if len(tags) == 0 {
		data := baseData{Page: pageData}
		reply, err := callGitterIndex(r.Context(), models.GitterRepo{
			Org:  org,
			Repo: repo,
			Tag:  "", // all tags
		}, gitterAddr)
		if err != nil {
			data.IndexingError = err.Error()
		}
		if reply != "" {
			data.IndexingReply = reply
		}

		emitCacheControl(w, 0)
		if err := page.HTML(w, http.StatusOK, "new", data); err != nil {
			logger.Error("failed to render new template", "err", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
		return
	}

	emitCacheControl(w, shortCacheDuration)
	if err := page.HTML(w, http.StatusOK, "list_tags", listTagsData{
		Page:  pageData,
		Repo:  strings.Join([]string{org, repo}, "/"),
		Tags:  tags,
		Total: len(tags),
	}); err != nil {
		logger.Error("failed to render list tags template", "org", org, "repo", repo, "err", err)
		fmt.Fprint(w, "Unable to render list tags template.")
		return
	}
	logger.Info("rendered list tags template", "org", org, "repo", repo)
}

type listRecentlyIndexedReposData struct {
	Page     pageData
	Repotags map[string][]tagInfo
}

func listRecentlyIndexedRepos(w http.ResponseWriter, r *http.Request) {
	pageData := getPageData(r, "Recently Indexed Repositories", false)
	rows, err := db.Query(r.Context(), "SELECT t.repo, t.name, t.time FROM tags t WHERE t.alias_tag_id IS NULL ORDER BY t.time DESC LIMIT 20;")
	if err != nil {
		logger.Error("failed to get recently indexed repos", "err", err)
		http.Error(w, "Unable to get recently indexed repositories.", http.StatusInternalServerError)
		return
	}

	repotags := map[string][]tagInfo{}
	for rows.Next() {
		var repo, tag string
		var timestamp time.Time
		if err := rows.Scan(&repo, &tag, &timestamp); err != nil {
			logger.Error("failed to scan recently indexed repos row", "err", err)
			fmt.Fprint(w, "Unable to render recently indexed repositories.")
			return
		}

		repotags[repo] = append(repotags[repo], tagInfo{
			Name:      tag,
			Timestamp: timestamp,
		})
	}

	emitCacheControl(w, shortCacheDuration)
	if err := page.HTML(w, http.StatusOK, "list_recently_indexed_repos", listRecentlyIndexedReposData{
		Page:     pageData,
		Repotags: repotags,
	}); err != nil {
		logger.Error("failed to render recently indexed repositories template", "err", err)
		fmt.Fprint(w, "Unable to render recently indexed repositories template.")
		return
	}

	logger.Info("rendered recently indexed repositories template")
}

func org(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	org := parameters["org"]
	repo := parameters["repo"]
	tag := parameters["tag"]

	// Validate tag format
	if err := validation.ValidateTag(tag); err != nil {
		logger.Warn("invalid tag format in org handler", "tag", tag, "remote_addr", r.RemoteAddr, "err", err)
		http.Error(w, "Invalid tag format.", http.StatusBadRequest)
		return
	}

	pageData := getPageData(r, fmt.Sprintf("%s/%s", org, repo), false)
	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", org, repo)

	b := &pgx.Batch{}
	pageData.Title += fmt.Sprintf("@%s", tag)
	b.Queue("SELECT t.name, c.group, c.version, c.kind FROM tags t INNER JOIN crds c ON (c.tag_id = COALESCE(t.alias_tag_id, t.id)) WHERE LOWER(t.repo)=LOWER($1) AND t.name=$2;", fullRepo, tag)
	b.Queue("SELECT name, time, encode(hash_sha1, 'hex'), alias_tag_id FROM tags WHERE LOWER(repo)=LOWER($1) ORDER BY time DESC;", fullRepo)
	br := db.SendBatch(r.Context(), b)
	defer br.Close()
	c, err := br.Query()
	if err != nil {
		logger.Error("failed to get CRDs for repo", "repo", repo, "full_repo", fullRepo, "err", err)

		emitCacheControl(w, 0)
		if err := page.HTML(w, http.StatusOK, "new", baseData{Page: pageData}); err != nil {
			logger.Error("failed to render new template", "err", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
		return
	}
	repoCRDs := map[string]models.RepoCRD{}
	foundTag := tag
	foundTagTimestamp := time.Time{}
	for c.Next() {
		var t, g, v, k string
		if err := c.Scan(&t, &g, &v, &k); err != nil {
			logger.Error("failed to scan CRD row", "err", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
		foundTag = t
		repoCRDs[g+"/"+v+"/"+k] = models.RepoCRD{
			Group:   g,
			Version: v,
			Kind:    k,
		}
	}
	c, err = br.Query()
	if err != nil {
		logger.Error("failed to get tags for repo", "repo", repo, "err", err)

		emitCacheControl(w, 0)
		if err := page.HTML(w, http.StatusOK, "new", baseData{Page: pageData}); err != nil {
			logger.Error("failed to render new template", "err", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
		return
	}
	tags := []tagInfo{}
	tagExists := false
	for c.Next() {
		var t string
		var ts time.Time
		var hashSHA1 string
		var aliasTagID *int
		if err := c.Scan(&t, &ts, &hashSHA1, &aliasTagID); err != nil {
			logger.Error("failed to scan tag row", "err", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
		if !tagExists && t == tag {
			tagExists = true
		}
		tags = append(tags, tagInfo{
			Name:       t,
			Timestamp:  ts,
			HashSHA1:   hashSHA1,
			AliasTagID: aliasTagID,
		})
	}
	if len(tags) == 0 || (!tagExists && tag != "") {
		data := baseData{Page: pageData}
		reply, err := callGitterIndex(r.Context(), models.GitterRepo{
			Org:  org,
			Repo: repo,
			Tag:  tag,
		}, gitterAddr)
		if err != nil {
			data.IndexingError = err.Error()
		}
		if reply != "" {
			data.IndexingReply = reply
		}

		emitCacheControl(w, 0)
		if err := page.HTML(w, http.StatusOK, "new", data); err != nil {
			logger.Error("failed to render new template", "err", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
		return
	}
	if foundTag == "" {
		foundTag = tags[0].Name
		foundTagTimestamp = tags[0].Timestamp
	} else {
		for _, t := range tags {
			if t.Name == foundTag {
				foundTagTimestamp = t.Timestamp
				break
			}
		}
	}

	emitCacheControl(w, longCacheDuration)
	if err := page.HTML(w, http.StatusOK, "org", orgData{
		Page:  pageData,
		Repo:  strings.Join([]string{org, repo}, "/"),
		Tag:   foundTag,
		At:    foundTagTimestamp.Format(time.RFC3339),
		Tags:  tags,
		CRDs:  repoCRDs,
		Total: len(repoCRDs),
	}); err != nil {
		logger.Error("failed to render org template", "org", org, "repo", repo, "tag", foundTag, "err", err)
		fmt.Fprint(w, "Unable to render org template.")
		return
	}
	logger.Info("rendered org template", "org", org, "repo", repo, "tag", foundTag)
}

func doc(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/repo")
	if !ok {
		http.Error(w, "Invalid URL.", http.StatusNotFound)
		return
	}

	org, repo, group, kind, version, tag, err := parseGHURL(rest)
	if err != nil {
		logger.Warn("failed to parse Github path", "path", r.URL.Path, "err", err)
		http.Error(w, "Repository not found.", http.StatusNotFound)
		return
	}

	if tag != "" {
		if err := validation.ValidateTag(tag); err != nil {
			logger.Warn("invalid tag format in doc handler", "tag", tag, "remote_addr", r.RemoteAddr, "err", err)
			http.Error(w, "Invalid tag format.", http.StatusBadRequest)
			return
		}
	}

	pageData := getPageData(r, fmt.Sprintf("%s.%s/%s", kind, group, version), false)
	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", org, repo)
	var c pgx.Row
	if tag == "" {
		c = db.QueryRow(r.Context(), "SELECT t.name, c.data::jsonb FROM tags t INNER JOIN crds c ON (c.tag_id = COALESCE(t.alias_tag_id, t.id)) WHERE LOWER(t.repo)=LOWER($1) AND t.id = (SELECT id FROM tags WHERE repo = $1 ORDER BY time DESC LIMIT 1) AND c.group=$2 AND c.version=$3 AND c.kind=$4;", fullRepo, group, version, kind)
	} else {
		c = db.QueryRow(r.Context(), "SELECT t.name, c.data::jsonb FROM tags t INNER JOIN crds c ON (c.tag_id = COALESCE(t.alias_tag_id, t.id)) WHERE LOWER(t.repo)=LOWER($1) AND t.name=$2 AND c.group=$3 AND c.version=$4 AND c.kind=$5;", fullRepo, tag, group, version, kind)
	}

	foundTag := tag
	crd := &apiextensions.CustomResourceDefinition{}
	if err := c.Scan(&foundTag, crd); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "CRD not found.", http.StatusNotFound)
			return
		}

		logger.Error("failed to get CRDs for repo", "repo", repo, "full_repo", fullRepo, "err", err)
		if err := page.HTML(w, http.StatusOK, "doc", baseData{Page: pageData}); err != nil {
			logger.Error("failed to render new template", "err", err)
			fmt.Fprint(w, "Unable to render new template.")
			return
		}
	}

	schema := crd.Spec.Validation
	if len(crd.Spec.Versions) > 1 {
		for _, version := range crd.Spec.Versions {
			if version.Storage == true {
				if version.Schema != nil {
					schema = version.Schema
				}
				break
			}
		}
	}

	if schema == nil || schema.OpenAPIV3Schema == nil {
		logger.Warn("CRD schema is nil", "repo", repo, "group", group, "version", version, "kind", kind)
		fmt.Fprint(w, "Supplied CRD has no schema.")
		return
	}

	gvk := crdutil.GetStoredGVK(crd)
	if gvk == nil {
		logger.Warn("CRD GVK is nil", "repo", repo)
		fmt.Fprint(w, "Supplied CRD has no GVK.")
		return
	}

	emitCacheControl(w, longCacheDuration)
	if err := page.HTML(w, http.StatusOK, "doc", docData{
		Page:        pageData,
		Repo:        strings.Join([]string{org, repo}, "/"),
		Tag:         foundTag,
		Group:       gvk.Group,
		Version:     gvk.Version,
		Kind:        gvk.Kind,
		Description: string(schema.OpenAPIV3Schema.Description),
		Schema:      *schema.OpenAPIV3Schema,
	}); err != nil {
		logger.Error("failed to render doc template", "err", err)
		fmt.Fprint(w, "Supplied CRD has no schema.")
		return
	}
	logger.Info("rendered doc template", "org", org, "repo", repo, "group", gvk.Group, "version", gvk.Version, "kind", gvk.Kind)
}

func emitCacheControl(w http.ResponseWriter, duration time.Duration) {
	if duration <= 0 {
		w.Header().Set("Cache-Control", "private, no-cache")
		return
	}

	value := fmt.Sprintf("public, max-age=%d, must-revalidate", int64(duration.Seconds()))
	w.Header().Set("Cache-Control", value)
}
