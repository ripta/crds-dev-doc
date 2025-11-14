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
	"log"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	crdutil "github.com/crdsdev/doc/pkg/crd"
	"github.com/crdsdev/doc/pkg/models"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	flag "github.com/spf13/pflag"
	"github.com/unrolled/render"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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

	cookieDarkMode = "halfmoon_preferredMode"

	address string

	gitterChan     chan models.GitterRepo
	gitterPingTime atomic.Int64
)

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
	Page pageData
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
	Repotags map[string][]string
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
			log.Print("dialing:", err)
			return
		}

		reply := ""
		if err := client.Call("Gitter.Ping", struct{}{}, &reply); err != nil {
			log.Printf("Gitter ping error: %v", err)
		} else {
			gitterPingTime.Store(time.Now().Unix())
			log.Printf("Gitter ping reply: %s", reply)
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

func gitterWorker(gitterChan <-chan models.GitterRepo, gitterAddr string) {
	for job := range gitterChan {
		client, err := rpc.DialHTTP("tcp", gitterAddr)
		if err != nil {
			log.Print("dialing:", err)
			continue
		}

		reply := ""
		if err := client.Call("Gitter.Index", job, &reply); err != nil {
			log.Printf("Gitter could not index %s/%s@%s: %v", job.Org, job.Repo, job.Tag, err)
		} else {
			log.Printf("Gitter indexed %s/%s@%s without error", job.Org, job.Repo, job.Tag)
		}
	}
}

func tryIndex(repo models.GitterRepo, gitterChan chan models.GitterRepo) bool {
	select {
	case gitterChan <- repo:
		return true
	default:
		return false
	}
}

func init() {
	gitterChan = make(chan models.GitterRepo, 4)
}

func main() {
	flag.Parse()
	dsn := os.Getenv("CRDS_DEV_STORAGE_DSN")
	if dsn == "" {
		dsn = fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", os.Getenv(userEnv), os.Getenv(passwordEnv), os.Getenv(hostEnv), os.Getenv(portEnv), os.Getenv(dbEnv))
	}

	conn, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		panic(err)
	}
	db, err = pgxpool.ConnectConfig(context.Background(), conn)
	if err != nil {
		panic(err)
	}

	gitterAddr := defaultGitterAddr
	if value, ok := os.LookupEnv(gitterAddrEnv); ok && value != "" {
		gitterAddr = value
	}

	log.Println("Gitter address:", gitterAddr)

	go gitterPinger(gitterAddr)
	for i := 0; i < 4; i++ {
		go gitterWorker(gitterChan, gitterAddr)
	}

	start()
}

func getPageData(r *http.Request, title string, disableNavBar bool) pageData {
	var isDarkMode = false
	if cookie, err := r.Cookie(cookieDarkMode); err == nil && cookie.Value == "dark-mode" {
		isDarkMode = true
	}
	return pageData{
		IsDarkMode:    isDarkMode,
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

	log.Println("Starting Doc server on", listenAddr)
	r := mux.NewRouter().StrictSlash(true)
	staticHandler := http.StripPrefix("/static/", http.FileServer(http.Dir("./static/")))
	r.HandleFunc("/", home)
	r.PathPrefix("/static/").Handler(staticHandler)
	r.HandleFunc("/gvk/{group}/{version}/{kind}", listGVK)
	r.HandleFunc("/gvk/{group}/{version}", listGroupVersion)
	r.HandleFunc("/gvk/{group}", listGroups)
	r.HandleFunc("/gvk", listAllGroups)
	r.HandleFunc("/repo/github.com/{org}/{repo}@{tag:.+}", org)
	r.HandleFunc("/repo/github.com/{org}/{repo}", listTags)
	r.HandleFunc("/raw/github.com/{org}/{repo}@{tag:.+}", raw)
	r.HandleFunc("/raw/github.com/{org}/{repo}", raw)
	r.PathPrefix("/").HandlerFunc(doc)
	log.Fatal(http.ListenAndServe(listenAddr, r))
}

func home(w http.ResponseWriter, r *http.Request) {
	data := homeData{Page: getPageData(r, "Doc", true)}
	if err := page.HTML(w, http.StatusOK, "home", data); err != nil {
		log.Printf("homeTemplate.Execute(): %v", err)
		fmt.Fprint(w, "Unable to render home template.")
		return
	}
	log.Print("successfully rendered home page")
}

func listGVK(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	group := parameters["group"]
	version := parameters["version"]
	kind := parameters["kind"]

	rows, err := db.Query(context.Background(), "SELECT t.repo, t.name FROM tags t INNER JOIN crds c ON (c.tag_id = t.id) WHERE c.group=$1 AND c.version=$2 AND c.kind=$3;", group, version, kind)
	if err != nil {
		log.Printf("failed to get repos for %s/%s/%s: %v", group, version, kind, err)
		http.Error(w, "Unable to get repositories for supplied GVK.", http.StatusInternalServerError)
		return
	}

	data := listGVKData{
		Page:     getPageData(r, fmt.Sprintf("%s/%s.%s", group, version, kind), false),
		Group:    group,
		Version:  version,
		Kind:     kind,
		Repotags: map[string][]string{},
	}

	for rows.Next() {
		var repo, tag string
		if err := rows.Scan(&repo, &tag); err != nil {
			log.Printf("failed to scan repo row for %s/%s/%s: %v", group, version, kind, err)
			fmt.Fprint(w, "Unable to get repositories for supplied GVK.")
			return
		}

		data.Repotags[repo] = append(data.Repotags[repo], tag)
		data.Total++
	}

	if data.Total == 0 {
		http.Error(w, "GVK not found.", http.StatusNotFound)
		return
	}

	if err := page.HTML(w, http.StatusOK, "list_gvk", data); err != nil {
		log.Printf("listGVKTemplate.Execute(): %v", err)
		fmt.Fprint(w, "Unable to render list GVK template.")
		return
	}
	log.Printf("successfully rendered list GVK template for %s/%s/%s", group, version, kind)
}

func listGroupVersion(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	group := parameters["group"]
	version := parameters["version"]

	rows, err := db.Query(context.Background(), "SELECT c.kind, COUNT(1) FROM crds c WHERE c.group=$1 AND c.version=$2 GROUP BY c.kind;", group, version)
	if err != nil {
		log.Printf("failed to get repos for %s/%s: %v", group, version, err)
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
			log.Printf("failed to scan repo row for %s/%s: %v", group, version, err)
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
		log.Printf("listGroupVersionTemplate.Execute(): %v", err)
		fmt.Fprint(w, "Unable to render list group-version template.")
		return
	}
	log.Printf("successfully rendered list group-version template for %s/%s", group, version)
}

func listGroups(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	group := parameters["group"]

	rows, err := db.Query(context.Background(), "SELECT c.version, COUNT(DISTINCT c.kind) FROM crds c WHERE c.group=$1 GROUP BY c.version;", group)
	if err != nil {
		log.Printf("failed to get versions for %s: %v", group, err)
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
			log.Printf("failed to scan version row for %s: %v", group, err)
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
		log.Printf("listGroupsTemplate.Execute(): %v", err)
		fmt.Fprint(w, "Unable to render list groups template.")
		return
	}
	log.Printf("successfully rendered list groups template for %s", group)
}

func listAllGroups(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(context.Background(), "SELECT c.group, COUNT(DISTINCT c.version), COUNT(DISTINCT c.kind) FROM crds c GROUP BY c.group ORDER BY c.group;")
	if err != nil {
		log.Printf("failed to get all groups: %v", err)
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
			log.Printf("failed to scan all groups row: %v", err)
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
		log.Printf("listAllGroupsTemplate.Execute(): %v", err)
		fmt.Fprint(w, "Unable to render list all groups template.")
		return
	}
	log.Printf("successfully rendered list all groups template")
}

func raw(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	org := parameters["org"]
	repo := parameters["repo"]
	tag := parameters["tag"]

	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", org, repo)
	var rows pgx.Rows
	var err error
	if tag == "" {
		rows, err = db.Query(context.Background(), "SELECT c.data::jsonb FROM tags t INNER JOIN crds c ON (c.tag_id = t.id) WHERE LOWER(t.repo)=LOWER($1) AND t.id = (SELECT id FROM tags WHERE LOWER(repo) = LOWER($1) ORDER BY time DESC LIMIT 1);", fullRepo)
	} else {
		rows, err = db.Query(context.Background(), "SELECT c.data::jsonb FROM tags t INNER JOIN crds c ON (c.tag_id = t.id) WHERE LOWER(t.repo)=LOWER($1) AND t.name=$2;", fullRepo, tag)
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
		crdv1 := &v1.CustomResourceDefinition{}
		if err := v1.Convert_apiextensions_CustomResourceDefinition_To_v1_CustomResourceDefinition(crd, crdv1, nil); err != nil {
			break
		}
		crdv1.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("CustomResourceDefinition"))
		y, err := yaml.Marshal(crdv1)
		if err != nil {
			break
		}
		total = append(total, y...)
		total = append(total, []byte("\n---\n")...)
	}

	if err != nil {
		fmt.Fprint(w, "Unable to render raw CRDs.")
		log.Printf("failed to get raw CRDs for %s (%s): %v", repo, fullRepo, err)
	} else {
		w.Write([]byte(total))
		log.Printf("successfully rendered raw CRDs")
	}
}

type tagInfo struct {
	Name      string
	Timestamp time.Time
}

func listTags(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	org := parameters["org"]
	repo := parameters["repo"]
	pageData := getPageData(r, fmt.Sprintf("%s/%s Tags", org, repo), false)
	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", org, repo)

	rows, err := db.Query(context.Background(), "SELECT name, time FROM tags WHERE LOWER(repo)=LOWER($1) ORDER BY time DESC;", fullRepo)
	if err != nil {
		log.Printf("failed to get tags for %s : %v", repo, err)
		http.Error(w, "Unable to get tags.", http.StatusInternalServerError)
		return
	}

	tags := []tagInfo{}
	for rows.Next() {
		var t string
		var ts time.Time
		if err := rows.Scan(&t, &ts); err != nil {
			log.Printf("listTags(): %v", err)
			fmt.Fprint(w, "Unable to render tags.")
			return
		}

		tags = append(tags, tagInfo{
			Name:      t,
			Timestamp: ts,
		})
	}

	if err := page.HTML(w, http.StatusOK, "list_tags", listTagsData{
		Page:  pageData,
		Repo:  strings.Join([]string{org, repo}, "/"),
		Tags:  tags,
		Total: len(tags),
	}); err != nil {
		log.Printf("listTagsTemplate.Execute(): %v", err)
		fmt.Fprint(w, "Unable to render list tags template.")
		return
	}
	log.Printf("successfully rendered list tags template for %s/%s", org, repo)
}

func org(w http.ResponseWriter, r *http.Request) {
	parameters := mux.Vars(r)
	org := parameters["org"]
	repo := parameters["repo"]
	tag := parameters["tag"]
	pageData := getPageData(r, fmt.Sprintf("%s/%s", org, repo), false)
	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", org, repo)

	b := &pgx.Batch{}
	pageData.Title += fmt.Sprintf("@%s", tag)
	b.Queue("SELECT t.name, c.group, c.version, c.kind FROM tags t INNER JOIN crds c ON (c.tag_id = t.id) WHERE LOWER(t.repo)=LOWER($1) AND t.name=$2;", fullRepo, tag)
	b.Queue("SELECT name, time FROM tags WHERE LOWER(repo)=LOWER($1) ORDER BY time DESC;", fullRepo)
	br := db.SendBatch(context.Background(), b)
	defer br.Close()
	c, err := br.Query()
	if err != nil {
		log.Printf("failed to get CRDs for %s (%s): %v", repo, fullRepo, err)
		if err := page.HTML(w, http.StatusOK, "new", baseData{Page: pageData}); err != nil {
			log.Printf("newTemplate.Execute(): %v", err)
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
			log.Printf("newTemplate.Execute(): %v", err)
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
		log.Printf("failed to get tags for %s : %v", repo, err)
		if err := page.HTML(w, http.StatusOK, "new", baseData{Page: pageData}); err != nil {
			log.Printf("newTemplate.Execute(): %v", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
		return
	}
	tags := []tagInfo{}
	tagExists := false
	for c.Next() {
		var t string
		var ts time.Time
		if err := c.Scan(&t, &ts); err != nil {
			log.Printf("newTemplate.Execute(): %v", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
		if !tagExists && t == tag {
			tagExists = true
		}
		tags = append(tags, tagInfo{
			Name:      t,
			Timestamp: ts,
		})
	}
	if len(tags) == 0 || (!tagExists && tag != "") {
		tryIndex(models.GitterRepo{
			Org:  org,
			Repo: repo,
			Tag:  tag,
		}, gitterChan)
		if err := page.HTML(w, http.StatusOK, "new", baseData{Page: pageData}); err != nil {
			log.Printf("newTemplate.Execute(): %v", err)
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
	if err := page.HTML(w, http.StatusOK, "org", orgData{
		Page:  pageData,
		Repo:  strings.Join([]string{org, repo}, "/"),
		Tag:   foundTag,
		At:    foundTagTimestamp.Format(time.RFC3339),
		Tags:  tags,
		CRDs:  repoCRDs,
		Total: len(repoCRDs),
	}); err != nil {
		log.Printf("orgTemplate.Execute(): %v", err)
		fmt.Fprint(w, "Unable to render org template.")
		return
	}
	log.Printf("successfully rendered org template %s/%s:%s", org, repo, foundTag)
}

func doc(w http.ResponseWriter, r *http.Request) {
	var schema *apiextensions.CustomResourceValidation
	crd := &apiextensions.CustomResourceDefinition{}
	log.Printf("Request Received: %s\n", r.URL.Path)
	org, repo, group, kind, version, tag, err := parseGHURL(strings.TrimPrefix(r.URL.Path, "/repo"))
	if err != nil {
		log.Printf("failed to parse Github path %q: %v", r.URL.Path, err)
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	pageData := getPageData(r, fmt.Sprintf("%s.%s/%s", kind, group, version), false)
	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", org, repo)
	var c pgx.Row
	if tag == "" {
		c = db.QueryRow(context.Background(), "SELECT t.name, c.data::jsonb FROM tags t INNER JOIN crds c ON (c.tag_id = t.id) WHERE LOWER(t.repo)=LOWER($1) AND t.id = (SELECT id FROM tags WHERE repo = $1 ORDER BY time DESC LIMIT 1) AND c.group=$2 AND c.version=$3 AND c.kind=$4;", fullRepo, group, version, kind)
	} else {
		c = db.QueryRow(context.Background(), "SELECT t.name, c.data::jsonb FROM tags t INNER JOIN crds c ON (c.tag_id = t.id) WHERE LOWER(t.repo)=LOWER($1) AND t.name=$2 AND c.group=$3 AND c.version=$4 AND c.kind=$5;", fullRepo, tag, group, version, kind)
	}
	foundTag := tag
	if err := c.Scan(&foundTag, crd); err != nil {
		log.Printf("failed to get CRDs for %s (%s): %v", repo, fullRepo, err)
		if err := page.HTML(w, http.StatusOK, "doc", baseData{Page: pageData}); err != nil {
			log.Printf("newTemplate.Execute(): %v", err)
			fmt.Fprint(w, "Unable to render new template.")
		}
	}
	schema = crd.Spec.Validation
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
		log.Print("CRD schema is nil.")
		fmt.Fprint(w, "Supplied CRD has no schema.")
		return
	}

	gvk := crdutil.GetStoredGVK(crd)
	if gvk == nil {
		log.Print("CRD GVK is nil.")
		fmt.Fprint(w, "Supplied CRD has no GVK.")
		return
	}

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
		log.Printf("docTemplate.Execute(): %v", err)
		fmt.Fprint(w, "Supplied CRD has no schema.")
		return
	}
	log.Printf("successfully rendered doc template")
}

// TODO(hasheddan): add testing and more reliable parse
func parseGHURL(uPath string) (org, repo, group, version, kind, tag string, err error) {
	u, err := url.Parse(uPath)
	if err != nil {
		return "", "", "", "", "", "", err
	}
	elements := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(elements) < 6 {
		return "", "", "", "", "", "", errors.New("invalid path")
	}

	tagSplit := strings.Split(u.Path, "@")
	if len(tagSplit) > 1 {
		tag = tagSplit[1]
	}

	return elements[1], elements[2], elements[3], elements[4], strings.Split(elements[5], "@")[0], tag, nil
}
