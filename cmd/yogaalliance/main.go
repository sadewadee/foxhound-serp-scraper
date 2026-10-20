// Command yogaalliance crawls the public Yoga Alliance directory (teachers +
// schools) via its guest Salesforce LWR Apex API and writes structured,
// 100%-in-niche records to CSV and/or directly into the existing database.
//
// Why: the SERP email-dork pipeline yields data-poor rows (~10% address, ~0%
// niche match). Yoga Alliance is a curated directory of Registered Yoga
// Teachers (RYT) and Schools (RYS) — every record is real and in-niche
// (yoga/wellness/fitness; no spa/massage). TEACHER records expose a published
// email + city + country + their school directly.
//
// API (guest, no auth/CSRF — just cookies):
//
//	POST https://app.yogaalliance.org/webruntime/api/apex/execute?language=en-US&asGuest=true&htmlEncode=false
//	  teacher: classname @udd/01pTR000001kCE1  method getTeacherDetails params {"teacherId":"<id>"}
//	  school : classname @udd/01pTR000001kCED  method getSchoolDetails  params {"schoolId":"<id>"}
//
// Teacher IDs come from app.yogaalliance.org/sitemap.xml (contact-*.xml). School
// IDs come from account-*.xml, but only the non-"household" slugs are real
// schools: probed 2026-10-20, 85,834 account ids of which 5,322 are non-household
// (all in account-5) and getSchoolDetails returns a full record for those, while
// the household ids are rejected with HTTP 400 (9/9 sampled). -mode school crawls
// only the 5,322. Each teacher record also carries its school name/address via
// teachingHistoryList ("assign to school").
//
// DB mapping (uses EXISTING tables — no migration):
//   - emails              ← teacher email (UNIQUE email)
//   - business_listings   ← one row per teacher; business_name = school (if the
//     teacher has one) else teacher name; contact_name =
//     teacher; niche_category='yoga'; off_niche=false;
//     category='yogaalliance'; synthetic unique domain
//     "<id>.ryt.yogaalliance.org" (schools: "<id>.rys.…", business_name =
//     school name, website = the school's own site)
//   - business_emails     ← link, source='yogaalliance'  ← the source tag
//   - business_meta       ← listing_type, designation, yoga_types, languages,
//     teaching_hours, offers_online, yacep, membership_begin, school_name,
//     training_formats. Same semantics as internal/prodir: the keys of a record
//     are replaced, an empty value never blanks an existing row.
//
// Resume: -resume <file> appends every finished id to <file> and skips the ids
// already in it, so an interrupted crawl continues where it stopped (CSV output
// is appended in that mode). Re-running without it is safe too — upserts are
// idempotent — it just re-fetches everything. -rps caps requests per second.
//
// Build & run:
//
//	go build -o yoga ./cmd/yogaalliance
//	./yoga -mode teacher -out teachers.csv -concurrency 12               # CSV only
//	./yoga -mode teacher -insert -concurrency 8                          # → DB (POSTGRES_DSN)
//	./yoga -mode teacher -insert -dsn "postgres://..." -limit 200        # test slice
//	./yoga -mode school -insert -resume ya-school-done.txt               # 5,322 schools
package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"

	appdb "github.com/sadewadee/serp-scraper/internal/db"
)

const (
	apexURL       = "https://app.yogaalliance.org/webruntime/api/apex/execute?language=en-US&asGuest=true&htmlEncode=false"
	sitemapIndex  = "https://app.yogaalliance.org/sitemap.xml"
	bootstrapURL  = "https://app.yogaalliance.org/teacherpublicprofile?id=bootstrap"
	userAgent     = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
	classSchool   = "@udd/01pTR000001kCED"
	classTeacher  = "@udd/01pTR000001kCE1"
	methodSchool  = "getSchoolDetails"
	methodTeacher = "getTeacherDetails"
	srcTag        = "yogaalliance"

	kindTeacher = "teacher"
	kindSchool  = "school"
)

var (
	locRe = regexp.MustCompile(`<loc>([^<]+)</loc>`)
	// idRe captures the record id and the URL slug of a profile <loc>.
	idRe     = regexp.MustCompile(`/(?:school|teacher)publicprofile/([A-Za-z0-9]{15,18})/([^<\s]*)`)
	tagsRe   = regexp.MustCompile(`<[^>]+>`)
	spacesRe = regexp.MustCompile(`\s+`)
	freeMail = map[string]bool{
		"gmail.com": true, "yahoo.com": true, "hotmail.com": true, "outlook.com": true,
		"aol.com": true, "icloud.com": true, "me.com": true, "mac.com": true,
		"live.com": true, "msn.com": true, "comcast.net": true, "gmx.com": true,
		"protonmail.com": true, "ymail.com": true, "yahoo.co.uk": true,
	}
)

type apexReq struct {
	Namespace      string         `json:"namespace"`
	Classname      string         `json:"classname"`
	Method         string         `json:"method"`
	IsContinuation bool           `json:"isContinuation"`
	Params         map[string]any `json:"params"`
	Cacheable      bool           `json:"cacheable"`
}

type teachingLocation struct {
	LocationName  string `json:"locationName"`
	GoogleAddress string `json:"googleAddress"`
	StatusLabel   string `json:"statusLabel"`
}

type teacherResp struct {
	ReturnValue struct {
		Id              string             `json:"Id"`
		DirectoryName   string             `json:"directoryName"`
		FirstName       string             `json:"firstName"`
		LastName        string             `json:"lastName"`
		Email           string             `json:"teacherEmail"`
		EmailPublished  bool               `json:"isEmailPublished"`
		Address         string             `json:"address"`
		MailingCity     string             `json:"mailingCity"`
		MailingState    string             `json:"mailingState"`
		MailingCountry  string             `json:"mailingCountry"`
		Instagram       string             `json:"teacherInstagram"`
		Designation     string             `json:"teacherDesignation"`
		Languages       string             `json:"languages"`
		TypesOfYoga     string             `json:"typesOfYogaTaught"`
		Biography       string             `json:"biography"`
		TeachingHours   float64            `json:"teachingHours"`
		OfferOnline     bool               `json:"offerOnlineClasses"`
		IsYACEP         bool               `json:"isYACEP"`
		MembershipBegin string             `json:"originalMembershipBeginDate"`
		Published       bool               `json:"isProfilePublished"`
		TeachingHistory []teachingLocation `json:"teachingHistoryList"`
	} `json:"returnValue"`
}

type schoolResp struct {
	ReturnValue struct {
		Id              string `json:"Id"`
		DirectoryName   string `json:"directoryName"`
		Email           string `json:"schoolEmail"`
		Address         string `json:"address"`
		Website         string `json:"schoolWebsite"`
		Facebook        string `json:"schoolFacebook"`
		Instagram       string `json:"schoolInstagram"`
		Designation     string `json:"schoolDesignation"`
		TypesOfYoga     string `json:"typesOfYogaTaught"`
		TrainingFormats string `json:"trainingFormats"`
		Biography       string `json:"biography"`
		MembershipBegin string `json:"originalMembershipBeginDate"`
	} `json:"returnValue"`
}

// entry is one directory record, a teacher (RYT) or a school (RYS).
type entry struct {
	kind                                           string
	id, name, email, city, state, country, address string
	instagram, facebook, website                   string
	designation, languages, yogaTypes              string
	trainingFormats, membershipBegin               string
	biography, schoolName, schoolAddr              string
	emailPublished, yacep, online                  bool
	teachingHours                                  float64
}

func main() {
	mode := flag.String("mode", kindTeacher, "teacher | school")
	out := flag.String("out", "", "output CSV path ('' = none when -insert, else <mode>s.csv)")
	conc := flag.Int("concurrency", 10, "concurrent requests")
	rps := flag.Float64("rps", 5, "max requests per second across all workers (0 = unlimited)")
	limit := flag.Int("limit", 0, "max records (0 = all)")
	idsFile := flag.String("ids", "", "optional file of IDs (one per line); default = crawl sitemap")
	resume := flag.String("resume", "", "file of finished IDs: skipped on start, appended as records finish")
	doInsert := flag.Bool("insert", false, "upsert into business_listings/emails/business_emails/business_meta")
	dsn := flag.String("dsn", os.Getenv("POSTGRES_DSN"), "Postgres DSN (default $POSTGRES_DSN)")
	flag.Parse()

	if *mode != kindTeacher && *mode != kindSchool {
		log.Fatalf("-mode must be teacher or school, got %q", *mode)
	}

	var db *sql.DB
	if *doInsert {
		if *dsn == "" {
			log.Fatal("-insert requires -dsn or $POSTGRES_DSN")
		}
		var err error
		db, err = sql.Open("postgres", *dsn)
		if err != nil {
			log.Fatalf("db open: %v", err)
		}
		db.SetMaxOpenConns(*conc + 2)
		if err := db.Ping(); err != nil {
			log.Fatalf("db ping: %v", err)
		}
		defer db.Close()
		if err := appdb.EnsureBusinessMeta(context.Background(), db); err != nil {
			log.Fatal(err)
		}
		log.Printf("yogaalliance: INSERT mode → DB (source=%s)", srcTag)
	}

	outPath := *out
	if outPath == "" && !*doInsert {
		outPath = *mode + "s.csv"
	}

	client := newClient()
	bootstrap(client)

	var ids []string
	if *idsFile != "" {
		ids = readLines(*idsFile)
	} else {
		ids = crawlSitemapIDs(client, *mode)
	}
	var doneFile *os.File
	var doneMu sync.Mutex
	if *resume != "" {
		skip := map[string]bool{}
		if _, err := os.Stat(*resume); err == nil {
			for _, id := range readLines(*resume) {
				skip[id] = true
			}
		}
		var todo []string
		for _, id := range ids {
			if !skip[id] {
				todo = append(todo, id)
			}
		}
		log.Printf("yogaalliance: resume — %d of %d IDs already done", len(ids)-len(todo), len(ids))
		ids = todo
		var err error
		if doneFile, err = os.OpenFile(*resume, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
			log.Fatal(err)
		}
		defer doneFile.Close()
	}
	if *limit > 0 && len(ids) > *limit {
		ids = ids[:*limit]
	}
	log.Printf("yogaalliance: %s — %d IDs (concurrency=%d, rps=%g, insert=%v, csv=%q)", *mode, len(ids), *conc, *rps, *doInsert, outPath)

	var w *csv.Writer
	var csvMu sync.Mutex
	if outPath != "" {
		flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
		if *resume != "" {
			flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
		}
		f, err := os.OpenFile(outPath, flags, 0o600)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		w = csv.NewWriter(f)
		defer w.Flush()
		if st, err := f.Stat(); err == nil && st.Size() == 0 {
			w.Write(csvHeader)
		}
	}

	var tick <-chan time.Time
	if *rps > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / *rps))
		defer t.Stop()
		tick = t.C
	}

	var done, ok, withEmail, inserted int64
	jobs := make(chan string, *conc*2)
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				if tick != nil {
					<-tick
				}
				e, final := fetchEntry(client, *mode, id)
				n := atomic.AddInt64(&done, 1)
				persisted := final
				if e != nil {
					atomic.AddInt64(&ok, 1)
					if e.email != "" {
						atomic.AddInt64(&withEmail, 1)
					}
					if w != nil {
						csvMu.Lock()
						w.Write(e.row())
						if n%500 == 0 {
							w.Flush()
						}
						csvMu.Unlock()
					}
					if db != nil {
						if err := upsertEntry(db, e); err != nil {
							log.Printf("  upsert %s failed: %v", e.id, err)
							persisted = false
						} else {
							atomic.AddInt64(&inserted, 1)
						}
					}
				}
				if doneFile != nil && persisted {
					doneMu.Lock()
					fmt.Fprintln(doneFile, id)
					doneMu.Unlock()
				}
				if n%1000 == 0 {
					log.Printf("  progress: %d/%d done, %d ok, %d email, %d inserted", n, len(ids), atomic.LoadInt64(&ok), atomic.LoadInt64(&withEmail), atomic.LoadInt64(&inserted))
				}
			}
		}()
	}
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	wg.Wait()
	if w != nil {
		w.Flush()
	}
	log.Printf("DONE: %d fetched, %d ok, %d with email, %d inserted", done, ok, withEmail, inserted)
}

var csvHeader = []string{"id", "kind", "name", "email", "email_published", "city", "state", "country", "address", "school_name", "school_address", "instagram", "facebook", "website", "designation", "languages", "yoga_types", "training_formats", "yacep", "offers_online", "teaching_hours", "membership_begin"}

func (e *entry) row() []string {
	return []string{
		e.id, e.kind, e.name, e.email, strconv.FormatBool(e.emailPublished), e.city, e.state, e.country, e.address,
		e.schoolName, e.schoolAddr, e.instagram, e.facebook, e.website, e.designation, e.languages, e.yogaTypes, e.trainingFormats,
		strconv.FormatBool(e.yacep), strconv.FormatBool(e.online), strconv.FormatFloat(e.teachingHours, 'f', -1, 64), e.membershipBegin,
	}
}

func (e *entry) domain() string {
	if e.kind == kindSchool {
		return strings.ToLower(e.id) + ".rys.yogaalliance.org"
	}
	return strings.ToLower(e.id) + ".ryt.yogaalliance.org"
}

func (e *entry) profileURL() string {
	return "https://app.yogaalliance.org/" + e.kind + "publicprofile?id=" + e.id
}

// meta is the entry's business_meta: key -> values. Empty values are omitted
// (they never blank an existing row); booleans are only written when true.
func (e *entry) meta() map[string][]string {
	m := map[string][]string{}
	add := func(key string, vals ...string) {
		for _, v := range vals {
			if v = clean(v); v != "" {
				m[key] = append(m[key], v)
			}
		}
	}
	add("listing_type", e.kind)
	add("designation", splitMeta(e.designation)...)
	add("yoga_types", splitMeta(e.yogaTypes)...)
	add("languages", splitMeta(e.languages)...)
	add("training_formats", splitMeta(e.trainingFormats)...)
	add("membership_begin", e.membershipBegin)
	add("school_name", e.schoolName)
	if e.teachingHours > 0 {
		add("teaching_hours", strconv.FormatFloat(e.teachingHours, 'f', -1, 64))
	}
	if e.online {
		add("offers_online", "true")
	}
	if e.yacep {
		add("yacep", "true")
	}
	return m
}

// splitMeta splits a multi-value string into its values: the API joins with ", "
// (sometimes a trailing comma). The *List fields are not used: they re-split the
// same string and break "Part-time (spread out over time)" in two.
func splitMeta(s string) []string {
	return strings.Split(s, ",")
}

func newClient() *http.Client {
	jar, _ := cookiejar.New(nil)
	return &http.Client{Jar: jar, Timeout: 30 * time.Second}
}

func bootstrap(c *http.Client) {
	req, _ := http.NewRequest("GET", bootstrapURL, nil)
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.Do(req)
	if err != nil {
		log.Fatalf("bootstrap failed: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// crawlSitemapIDs lists the record ids of the mode from the sitemap: teachers
// from contact-*.xml, schools from account-*.xml minus the "household" slugs
// (individual accounts that getSchoolDetails rejects).
func crawlSitemapIDs(c *http.Client, mode string) []string {
	prefix := "sitemap-contact"
	if mode == kindSchool {
		prefix = "sitemap-account"
	}
	idxBody := httpGet(c, sitemapIndex)
	var subs []string
	for _, m := range locRe.FindAllStringSubmatch(string(idxBody), -1) {
		if strings.Contains(m[1], prefix) && !strings.Contains(m[1], "weekly") {
			subs = append(subs, m[1])
		}
	}
	seen := map[string]bool{}
	var ids []string
	for _, sm := range subs {
		body := httpGet(c, sm)
		for _, id := range sitemapRecordIDs(string(body), mode) {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		log.Printf("  %s → %d unique IDs so far", sm[strings.LastIndex(sm, "/")+1:], len(ids))
	}
	return ids
}

func sitemapRecordIDs(body, mode string) []string {
	var ids []string
	for _, m := range idRe.FindAllStringSubmatch(body, -1) {
		if mode == kindSchool && strings.Contains(strings.ToLower(m[2]), "household") {
			continue
		}
		ids = append(ids, m[1])
	}
	return ids
}

// fetchEntry returns the record for id. final is false when the fetch failed
// transiently (network / 5xx) so the id must be retried on a later run; a nil
// record with final=true means the id is definitively not a valid profile.
func fetchEntry(c *http.Client, mode, id string) (e *entry, final bool) {
	class, method, param, parse := classTeacher, methodTeacher, "teacherId", parseTeacher
	if mode == kindSchool {
		class, method, param, parse = classSchool, methodSchool, "schoolId", parseSchool
	}
	reqBody, _ := json.Marshal(apexReq{Classname: class, Method: method, Params: map[string]any{param: id}})
	for attempt := 0; attempt < 3; attempt++ {
		req, _ := http.NewRequest("POST", apexURL, bytes.NewReader(reqBody))
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		resp, err := c.Do(req)
		if err != nil {
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusBadRequest {
			return nil, true // "No valid ... profile found with the provided Id."
		}
		if resp.StatusCode != 200 {
			time.Sleep(time.Duration(attempt+1) * 400 * time.Millisecond)
			continue
		}
		return parse(body), true
	}
	return nil, false
}

func parseTeacher(body []byte) *entry {
	var r teacherResp
	if json.Unmarshal(body, &r) != nil || r.ReturnValue.Id == "" {
		return nil
	}
	v := r.ReturnValue
	name := clean(v.DirectoryName)
	if name == "" {
		name = clean(strings.TrimSpace(v.FirstName + " " + v.LastName))
	}
	schoolName, schoolAddr := pickSchool(v.TeachingHistory)
	return &entry{
		kind: kindTeacher,
		id:   v.Id, name: name, email: strings.TrimSpace(v.Email), emailPublished: v.EmailPublished,
		city: clean(v.MailingCity), state: clean(v.MailingState), country: clean(v.MailingCountry),
		address: clean(v.Address), schoolName: schoolName, schoolAddr: schoolAddr,
		instagram: strings.TrimSpace(v.Instagram), designation: clean(v.Designation),
		languages: clean(v.Languages), yogaTypes: clean(v.TypesOfYoga), biography: clean(v.Biography),
		yacep: v.IsYACEP, online: v.OfferOnline, teachingHours: v.TeachingHours,
		membershipBegin: clean(v.MembershipBegin),
	}
}

// parseSchool maps getSchoolDetails. The API gives one free-text address (no
// city/country fields), so city/state/country stay empty rather than guessed.
func parseSchool(body []byte) *entry {
	var r schoolResp
	if json.Unmarshal(body, &r) != nil || r.ReturnValue.Id == "" {
		return nil
	}
	v := r.ReturnValue
	return &entry{
		kind: kindSchool,
		id:   v.Id, name: clean(v.DirectoryName), email: strings.TrimSpace(v.Email), emailPublished: v.Email != "",
		address: clean(v.Address), website: strings.TrimSpace(v.Website),
		instagram: strings.TrimSpace(v.Instagram), facebook: strings.TrimSpace(v.Facebook),
		designation: clean(v.Designation), yogaTypes: clean(v.TypesOfYoga),
		trainingFormats: clean(v.TrainingFormats), biography: clean(v.Biography),
		membershipBegin: clean(v.MembershipBegin),
	}
}

// pickSchool returns the teacher's primary school: the first "Current" location
// that has a real street address, else the first Current, else the first.
func pickSchool(hist []teachingLocation) (name, addr string) {
	var fallback *teachingLocation
	for i := range hist {
		h := &hist[i]
		if fallback == nil {
			fallback = h
		}
		if strings.EqualFold(h.StatusLabel, "Current") {
			if strings.TrimSpace(h.GoogleAddress) != "" {
				return clean(h.LocationName), clean(h.GoogleAddress)
			}
			if name == "" {
				name, addr = clean(h.LocationName), clean(h.GoogleAddress)
			}
		}
	}
	if name != "" {
		return name, addr
	}
	if fallback != nil {
		return clean(fallback.LocationName), clean(fallback.GoogleAddress)
	}
	return "", ""
}

// upsertEntry writes one record into the existing schema, tagged source=yogaalliance.
func upsertEntry(db *sql.DB, e *entry) error {
	bizName := e.schoolName
	if bizName == "" {
		bizName = e.name
	}
	if e.kind == kindSchool {
		bizName = e.name
	}
	addr := e.address
	if addr == "" {
		addr = e.schoolAddr
	}
	profileURL := e.profileURL()
	website := e.website
	if website == "" {
		website = profileURL
	}

	social := map[string]string{}
	if e.instagram != "" {
		social["instagram"] = e.instagram
	}
	if e.facebook != "" {
		social["facebook"] = e.facebook
	}
	socialJSON, _ := json.Marshal(social)

	var bizID int64
	err := db.QueryRow(`
		INSERT INTO business_listings
		  (domain, url, business_name, contact_name, address, city, country, description,
		   social_links, niche_category, off_niche, category, website, created_at, updated_at)
		VALUES ($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''),NULLIF($7,''),NULLIF($8,''),
		   $9::jsonb,'yoga',false,'yogaalliance',$10,NOW(),NOW())
		ON CONFLICT (domain) DO UPDATE SET
		  business_name  = COALESCE(NULLIF(EXCLUDED.business_name,''), business_listings.business_name),
		  contact_name   = COALESCE(NULLIF(EXCLUDED.contact_name,''),  business_listings.contact_name),
		  address        = COALESCE(business_listings.address, EXCLUDED.address),
		  city           = COALESCE(business_listings.city,    EXCLUDED.city),
		  country        = COALESCE(business_listings.country, EXCLUDED.country),
		  niche_category = 'yoga', off_niche = false, category = 'yogaalliance',
		  updated_at     = NOW()
		RETURNING id`,
		e.domain(), profileURL, bizName, e.name, addr, e.city, e.country, e.biography, string(socialJSON), website,
	).Scan(&bizID)
	if err != nil {
		return fmt.Errorf("business_listings upsert: %w", err)
	}

	if err := writeMeta(db, bizID, e.meta()); err != nil {
		return err
	}

	if e.email == "" {
		return nil
	}
	at := strings.LastIndex(e.email, "@")
	if at < 1 {
		return nil
	}
	emailDomain := strings.ToLower(e.email[at+1:])
	localPart := e.email[:at]

	var emailID int64
	err = db.QueryRow(`
		INSERT INTO emails (email, domain, local_part, free_email, created_at)
		VALUES ($1,$2,$3,$4,NOW())
		ON CONFLICT (email) DO UPDATE SET domain = EXCLUDED.domain
		RETURNING id`,
		strings.ToLower(e.email), emailDomain, localPart, freeMail[emailDomain],
	).Scan(&emailID)
	if err != nil {
		return fmt.Errorf("emails upsert: %w", err)
	}

	_, err = db.Exec(`
		INSERT INTO business_emails (business_id, email_id, source)
		VALUES ($1,$2,$3) ON CONFLICT (business_id, email_id) DO NOTHING`,
		bizID, emailID, srcTag)
	if err != nil {
		return fmt.Errorf("business_emails link: %w", err)
	}
	return nil
}

// writeMeta replaces the business_meta rows of the record's keys; keys the
// record does not carry keep their old rows (same as internal/prodir).
func writeMeta(db *sql.DB, bizID int64, m map[string][]string) error {
	if len(m) == 0 {
		return nil
	}
	var keys, mk, mv []string
	for k, vals := range m {
		keys = append(keys, k)
		for _, v := range vals {
			mk, mv = append(mk, k), append(mv, v)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("business_meta begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM business_meta WHERE business_id = $1 AND meta_key = ANY($2::text[])`,
		bizID, pq.Array(keys)); err != nil {
		return fmt.Errorf("business_meta delete: %w", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO business_meta (business_id, meta_key, meta_value)
		SELECT $1, k, v FROM unnest($2::text[], $3::text[]) AS t(k, v)
		ON CONFLICT DO NOTHING`,
		bizID, pq.Array(mk), pq.Array(mv)); err != nil {
		return fmt.Errorf("business_meta insert: %w", err)
	}
	return tx.Commit()
}

func clean(s string) string {
	s = tagsRe.ReplaceAllString(s, " ")
	return strings.TrimSpace(spacesRe.ReplaceAllString(s, " "))
}

func httpGet(c *http.Client, url string) []byte {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.Do(req)
	if err != nil {
		log.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b
}

func readLines(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if s := strings.TrimSpace(sc.Text()); s != "" {
			out = append(out, s)
		}
	}
	return out
}
