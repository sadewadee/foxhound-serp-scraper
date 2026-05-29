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
// Teacher IDs come from app.yogaalliance.org/sitemap.xml (contact-*.xml). The
// account-*.xml "schools" are mostly individual household accounts that
// getSchoolDetails rejects, so the default crawl is teachers — each teacher
// record carries its school via teachingHistoryList ("assign to school").
//
// DB mapping (uses EXISTING tables — no migration):
//   - emails              ← teacher email (UNIQUE email)
//   - business_listings   ← one row per teacher; business_name = school (if the
//     teacher has one) else teacher name; contact_name =
//     teacher; niche_category='yoga'; off_niche=false;
//     category='yogaalliance'; synthetic unique domain
//     "<id>.ryt.yogaalliance.org"
//   - business_emails     ← link, source='yogaalliance'  ← the source tag
//
// Build & run:
//
//	go build -o yoga ./cmd/yogaalliance
//	./yoga -mode teacher -out teachers.csv -concurrency 12               # CSV only
//	./yoga -mode teacher -insert -concurrency 8                          # → DB (POSTGRES_DSN)
//	./yoga -mode teacher -insert -dsn "postgres://..." -limit 200        # test slice
package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"html"
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

	_ "github.com/lib/pq"
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

	// Directory search controller (YaDirectorySearchController). fetchSchoolRecords
	// with EMPTY location fields returns ALL published RYS schools globally (~6.8K),
	// paginated; max pageSize is 25 (50+ errors). fetchSchoolRecordsCount gives the
	// total. Discovered via the /directory page network calls (2026-05-29).
	classSchoolSearch   = "@udd/01pTR000001kCE3"
	methodSchoolRecords = "fetchSchoolRecords"
	methodSchoolCount   = "fetchSchoolRecordsCount"
	schoolPageSize      = 25
	schoolSrcTag        = "yogaalliance-school"
)

var (
	locRe    = regexp.MustCompile(`<loc>([^<]+)</loc>`)
	idRe     = regexp.MustCompile(`/(?:school|teacher)publicprofile/([A-Za-z0-9]{15,18})/`)
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

type teacher struct {
	id, name, email, city, state, country, address string
	instagram, designation, languages, yogaTypes   string
	biography, schoolName, schoolAddr              string
	emailPublished, yacep, online                  bool
	teachingHours                                  float64
}

func main() {
	mode := flag.String("mode", "teacher", "teacher | school")
	out := flag.String("out", "", "output CSV path ('' = none when -insert, else <mode>s.csv)")
	conc := flag.Int("concurrency", 10, "concurrent requests")
	limit := flag.Int("limit", 0, "max records (0 = all)")
	idsFile := flag.String("ids", "", "optional file of IDs (one per line); default = crawl sitemap")
	doInsert := flag.Bool("insert", false, "upsert into business_listings/emails/business_emails")
	dsn := flag.String("dsn", os.Getenv("POSTGRES_DSN"), "Postgres DSN (default $POSTGRES_DSN)")
	flag.Parse()

	if *mode != "teacher" && *mode != "school" {
		log.Fatalf("-mode must be 'teacher' or 'school'")
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
		log.Printf("yogaalliance: INSERT mode → DB (source=%s)", srcTag)
	}

	outPath := *out
	if outPath == "" && !*doInsert {
		outPath = *mode + "s.csv"
	}

	client := newClient()
	bootstrap(client)

	if *mode == "school" {
		runSchools(client, db, *conc, *limit, outPath)
		return
	}

	var ids []string
	if *idsFile != "" {
		ids = readLines(*idsFile)
	} else {
		ids = crawlSitemapIDs(client)
	}
	if *limit > 0 && len(ids) > *limit {
		ids = ids[:*limit]
	}
	log.Printf("yogaalliance: teacher — %d IDs (concurrency=%d, insert=%v, csv=%q)", len(ids), *conc, *doInsert, outPath)

	var w *csv.Writer
	var csvMu sync.Mutex
	if outPath != "" {
		f, err := os.Create(outPath)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		w = csv.NewWriter(f)
		defer w.Flush()
		w.Write([]string{"id", "name", "email", "email_published", "city", "state", "country", "address", "school_name", "school_address", "instagram", "designation", "languages", "yoga_types", "yacep", "teaching_hours"})
	}

	var done, ok, withEmail, inserted int64
	jobs := make(chan string, *conc*2)
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				t := fetchTeacher(client, id)
				n := atomic.AddInt64(&done, 1)
				if t != nil {
					atomic.AddInt64(&ok, 1)
					if t.email != "" {
						atomic.AddInt64(&withEmail, 1)
					}
					if w != nil {
						csvMu.Lock()
						w.Write(t.row())
						if n%500 == 0 {
							w.Flush()
						}
						csvMu.Unlock()
					}
					if db != nil {
						if err := upsertTeacher(db, t); err != nil {
							log.Printf("  upsert %s failed: %v", t.id, err)
						} else {
							atomic.AddInt64(&inserted, 1)
						}
					}
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

func (t *teacher) row() []string {
	return []string{
		t.id, t.name, t.email, strconv.FormatBool(t.emailPublished), t.city, t.state, t.country, t.address,
		t.schoolName, t.schoolAddr, t.instagram, t.designation, t.languages, t.yogaTypes,
		strconv.FormatBool(t.yacep), strconv.FormatFloat(t.teachingHours, 'f', -1, 64),
	}
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

func crawlSitemapIDs(c *http.Client) []string {
	idxBody := httpGet(c, sitemapIndex)
	var subs []string
	for _, m := range locRe.FindAllStringSubmatch(string(idxBody), -1) {
		if strings.Contains(m[1], "sitemap-contact") && !strings.Contains(m[1], "weekly") {
			subs = append(subs, m[1])
		}
	}
	seen := map[string]bool{}
	var ids []string
	for _, sm := range subs {
		body := httpGet(c, sm)
		for _, m := range idRe.FindAllStringSubmatch(string(body), -1) {
			if !seen[m[1]] {
				seen[m[1]] = true
				ids = append(ids, m[1])
			}
		}
		log.Printf("  %s → %d unique IDs so far", sm[strings.LastIndex(sm, "/")+1:], len(ids))
	}
	return ids
}

func fetchTeacher(c *http.Client, id string) *teacher {
	reqBody, _ := json.Marshal(apexReq{
		Classname: classTeacher, Method: methodTeacher, Params: map[string]any{"teacherId": id},
	})
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
		if resp.StatusCode != 200 {
			time.Sleep(time.Duration(attempt+1) * 400 * time.Millisecond)
			continue
		}
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
		return &teacher{
			id: v.Id, name: name, email: strings.TrimSpace(v.Email), emailPublished: v.EmailPublished,
			city: clean(v.MailingCity), state: clean(v.MailingState), country: clean(v.MailingCountry),
			address: clean(v.Address), schoolName: schoolName, schoolAddr: schoolAddr,
			instagram: strings.TrimSpace(v.Instagram), designation: clean(v.Designation),
			languages: clean(v.Languages), yogaTypes: clean(v.TypesOfYoga), biography: clean(v.Biography),
			yacep: v.IsYACEP, online: v.OfferOnline, teachingHours: v.TeachingHours,
		}
	}
	return nil
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

// upsertTeacher writes one teacher into the existing schema, tagged source=yogaalliance.
func upsertTeacher(db *sql.DB, t *teacher) error {
	bizName := t.schoolName
	if bizName == "" {
		bizName = t.name
	}
	addr := t.address
	if addr == "" {
		addr = t.schoolAddr
	}
	domain := strings.ToLower(t.id) + ".ryt.yogaalliance.org"
	profileURL := "https://app.yogaalliance.org/teacherpublicprofile?id=" + t.id

	social := map[string]string{}
	if t.instagram != "" {
		social["instagram"] = t.instagram
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
		domain, profileURL, bizName, t.name, addr, t.city, t.country, t.biography, string(socialJSON), profileURL,
	).Scan(&bizID)
	if err != nil {
		return fmt.Errorf("business_listings upsert: %w", err)
	}

	if t.email == "" {
		return nil
	}
	at := strings.LastIndex(t.email, "@")
	if at < 1 {
		return nil
	}
	emailDomain := strings.ToLower(t.email[at+1:])
	localPart := t.email[:at]

	var emailID int64
	err = db.QueryRow(`
		INSERT INTO emails (email, domain, local_part, free_email, created_at)
		VALUES ($1,$2,$3,$4,NOW())
		ON CONFLICT (email) DO UPDATE SET domain = EXCLUDED.domain
		RETURNING id`,
		strings.ToLower(t.email), emailDomain, localPart, freeMail[emailDomain],
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

func clean(s string) string {
	s = tagsRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s) // decode &amp; &#39; etc. (entity bug, 2026-05-29)
	return strings.TrimSpace(spacesRe.ReplaceAllString(s, " "))
}

// ---- School crawl (RYS directory) ----

type schoolListResp struct {
	ReturnValue []schoolRec `json:"returnValue"`
}
type schoolRec struct {
	Id            string `json:"Id"`
	DirectoryName string `json:"directoryName"`
	Address       string `json:"address"`
	Website       string `json:"schoolWebsite"`
	Designation   string `json:"schoolDesignation"`
	ParentName    string `json:"parentName"`
}
type schoolDetailResp struct {
	ReturnValue struct {
		Id            string `json:"Id"`
		DirectoryName string `json:"directoryName"`
		Address       string `json:"address"`
		Website       string `json:"schoolWebsite"`
		Email         string `json:"schoolEmail"`
		Instagram     string `json:"schoolInstagram"`
		Facebook      string `json:"schoolFacebook"`
		Twitter       string `json:"schoolTwitter"`
		Biography     string `json:"biography"`
		Designation   string `json:"schoolDesignation"`
		ParentName    string `json:"parentName"`
		Languages     string `json:"languages"`
		TypesOfYoga   string `json:"typesOfYogaTaught"`
	} `json:"returnValue"`
}
type school struct {
	id, name, address, website, email string
	instagram, facebook, twitter, bio string
	designation, parentName, yoga     string
}

// searchParams builds the fetchSchoolRecords/Count param map. Empty location =>
// global (all published RYS schools); page* only used by the records call.
func searchParams(pageSize, pageNumber int) map[string]any {
	return map[string]any{
		"schoolId": "", "schoolGoogleAddress": "", "schoolLatitude": 0, "schoolLongitude": 0,
		"schoolStreet": "", "schoolCity": "", "schoolState": "", "schoolCountry": "",
		"searchRadius": 50, "name": "", "designation": "",
		"onlineServices": false, "closedCaptioning": false,
		"selectedDesignations": []any{}, "selectedTrainingFormats": []any{},
		"offerScholarship": false, "offerExchangePrograms": false,
		"acceptsMyCAA": false, "acceptsGiBill": false,
		"ratings": []any{}, "typesOfYoga": []any{}, "language": "",
		"sortDirection": "ASC", "pageSize": pageSize, "pageNumber": pageNumber, "SearchFlag": true,
	}
}

func apexPOST(c *http.Client, class, method string, params map[string]any) ([]byte, error) {
	reqBody, _ := json.Marshal(apexReq{Classname: class, Method: method, Params: params})
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, _ := http.NewRequest("POST", apexURL, bytes.NewReader(reqBody))
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		resp, err := c.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			time.Sleep(time.Duration(attempt+1) * 400 * time.Millisecond)
			continue
		}
		return body, nil
	}
	return nil, lastErr
}

func fetchSchoolCount(c *http.Client) int {
	body, err := apexPOST(c, classSchoolSearch, methodSchoolCount, searchParams(schoolPageSize, 1))
	if err != nil {
		return 0
	}
	var r struct {
		ReturnValue int `json:"returnValue"`
	}
	json.Unmarshal(body, &r)
	return r.ReturnValue
}

// fetchSchoolList paginates the global RYS directory and returns unique school
// records (deduped by Id). limit caps the count (0 = all).
func fetchSchoolList(c *http.Client, limit int) []schoolRec {
	seen := map[string]bool{}
	var out []schoolRec
	for page := 1; ; page++ {
		body, err := apexPOST(c, classSchoolSearch, methodSchoolRecords, searchParams(schoolPageSize, page))
		if err != nil {
			log.Printf("  school list page %d: %v", page, err)
			break
		}
		var r schoolListResp
		if json.Unmarshal(body, &r) != nil || len(r.ReturnValue) == 0 {
			break
		}
		for _, rec := range r.ReturnValue {
			if rec.Id == "" || seen[rec.Id] {
				continue
			}
			seen[rec.Id] = true
			out = append(out, rec)
		}
		if page%20 == 0 {
			log.Printf("  enumerated %d unique schools (page %d)", len(out), page)
		}
		if limit > 0 && len(out) >= limit {
			out = out[:limit]
			break
		}
	}
	return out
}

// fetchSchool merges the list record with getSchoolDetails (email/social/bio).
func fetchSchool(c *http.Client, rec schoolRec) *school {
	s := &school{
		id: rec.Id, name: clean(rec.DirectoryName), address: clean(rec.Address),
		website: strings.TrimSpace(rec.Website), designation: clean(rec.Designation),
		parentName: clean(rec.ParentName),
	}
	body, err := apexPOST(c, classSchool, methodSchool, map[string]any{"schoolId": rec.Id})
	if err == nil {
		var d schoolDetailResp
		if json.Unmarshal(body, &d) == nil && d.ReturnValue.Id != "" {
			v := d.ReturnValue
			if clean(v.DirectoryName) != "" {
				s.name = clean(v.DirectoryName)
			}
			if clean(v.Address) != "" {
				s.address = clean(v.Address)
			}
			if strings.TrimSpace(v.Website) != "" {
				s.website = strings.TrimSpace(v.Website)
			}
			s.email = strings.TrimSpace(v.Email)
			s.instagram = strings.TrimSpace(v.Instagram)
			s.facebook = strings.TrimSpace(v.Facebook)
			s.twitter = strings.TrimSpace(v.Twitter)
			s.bio = clean(v.Biography)
			s.yoga = clean(v.TypesOfYoga)
		}
	}
	return s
}

func (s *school) row() []string {
	return []string{s.id, s.name, s.email, s.address, s.website, s.instagram, s.facebook, s.twitter, s.designation, s.parentName, s.yoga}
}

func runSchools(c *http.Client, db *sql.DB, conc, limit int, outPath string) {
	total := fetchSchoolCount(c)
	log.Printf("yogaalliance: school — directory reports %d published RYS schools globally", total)
	recs := fetchSchoolList(c, limit)
	log.Printf("yogaalliance: school — %d unique schools to fetch (concurrency=%d, insert=%v, csv=%q)", len(recs), conc, db != nil, outPath)

	var w *csv.Writer
	var csvMu sync.Mutex
	if outPath != "" {
		f, err := os.Create(outPath)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		w = csv.NewWriter(f)
		defer w.Flush()
		w.Write([]string{"id", "name", "email", "address", "website", "instagram", "facebook", "twitter", "designation", "parent_name", "yoga_types"})
	}

	var done, ok, withEmail, withWeb, inserted int64
	jobs := make(chan schoolRec, conc*2)
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rec := range jobs {
				s := fetchSchool(c, rec)
				n := atomic.AddInt64(&done, 1)
				if s != nil && s.name != "" {
					atomic.AddInt64(&ok, 1)
					if s.email != "" {
						atomic.AddInt64(&withEmail, 1)
					}
					if s.website != "" {
						atomic.AddInt64(&withWeb, 1)
					}
					if w != nil {
						csvMu.Lock()
						w.Write(s.row())
						if n%200 == 0 {
							w.Flush()
						}
						csvMu.Unlock()
					}
					if db != nil {
						if err := upsertSchool(db, s); err != nil {
							log.Printf("  upsert school %s failed: %v", s.id, err)
						} else {
							atomic.AddInt64(&inserted, 1)
						}
					}
				}
				if n%200 == 0 {
					log.Printf("  progress: %d/%d done, %d ok, %d web, %d email, %d inserted",
						n, len(recs), atomic.LoadInt64(&ok), atomic.LoadInt64(&withWeb), atomic.LoadInt64(&withEmail), atomic.LoadInt64(&inserted))
				}
			}
		}()
	}
	for _, rec := range recs {
		jobs <- rec
	}
	close(jobs)
	wg.Wait()
	if w != nil {
		w.Flush()
	}
	log.Printf("DONE schools: %d fetched, %d ok, %d with website, %d with email, %d inserted", done, ok, withWeb, withEmail, inserted)
}

// upsertSchool writes one RYS school as a standalone listing (category
// yogaalliance-school), synthetic unique domain "<id>.rys.yogaalliance.org",
// real studio site in website, socials in social_links.
func upsertSchool(db *sql.DB, s *school) error {
	domain := strings.ToLower(s.id) + ".rys.yogaalliance.org"
	profileURL := "https://app.yogaalliance.org/schoolpublicprofile?id=" + s.id

	social := map[string]string{}
	if s.instagram != "" {
		social["instagram"] = s.instagram
	}
	if s.facebook != "" {
		social["facebook"] = s.facebook
	}
	if s.twitter != "" {
		social["twitter"] = s.twitter
	}
	socialJSON, _ := json.Marshal(social)

	var bizID int64
	err := db.QueryRow(`
		INSERT INTO business_listings
		  (domain, url, business_name, contact_name, address, description,
		   social_links, niche_category, off_niche, category, website, created_at, updated_at)
		VALUES ($1,$2,$3,'',NULLIF($4,''),NULLIF($5,''),
		   $6::jsonb,'yoga',false,'yogaalliance-school',NULLIF($7,''),NOW(),NOW())
		ON CONFLICT (domain) DO UPDATE SET
		  business_name = COALESCE(NULLIF(EXCLUDED.business_name,''), business_listings.business_name),
		  address       = COALESCE(NULLIF(EXCLUDED.address,''), business_listings.address),
		  social_links  = EXCLUDED.social_links,
		  website       = COALESCE(NULLIF(EXCLUDED.website,''), business_listings.website),
		  description   = COALESCE(NULLIF(EXCLUDED.description,''), business_listings.description),
		  niche_category = 'yoga', off_niche = false, category = 'yogaalliance-school',
		  updated_at = NOW()
		RETURNING id`,
		domain, profileURL, s.name, s.address, s.bio, string(socialJSON), s.website,
	).Scan(&bizID)
	if err != nil {
		return fmt.Errorf("business_listings upsert: %w", err)
	}

	if s.email == "" {
		return nil
	}
	at := strings.LastIndex(s.email, "@")
	if at < 1 {
		return nil
	}
	emailDomain := strings.ToLower(s.email[at+1:])
	localPart := s.email[:at]

	var emailID int64
	err = db.QueryRow(`
		INSERT INTO emails (email, domain, local_part, free_email, created_at)
		VALUES ($1,$2,$3,$4,NOW())
		ON CONFLICT (email) DO UPDATE SET domain = EXCLUDED.domain
		RETURNING id`,
		strings.ToLower(s.email), emailDomain, localPart, freeMail[emailDomain],
	).Scan(&emailID)
	if err != nil {
		return fmt.Errorf("emails upsert: %w", err)
	}

	_, err = db.Exec(`
		INSERT INTO business_emails (business_id, email_id, source)
		VALUES ($1,$2,$3) ON CONFLICT (business_id, email_id) DO NOTHING`,
		bizID, emailID, schoolSrcTag)
	if err != nil {
		return fmt.Errorf("business_emails link: %w", err)
	}
	return nil
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
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	var out []string
	for _, ln := range strings.Split(string(b), "\n") {
		if s := strings.TrimSpace(ln); s != "" {
			out = append(out, s)
		}
	}
	return out
}
