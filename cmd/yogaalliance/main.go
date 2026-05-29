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

// searchParams builds the fetchSchoolRecords/Count param map. Empty addr =>
// global result set (capped ~2000 by the backend); a non-empty addr+lat/lng
// filters to schools within searchRadius miles (sorted by distance), which is
// how we sweep past the global cap. page* only used by the records call.
func searchParams(lat, lng float64, addr, country string, pageSize, pageNumber int) map[string]any {
	return map[string]any{
		"schoolId": "", "schoolGoogleAddress": addr, "schoolLatitude": lat, "schoolLongitude": lng,
		"schoolStreet": "", "schoolCity": "", "schoolState": "", "schoolCountry": country,
		"searchRadius": 50, "name": "", "designation": "",
		"onlineServices": false, "closedCaptioning": false,
		"selectedDesignations": []any{}, "selectedTrainingFormats": []any{},
		"offerScholarship": false, "offerExchangePrograms": false,
		"acceptsMyCAA": false, "acceptsGiBill": false,
		"ratings": []any{}, "typesOfYoga": []any{}, "language": "",
		"sortDirection": "ASC", "pageSize": pageSize, "pageNumber": pageNumber, "SearchFlag": true,
	}
}

// geoCenter is a sweep search center. The global pass caps at ~2000; sweeping
// these worldwide centers (radius 50mi, deduped by Id) surfaces the rest.
type geoCenter struct {
	lat, lng      float64
	addr, country string
}

// schoolCenters: yoga-dense metros worldwide (heavy US — most RYS schools are
// US). Coords are approximate; radius 50mi tolerates imprecision. The sweep
// stops early once the deduped unique count reaches the reported total.
var schoolCenters = []geoCenter{
	{40.71, -74.01, "New York, NY, USA", "United States"}, {34.05, -118.24, "Los Angeles, CA, USA", "United States"},
	{41.88, -87.63, "Chicago, IL, USA", "United States"}, {29.76, -95.37, "Houston, TX, USA", "United States"},
	{33.45, -112.07, "Phoenix, AZ, USA", "United States"}, {39.95, -75.17, "Philadelphia, PA, USA", "United States"},
	{29.42, -98.49, "San Antonio, TX, USA", "United States"}, {32.72, -117.16, "San Diego, CA, USA", "United States"},
	{32.78, -96.80, "Dallas, TX, USA", "United States"}, {30.27, -97.74, "Austin, TX, USA", "United States"},
	{30.33, -81.66, "Jacksonville, FL, USA", "United States"}, {39.96, -82.99, "Columbus, OH, USA", "United States"},
	{35.23, -80.84, "Charlotte, NC, USA", "United States"}, {37.77, -122.42, "San Francisco, CA, USA", "United States"},
	{39.77, -86.16, "Indianapolis, IN, USA", "United States"}, {47.61, -122.33, "Seattle, WA, USA", "United States"},
	{39.74, -104.99, "Denver, CO, USA", "United States"}, {38.91, -77.04, "Washington, DC, USA", "United States"},
	{42.36, -71.06, "Boston, MA, USA", "United States"}, {36.16, -86.78, "Nashville, TN, USA", "United States"},
	{45.52, -122.68, "Portland, OR, USA", "United States"}, {36.17, -115.14, "Las Vegas, NV, USA", "United States"},
	{42.33, -83.05, "Detroit, MI, USA", "United States"}, {35.15, -90.05, "Memphis, TN, USA", "United States"},
	{38.25, -85.76, "Louisville, KY, USA", "United States"}, {43.04, -87.91, "Milwaukee, WI, USA", "United States"},
	{35.08, -106.65, "Albuquerque, NM, USA", "United States"}, {32.22, -110.97, "Tucson, AZ, USA", "United States"},
	{38.58, -121.49, "Sacramento, CA, USA", "United States"}, {39.10, -94.58, "Kansas City, MO, USA", "United States"},
	{33.75, -84.39, "Atlanta, GA, USA", "United States"}, {25.76, -80.19, "Miami, FL, USA", "United States"},
	{35.78, -78.64, "Raleigh, NC, USA", "United States"}, {41.26, -95.93, "Omaha, NE, USA", "United States"},
	{44.98, -93.27, "Minneapolis, MN, USA", "United States"}, {27.95, -82.46, "Tampa, FL, USA", "United States"},
	{29.95, -90.07, "New Orleans, LA, USA", "United States"}, {41.50, -81.69, "Cleveland, OH, USA", "United States"},
	{21.31, -157.86, "Honolulu, HI, USA", "United States"}, {40.76, -111.89, "Salt Lake City, UT, USA", "United States"},
	{43.62, -116.21, "Boise, ID, USA", "United States"}, {37.54, -77.44, "Richmond, VA, USA", "United States"},
	{35.60, -82.55, "Asheville, NC, USA", "United States"}, {40.01, -105.27, "Boulder, CO, USA", "United States"},
	{35.69, -105.94, "Santa Fe, NM, USA", "United States"}, {43.66, -70.26, "Portland, ME, USA", "United States"},
	{44.48, -73.21, "Burlington, VT, USA", "United States"}, {28.54, -81.38, "Orlando, FL, USA", "United States"},
	{26.12, -80.14, "Fort Lauderdale, FL, USA", "United States"}, {32.08, -81.09, "Savannah, GA, USA", "United States"},
	{36.85, -76.29, "Norfolk, VA, USA", "United States"}, {39.29, -76.61, "Baltimore, MD, USA", "United States"},
	{40.44, -79.99, "Pittsburgh, PA, USA", "United States"}, {42.89, -78.88, "Buffalo, NY, USA", "United States"},
	{43.16, -77.61, "Rochester, NY, USA", "United States"}, {42.65, -73.76, "Albany, NY, USA", "United States"},
	{41.76, -72.69, "Hartford, CT, USA", "United States"}, {40.74, -74.17, "Newark, NJ, USA", "United States"},
	{34.00, -81.03, "Columbia, SC, USA", "United States"}, {30.44, -84.28, "Tallahassee, FL, USA", "United States"},
	{32.30, -90.18, "Jackson, MS, USA", "United States"}, {34.75, -92.29, "Little Rock, AR, USA", "United States"},
	{35.47, -97.52, "Oklahoma City, OK, USA", "United States"}, {41.59, -93.62, "Des Moines, IA, USA", "United States"},
	{43.07, -89.40, "Madison, WI, USA", "United States"}, {46.59, -112.04, "Helena, MT, USA", "United States"},
	{33.45, -94.04, "Texarkana, USA", "United States"}, {31.76, -106.49, "El Paso, TX, USA", "United States"},
	{36.75, -119.77, "Fresno, CA, USA", "United States"}, {34.42, -119.70, "Santa Barbara, CA, USA", "United States"},
	{37.34, -121.89, "San Jose, CA, USA", "United States"}, {38.44, -122.71, "Santa Rosa, CA, USA", "United States"},
	{51.51, -0.13, "London, UK", "United Kingdom"}, {53.48, -2.24, "Manchester, UK", "United Kingdom"},
	{55.95, -3.19, "Edinburgh, UK", "United Kingdom"}, {53.41, -2.98, "Liverpool, UK", "United Kingdom"},
	{52.49, -1.89, "Birmingham, UK", "United Kingdom"}, {53.35, -6.26, "Dublin, Ireland", "Ireland"},
	{48.86, 2.35, "Paris, France", "France"}, {52.52, 13.40, "Berlin, Germany", "Germany"},
	{48.14, 11.58, "Munich, Germany", "Germany"}, {50.94, 6.96, "Cologne, Germany", "Germany"},
	{52.37, 4.90, "Amsterdam, Netherlands", "Netherlands"}, {41.39, 2.17, "Barcelona, Spain", "Spain"},
	{40.42, -3.70, "Madrid, Spain", "Spain"}, {41.90, 12.50, "Rome, Italy", "Italy"},
	{45.46, 9.19, "Milan, Italy", "Italy"}, {47.37, 8.54, "Zurich, Switzerland", "Switzerland"},
	{48.21, 16.37, "Vienna, Austria", "Austria"}, {55.68, 12.57, "Copenhagen, Denmark", "Denmark"},
	{59.33, 18.07, "Stockholm, Sweden", "Sweden"}, {59.91, 10.75, "Oslo, Norway", "Norway"},
	{38.72, -9.14, "Lisbon, Portugal", "Portugal"}, {37.98, 23.73, "Athens, Greece", "Greece"},
	{50.08, 14.44, "Prague, Czech Republic", "Czechia"}, {52.23, 21.01, "Warsaw, Poland", "Poland"},
	{43.65, -79.38, "Toronto, ON, Canada", "Canada"}, {49.28, -123.12, "Vancouver, BC, Canada", "Canada"},
	{45.50, -73.57, "Montreal, QC, Canada", "Canada"}, {51.05, -114.07, "Calgary, AB, Canada", "Canada"},
	{-33.87, 151.21, "Sydney, Australia", "Australia"}, {-37.81, 144.96, "Melbourne, Australia", "Australia"},
	{-27.47, 153.03, "Brisbane, Australia", "Australia"}, {-31.95, 115.86, "Perth, Australia", "Australia"},
	{-36.85, 174.76, "Auckland, New Zealand", "New Zealand"}, {19.08, 72.88, "Mumbai, India", "India"},
	{28.61, 77.21, "Delhi, India", "India"}, {12.97, 77.59, "Bangalore, India", "India"},
	{18.52, 73.86, "Pune, India", "India"}, {30.09, 78.27, "Rishikesh, India", "India"},
	{15.30, 74.12, "Goa, India", "India"}, {13.08, 80.27, "Chennai, India", "India"},
	{-8.65, 115.22, "Bali, Indonesia", "Indonesia"}, {13.76, 100.50, "Bangkok, Thailand", "Thailand"},
	{1.35, 103.82, "Singapore", "Singapore"}, {3.14, 101.69, "Kuala Lumpur, Malaysia", "Malaysia"},
	{22.32, 114.17, "Hong Kong", "Hong Kong"}, {35.68, 139.69, "Tokyo, Japan", "Japan"},
	{37.57, 126.98, "Seoul, South Korea", "South Korea"}, {25.20, 55.27, "Dubai, UAE", "United Arab Emirates"},
	{32.08, 34.78, "Tel Aviv, Israel", "Israel"}, {-33.92, 18.42, "Cape Town, South Africa", "South Africa"},
	{-26.20, 28.05, "Johannesburg, South Africa", "South Africa"}, {19.43, -99.13, "Mexico City, Mexico", "Mexico"},
	{20.21, -87.47, "Tulum, Mexico", "Mexico"}, {-23.55, -46.63, "Sao Paulo, Brazil", "Brazil"},
	{-22.91, -43.17, "Rio de Janeiro, Brazil", "Brazil"}, {-34.60, -58.38, "Buenos Aires, Argentina", "Argentina"},
	{4.71, -74.07, "Bogota, Colombia", "Colombia"}, {-12.05, -77.04, "Lima, Peru", "Peru"},
	{-33.45, -70.67, "Santiago, Chile", "Chile"}, {9.93, -84.08, "San Jose, Costa Rica", "Costa Rica"},
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
	body, err := apexPOST(c, classSchoolSearch, methodSchoolCount, searchParams(0, 0, "", "", schoolPageSize, 1))
	if err != nil {
		return 0
	}
	var r struct {
		ReturnValue int `json:"returnValue"`
	}
	json.Unmarshal(body, &r)
	return r.ReturnValue
}

// fetchSchoolPage returns one page of school records for the given search params.
func fetchSchoolPage(c *http.Client, params map[string]any) ([]schoolRec, error) {
	body, err := apexPOST(c, classSchoolSearch, methodSchoolRecords, params)
	if err != nil {
		return nil, err
	}
	var r schoolListResp
	if json.Unmarshal(body, &r) != nil {
		return nil, fmt.Errorf("parse school records")
	}
	return r.ReturnValue, nil
}

// enumerateSchools returns unique RYS school records (deduped by Id). The global
// pass (empty location) caps at ~2000, so we then sweep schoolCenters worldwide
// (radius 50mi each) until the deduped count reaches the reported total. limit
// caps the result (0 = all).
func enumerateSchools(c *http.Client, limit int) []schoolRec {
	seen := map[string]bool{}
	var out []schoolRec
	total := fetchSchoolCount(c)

	add := func(recs []schoolRec) {
		for _, rec := range recs {
			if rec.Id == "" || seen[rec.Id] {
				continue
			}
			seen[rec.Id] = true
			out = append(out, rec)
		}
	}
	// paginate one location (empty addr = global) up to 120 pages.
	sweep := func(lat, lng float64, addr, country string) {
		for page := 1; page <= 120; page++ {
			recs, err := fetchSchoolPage(c, searchParams(lat, lng, addr, country, schoolPageSize, page))
			if err != nil || len(recs) == 0 {
				break
			}
			add(recs)
			if limit > 0 && len(out) >= limit {
				return
			}
		}
	}

	sweep(0, 0, "", "") // global pass
	log.Printf("  global pass: %d unique (of %d total)", len(out), total)

	for i, ctr := range schoolCenters {
		if (limit > 0 && len(out) >= limit) || (total > 0 && len(out) >= total) {
			break
		}
		sweep(ctr.lat, ctr.lng, ctr.addr, ctr.country)
		if (i+1)%15 == 0 || len(out) >= total {
			log.Printf("  swept %d/%d centers: %d/%d unique schools", i+1, len(schoolCenters), len(out), total)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
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
	recs := enumerateSchools(c, limit)
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
