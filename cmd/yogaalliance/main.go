// Command yogaalliance crawls the public Yoga Alliance directory (schools +
// teachers) via its guest Salesforce LWR Apex API and writes structured,
// 100%-in-niche records to CSV.
//
// Why this exists: the SERP email-dork pipeline yields data-poor rows (only
// ~10% have an address, niche match ~0%). Yoga Alliance is a curated directory
// of Registered Yoga Schools (RYS) and Teachers (RYT) — every record is real,
// in-niche (yoga/wellness/fitness), and TEACHER records expose a published
// email + city + country directly. No spa/massage contamination.
//
// API (reverse-engineered, guest-accessible, no auth/CSRF — just cookies):
//
//	POST https://app.yogaalliance.org/webruntime/api/apex/execute?language=en-US&asGuest=true&htmlEncode=false
//	  body: {"namespace":"","classname":"<HASH>","method":"<M>","isContinuation":false,"params":{...},"cacheable":false}
//	  school : classname @udd/01pTR000001kCED  method getSchoolDetails  params {"schoolId": "<id>"}
//	  teacher: classname @udd/01pTR000001kCE1  method getTeacherDetails params {"teacherId":"<id>"}
//
// The classname hashes are Salesforce build artifacts and CAN change on a
// redeploy; if every call starts returning 400, re-capture them from a profile
// page's network tab and update the constants below.
//
// IDs come from the sitemap index at https://app.yogaalliance.org/sitemap.xml
// (account-*.xml = schools, contact-*.xml = teachers).
//
// Build & run (standalone — NOT covered by the playwright tag):
//
//	go build -o yoga ./cmd/yogaalliance
//	./yoga -mode teacher -out teachers.csv -concurrency 12
//	./yoga -mode school  -out schools.csv  -concurrency 12 -limit 500   # test slice
package main

import (
	"bytes"
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
)

var (
	locRe    = regexp.MustCompile(`<loc>([^<]+)</loc>`)
	idRe     = regexp.MustCompile(`/(?:school|teacher)publicprofile/([A-Za-z0-9]{15,18})/`)
	tagsRe   = regexp.MustCompile(`<[^>]+>`)
	spacesRe = regexp.MustCompile(`\s+`)
)

type apexReq struct {
	Namespace      string         `json:"namespace"`
	Classname      string         `json:"classname"`
	Method         string         `json:"method"`
	IsContinuation bool           `json:"isContinuation"`
	Params         map[string]any `json:"params"`
	Cacheable      bool           `json:"cacheable"`
}

// schoolResp / teacherResp capture only the fields we persist.
type schoolResp struct {
	ReturnValue struct {
		Id             string `json:"Id"`
		DirectoryName  string `json:"directoryName"`
		Address        string `json:"address"`
		Designation    string `json:"schoolDesignation"`
		Facebook       string `json:"schoolFacebook"`
		Instagram      string `json:"schoolInstagram"`
		Languages      string `json:"languages"`
		TypesOfYoga    string `json:"typesOfYogaTaught"`
		Published      bool   `json:"isSchoolProfilePublished"`
		TrainerDetails []struct {
			TrainerId            string `json:"trainerId"`
			TrainerDirectoryName string `json:"trainerDirectoryName"`
		} `json:"trainerDetailsList"`
	} `json:"returnValue"`
}

type teacherResp struct {
	ReturnValue struct {
		Id             string  `json:"Id"`
		DirectoryName  string  `json:"directoryName"`
		FirstName      string  `json:"firstName"`
		LastName       string  `json:"lastName"`
		Email          string  `json:"teacherEmail"`
		EmailPublished bool    `json:"isEmailPublished"`
		Address        string  `json:"address"`
		MailingCity    string  `json:"mailingCity"`
		MailingState   string  `json:"mailingState"`
		MailingCountry string  `json:"mailingCountry"`
		Instagram      string  `json:"teacherInstagram"`
		Designation    string  `json:"teacherDesignation"`
		Languages      string  `json:"languages"`
		TypesOfYoga    string  `json:"typesOfYogaTaught"`
		TeachingHours  float64 `json:"teachingHours"`
		Published      bool    `json:"isProfilePublished"`
	} `json:"returnValue"`
}

func main() {
	mode := flag.String("mode", "teacher", "teacher | school")
	out := flag.String("out", "", "output CSV path (default: <mode>s.csv)")
	conc := flag.Int("concurrency", 10, "concurrent requests")
	limit := flag.Int("limit", 0, "max records (0 = all)")
	idsFile := flag.String("ids", "", "optional file of IDs (one per line); default = crawl sitemap")
	flag.Parse()

	if *mode != "teacher" && *mode != "school" {
		log.Fatalf("mode must be teacher or school")
	}
	outPath := *out
	if outPath == "" {
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
	if *limit > 0 && len(ids) > *limit {
		ids = ids[:*limit]
	}
	log.Printf("yogaalliance: %s — %d IDs to fetch (concurrency=%d) → %s", *mode, len(ids), *conc, outPath)

	f, err := os.Create(outPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	var mu sync.Mutex // guards csv writer

	if *mode == "teacher" {
		w.Write([]string{"id", "name", "email", "email_published", "city", "state", "country", "address", "instagram", "designation", "languages", "yoga_types", "teaching_hours"})
	} else {
		w.Write([]string{"id", "name", "address", "facebook", "instagram", "designation", "languages", "yoga_types", "trainer_ids"})
	}

	var done, ok, withEmail int64
	jobs := make(chan string, *conc*2)
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				row, hasEmail := fetchOne(client, *mode, id)
				n := atomic.AddInt64(&done, 1)
				if row != nil {
					atomic.AddInt64(&ok, 1)
					if hasEmail {
						atomic.AddInt64(&withEmail, 1)
					}
					mu.Lock()
					w.Write(row)
					if n%500 == 0 {
						w.Flush()
					}
					mu.Unlock()
				}
				if n%1000 == 0 {
					log.Printf("  progress: %d/%d done, %d ok, %d with-email", n, len(ids), atomic.LoadInt64(&ok), atomic.LoadInt64(&withEmail))
				}
			}
		}()
	}
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	wg.Wait()
	w.Flush()
	log.Printf("DONE: %d fetched, %d records written, %d with email → %s", done, ok, withEmail, outPath)
}

func newClient() *http.Client {
	jar, _ := cookiejar.New(nil)
	return &http.Client{Jar: jar, Timeout: 30 * time.Second}
}

// bootstrap seeds guest cookies (the apex API rejects requests with no session
// cookies). A single GET to any profile page is enough.
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

func crawlSitemapIDs(c *http.Client, mode string) []string {
	want := "contact" // teachers
	if mode == "school" {
		want = "account"
	}
	idxBody := httpGet(c, sitemapIndex)
	var subs []string
	for _, m := range locRe.FindAllStringSubmatch(string(idxBody), -1) {
		if strings.Contains(m[1], "sitemap-"+want) && !strings.Contains(m[1], "weekly") {
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
		log.Printf("  sitemap %s → %d unique IDs so far", sm[strings.LastIndex(sm, "/")+1:], len(ids))
	}
	return ids
}

func fetchOne(c *http.Client, mode, id string) ([]string, bool) {
	var class, method, pkey string
	if mode == "school" {
		class, method, pkey = classSchool, methodSchool, "schoolId"
	} else {
		class, method, pkey = classTeacher, methodTeacher, "teacherId"
	}
	reqBody, _ := json.Marshal(apexReq{
		Classname: class, Method: method, Params: map[string]any{pkey: id},
	})

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
			lastErr = fmt.Errorf("http %d", resp.StatusCode)
			time.Sleep(time.Duration(attempt+1) * 400 * time.Millisecond)
			continue
		}
		if mode == "school" {
			return parseSchool(body)
		}
		return parseTeacher(body)
	}
	_ = lastErr
	return nil, false
}

func parseSchool(body []byte) ([]string, bool) {
	var r schoolResp
	if json.Unmarshal(body, &r) != nil || r.ReturnValue.Id == "" {
		return nil, false
	}
	v := r.ReturnValue
	var tids []string
	for _, t := range v.TrainerDetails {
		if t.TrainerId != "" {
			tids = append(tids, t.TrainerId)
		}
	}
	return []string{
		v.Id, clean(v.DirectoryName), clean(v.Address), v.Facebook, v.Instagram,
		clean(v.Designation), clean(v.Languages), clean(v.TypesOfYoga), strings.Join(tids, ";"),
	}, false
}

func parseTeacher(body []byte) ([]string, bool) {
	var r teacherResp
	if json.Unmarshal(body, &r) != nil || r.ReturnValue.Id == "" {
		return nil, false
	}
	v := r.ReturnValue
	name := clean(v.DirectoryName)
	if name == "" {
		name = clean(strings.TrimSpace(v.FirstName + " " + v.LastName))
	}
	return []string{
		v.Id, name, strings.TrimSpace(v.Email), strconv.FormatBool(v.EmailPublished),
		clean(v.MailingCity), clean(v.MailingState), clean(v.MailingCountry), clean(v.Address),
		v.Instagram, clean(v.Designation), clean(v.Languages), clean(v.TypesOfYoga),
		strconv.FormatFloat(v.TeachingHours, 'f', -1, 64),
	}, strings.TrimSpace(v.Email) != ""
}

func clean(s string) string {
	s = tagsRe.ReplaceAllString(s, " ")
	s = strings.ReplaceAll(s, " ", " ")
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
