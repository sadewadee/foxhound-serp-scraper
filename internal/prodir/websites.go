package prodir

// `prodir websites`: link-back of own-website results onto directory rows.
// `prodir sites` enqueues the own website of every directory row into the enrich
// pipeline; the results land on organic business_listings rows (domain = the
// website host, or www.host), never on the directory row. This step copies them
// back, following the directory-email rules:
//
//   - a row that has an email from its directory page (any business_emails row
//     whose source is not 'website_match') is never given a website email;
//   - otherwise it gets at most ONE email, the person's own, chosen exactly like
//     the CRM loader does (webemail.go: chooseWebEmail + prep.py's junk filters),
//     stored with source 'website_match' so the loader can tell it from a page
//     email; no pick = nothing linked; a row that already has one is not re-picked;
//   - the organic row's phone fills the directory row's phone only when empty;
//   - hosts listed by more than 3 directory rows (employer / platform) are skipped;
//   - nothing existing is overwritten and the organic row is never touched.
//
// It also counts, per source, how the enrichment_jobs of the websites ended
// (completed / pending / dead / none). Row selection and host skip rules are
// shared with sites.go. Dry-run executes the same statements and rolls them
// back, so its counts are exact.

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"github.com/lib/pq"
)

const (
	websitesLinkChunk = 2000
	websiteMatchLink  = "website_match"
)

// WebsiteStats are the per-source counters printed by `prodir websites`.
type WebsiteStats struct {
	Selected, Skipped, Hosts, SharedSkipped        int
	PageEmailRows, WebEmailRows, NoPick            int
	LinkedRows, EmailsLinked, PhonesFilled         int
	JobsCompleted, JobsPending, JobsDead, JobsNone int // hosts by enrichment_jobs outcome
}

type dirRow struct {
	id   int64
	name string
	url  string
	key  string // host without www.
}

const websiteRowsSQL = `
SELECT id, COALESCE(business_name, ''), website FROM business_listings
WHERE category = ANY($1) AND domain LIKE $2 AND website IS NOT NULL AND website <> ''
ORDER BY id`

// selectWebsiteRows reads the directory rows of src whose website is a usable
// own site; the rest is counted in st.Skipped. Several rows may share a host.
func selectWebsiteRows(ctx context.Context, db *sql.DB, src siteSource, st *WebsiteStats) ([]dirRow, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '"+sitesReadTimeout+"'"); err != nil {
		return nil, err
	}
	rs, err := tx.QueryContext(ctx, websiteRowsSQL, pq.Array(src.categories), src.domainLike)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []dirRow
	for rs.Next() {
		var id int64
		var name, website string
		if err := rs.Scan(&id, &name, &website); err != nil {
			return nil, err
		}
		st.Selected++
		u, host, ok := normalizeSiteURL(website)
		if !ok || siteSkipReason(u, host, src.host) != "" {
			st.Skipped++
			continue
		}
		out = append(out, dirRow{id, name, u, hostKey(host)})
	}
	return out, rs.Err()
}

// linkOrgCTE pairs every directory row id with the organic rows of its host.
const linkOrgCTE = `
WITH pairs AS (SELECT * FROM unnest($1::bigint[], $2::text[]) AS p(dir_id, key)),
org AS (
  SELECT p.dir_id, o.id AS org_id FROM pairs p JOIN business_listings o ON o.domain = p.key
  UNION
  SELECT p.dir_id, o.id FROM pairs p JOIN business_listings o ON o.domain = 'www.' || p.key
)`

const (
	linkCountSQL = linkOrgCTE + ` SELECT COUNT(DISTINCT dir_id) FROM org WHERE org_id <> dir_id`

	linkPhonesSQL = linkOrgCTE + `,
pick AS (
  SELECT DISTINCT ON (org.dir_id) org.dir_id, o.phone
  FROM org JOIN business_listings o ON o.id = org.org_id
  WHERE org.org_id <> org.dir_id AND o.phone IS NOT NULL AND o.phone <> ''
  ORDER BY org.dir_id, org.org_id
)
UPDATE business_listings d
SET phone = pick.phone,
    phones = CASE WHEN COALESCE(cardinality(d.phones), 0) = 0 THEN ARRAY[pick.phone] ELSE d.phones END,
    updated_at = NOW()
FROM pick
WHERE d.id = pick.dir_id AND (d.phone IS NULL OR d.phone = '')`
)

// finish commits the transaction, or rolls it back on a dry run.
func finish(tx *sql.Tx, dryRun bool) error {
	if dryRun {
		return tx.Rollback()
	}
	return tx.Commit()
}

// beginWrite opens a short transaction with a statement timeout.
func beginWrite(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '"+sitesWriteTime+"'"); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// linkPhones fills the empty phones of the directory rows from their organic rows.
func linkPhones(ctx context.Context, db *sql.DB, rows []dirRow, dryRun bool, st *WebsiteStats) error {
	for i := 0; i < len(rows); i += websitesLinkChunk {
		chunk := rows[i:min(i+websitesLinkChunk, len(rows))]
		ids := make([]int64, len(chunk))
		keys := make([]string, len(chunk))
		for j, r := range chunk {
			ids[j], keys[j] = r.id, r.key
		}
		tx, err := beginWrite(ctx, db)
		if err != nil {
			return err
		}
		var linked int
		if err := tx.QueryRowContext(ctx, linkCountSQL, pq.Array(ids), pq.Array(keys)).Scan(&linked); err != nil {
			tx.Rollback()
			return err
		}
		res, err := tx.ExecContext(ctx, linkPhonesSQL, pq.Array(ids), pq.Array(keys))
		if err != nil {
			tx.Rollback()
			return err
		}
		phones, _ := res.RowsAffected()
		if err := finish(tx, dryRun); err != nil {
			return err
		}
		st.LinkedRows += linked
		st.PhonesFilled += int(phones)
	}
	return nil
}

// readTx runs fn in a read-only transaction with a statement timeout.
func readTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '"+sitesReadTimeout+"'"); err != nil {
		return err
	}
	return fn(tx)
}

// hostEmails reads the emails of the organic rows of the hosts (domain = key or
// www.key), lower-cased, per host key.
func hostEmails(ctx context.Context, db *sql.DB, keys []string) (map[string][]string, error) {
	out := map[string][]string{}
	err := readTx(ctx, db, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i += sitesHostChunk {
			var doms []string
			for _, k := range keys[i:min(i+sitesHostChunk, len(keys))] {
				doms = append(doms, k, "www."+k)
			}
			rs, err := tx.QueryContext(ctx, `
SELECT o.domain, e.email FROM business_listings o
JOIN business_emails be ON be.business_id = o.id JOIN emails e ON e.id = be.email_id
WHERE o.domain = ANY($1)`, pq.Array(doms))
			if err != nil {
				return err
			}
			for rs.Next() {
				var d, e string
				if err := rs.Scan(&d, &e); err != nil {
					rs.Close()
					return err
				}
				k := hostKey(d)
				out[k] = addString(out[k], strings.ToLower(strings.TrimSpace(e)))
			}
			if err := rs.Err(); err != nil {
				rs.Close()
				return err
			}
			rs.Close()
		}
		return nil
	})
	return out, err
}

// emailState reads, per directory row, whether it holds a page email (a
// business_emails row not from this command) and whether it holds a website one.
func emailState(ctx context.Context, db *sql.DB, rows []dirRow) (page, web map[int64]bool, err error) {
	page, web = map[int64]bool{}, map[int64]bool{}
	err = readTx(ctx, db, func(tx *sql.Tx) error {
		for i := 0; i < len(rows); i += sitesHostChunk {
			ids := make([]int64, 0, sitesHostChunk)
			for _, r := range rows[i:min(i+sitesHostChunk, len(rows))] {
				ids = append(ids, r.id)
			}
			rs, err := tx.QueryContext(ctx, `
SELECT business_id, source IS DISTINCT FROM '`+websiteMatchLink+`' FROM business_emails WHERE business_id = ANY($1)`, pq.Array(ids))
			if err != nil {
				return err
			}
			for rs.Next() {
				var id int64
				var isPage bool
				if err := rs.Scan(&id, &isPage); err != nil {
					rs.Close()
					return err
				}
				if isPage {
					page[id] = true
				} else {
					web[id] = true
				}
			}
			if err := rs.Err(); err != nil {
				rs.Close()
				return err
			}
			rs.Close()
		}
		return nil
	})
	return page, web, err
}

// linkEmailsSQL links one picked email per directory row. The NOT EXISTS keeps
// a row that gained any email since it was read untouched.
const linkEmailsSQL = `
INSERT INTO business_emails (business_id, email_id, source)
SELECT t.id, e.id, '` + websiteMatchLink + `'
FROM unnest($1::bigint[], $2::text[]) AS t(id, email)
JOIN emails e ON e.email = t.email
WHERE NOT EXISTS (SELECT 1 FROM business_emails x WHERE x.business_id = t.id)
ON CONFLICT (business_id, email_id) DO NOTHING`

func linkEmails(ctx context.Context, db *sql.DB, ids []int64, emails []string, dryRun bool) (int, error) {
	total := 0
	for i := 0; i < len(ids); i += websitesLinkChunk {
		end := min(i+websitesLinkChunk, len(ids))
		tx, err := beginWrite(ctx, db)
		if err != nil {
			return total, err
		}
		res, err := tx.ExecContext(ctx, linkEmailsSQL, pq.Array(ids[i:end]), pq.Array(emails[i:end]))
		if err != nil {
			tx.Rollback()
			return total, err
		}
		n, _ := res.RowsAffected()
		if err := finish(tx, dryRun); err != nil {
			return total, err
		}
		total += int(n)
	}
	return total, nil
}

// pickEmails chooses the website email of every row that has no email yet.
func pickEmails(rows []dirRow, page, web map[int64]bool, emails map[string][]string, st *WebsiteStats) (ids []int64, picked []string) {
	for _, r := range rows {
		switch {
		case page[r.id]:
			st.PageEmailRows++
			continue
		case web[r.id]:
			st.WebEmailRows++
			continue
		}
		var cands []webCand
		for _, e := range emails[r.key] {
			cands = append(cands, webCand{e, r.key})
		}
		if len(cands) == 0 {
			continue
		}
		// the CRM loader names the person after the part of business_name before the first comma
		first, _, _ := strings.Cut(r.name, ",")
		if c, ok := chooseWebEmail(cands, first, ""); ok {
			ids, picked = append(ids, r.id), append(picked, c.email)
		} else {
			st.NoPick++
		}
	}
	return ids, picked
}

// jobCoverage counts the hosts by the best outcome of their enrichment_jobs
// (domain = host or www.host): completed, else pending / processing / failed,
// else dead, else no job.
func jobCoverage(ctx context.Context, db *sql.DB, rows []dirRow, st *WebsiteStats) error {
	seen := map[string]bool{}
	var doms []string
	for _, r := range rows {
		if !seen[r.key] {
			seen[r.key] = true
			doms = append(doms, r.key, "www."+r.key)
		}
	}
	st.Hosts = len(seen)
	rank := map[string]int{} // host key -> 3 completed, 2 pending, 1 dead
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '"+sitesReadTimeout+"'"); err != nil {
		return err
	}
	for i := 0; i < len(doms); i += sitesHostChunk {
		rs, err := tx.QueryContext(ctx, `SELECT DISTINCT domain, status FROM enrichment_jobs WHERE domain = ANY($1)`,
			pq.Array(doms[i:min(i+sitesHostChunk, len(doms))]))
		if err != nil {
			return err
		}
		for rs.Next() {
			var d, status string
			if err := rs.Scan(&d, &status); err != nil {
				rs.Close()
				return err
			}
			r := 2
			switch status {
			case "completed":
				r = 3
			case "dead":
				r = 1
			}
			if k := hostKey(d); r > rank[k] {
				rank[k] = r
			}
		}
		if err := rs.Err(); err != nil {
			rs.Close()
			return err
		}
		rs.Close()
	}
	for k := range seen {
		switch rank[k] {
		case 3:
			st.JobsCompleted++
		case 2:
			st.JobsPending++
		case 1:
			st.JobsDead++
		default:
			st.JobsNone++
		}
	}
	return nil
}

// Websites links the enriched results of the selected source's websites back
// onto its directory rows (ace, icf, yogaalliance, a generic slug or "all").
// dryRun writes nothing. Per-source counts go to w. The "listed by more than 3
// rows" rule counts the rows of the selected sources, so use `all` for the
// exact figure.
func Websites(ctx context.Context, db *sql.DB, sel string, dryRun bool, w io.Writer) error {
	srcs, err := siteSources(sel)
	if err != nil {
		return err
	}
	type loaded struct {
		src  siteSource
		rows []dirRow
		st   *WebsiteStats
	}
	var all []loaded
	share := map[string]int{}
	for _, src := range srcs {
		st := &WebsiteStats{}
		rows, err := selectWebsiteRows(ctx, db, src, st)
		if err != nil {
			return fmt.Errorf("%s: read: %w", src.name, err)
		}
		for _, r := range rows {
			share[r.key]++
		}
		all = append(all, loaded{src, rows, st})
	}
	keys := make([]string, 0, len(share))
	for k := range share {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	raw, err := hostEmails(ctx, db, keys)
	if err != nil {
		return fmt.Errorf("host emails: %w", err)
	}
	emails := siteEmailSet(raw, share)

	for _, l := range all {
		st := l.st
		var own []dirRow
		for _, r := range l.rows {
			if share[r.key] > maxHostShare {
				st.SharedSkipped++
				continue
			}
			own = append(own, r)
		}
		if err := linkPhones(ctx, db, own, dryRun, st); err != nil {
			return fmt.Errorf("%s: link phones: %w", l.src.name, err)
		}
		page, web, err := emailState(ctx, db, own)
		if err != nil {
			return fmt.Errorf("%s: email state: %w", l.src.name, err)
		}
		ids, picked := pickEmails(own, page, web, emails, st)
		if st.EmailsLinked, err = linkEmails(ctx, db, ids, picked, dryRun); err != nil {
			return fmt.Errorf("%s: link emails: %w", l.src.name, err)
		}
		if err := jobCoverage(ctx, db, l.rows, st); err != nil {
			return fmt.Errorf("%s: job coverage: %w", l.src.name, err)
		}
		slog.Info("prodir websites", "source", l.src.name, "dry_run", dryRun, "selected", st.Selected,
			"emails_linked", st.EmailsLinked, "phones_filled", st.PhonesFilled)
		fmt.Fprintf(w, "%s: selected=%d platform_skipped=%d hosts=%d shared_host_rows=%d page_email_rows=%d web_email_rows=%d no_pick=%d organic_matched_rows=%d emails_linked=%d phones_filled=%d jobs_completed=%d jobs_pending=%d jobs_dead=%d jobs_none=%d dry_run=%v\n",
			l.src.name, st.Selected, st.Skipped, st.Hosts, st.SharedSkipped, st.PageEmailRows, st.WebEmailRows, st.NoPick,
			st.LinkedRows, st.EmailsLinked, st.PhonesFilled, st.JobsCompleted, st.JobsPending, st.JobsDead, st.JobsNone, dryRun)
	}
	return nil
}
