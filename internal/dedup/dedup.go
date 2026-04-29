package dedup

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sadewadee/serp-scraper/internal/config"
)

// Store wraps a Redis client for dedup operations using SETs.
type Store struct {
	client *redis.Client
}

// New creates a dedup Store connected to Redis.
func New(cfg *config.RedisConfig) (*Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("dedup: redis ping: %w", err)
	}
	return &Store{client: client}, nil
}

// NewFromClient creates a dedup Store from an existing redis.Client.
func NewFromClient(client *redis.Client) *Store {
	return &Store{client: client}
}

// Client returns the underlying redis.Client for shared use.
func (s *Store) Client() *redis.Client {
	return s.client
}

// Close releases the Redis client.
func (s *Store) Close() error {
	return s.client.Close()
}

// --- Helper functions ---

// SHA256 computes hex-encoded SHA256 of s.
func SHA256(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}

// HashQuery returns SHA256(lower(trim(query))).
func HashQuery(query string) string {
	return SHA256(strings.ToLower(strings.TrimSpace(query)))
}

// HashURL returns SHA256 of the canonical URL.
func HashURL(rawURL string) string {
	return SHA256(CanonicalURL(rawURL))
}

// HashEmail returns SHA256(lower(email)).
func HashEmail(email string) string {
	return SHA256(strings.ToLower(strings.TrimSpace(email)))
}

// trackingParams: ad/analytics/affiliate query params dropped from CanonicalURL
// before hashing. Two URLs that differ ONLY in these params produce the same
// url_hash, so the SERP-result-INSERT trigger dedupes them via ON CONFLICT.
//
// List synthesized from production frequency (msockid alone hit 109K times in
// 7 days) and the ClearURLs ruleset. Functional params (q, page, lang, hl, v,
// id, slug, find_loc, find_desc, cflt, t, s, etc.) are intentionally NOT in
// this list — stripping them would lose page identity.
var trackingParams = map[string]struct{}{
	// Bing (PRIMARY — production-confirmed)
	"msockid": {}, "cvid": {}, "ocid": {}, "form": {},
	// Google
	"gclid": {}, "dclid": {}, "ved": {}, "ei": {}, "sxsrf": {}, "rlz": {},
	"aqs": {}, "cd": {}, "cad": {}, "uact": {}, "oq": {},
	"pcampaignid": {}, "sca_esv": {}, "sca_upv": {}, "srsltid": {},
	"gad_source": {}, "gad_campaignid": {}, "gws_rd": {},
	// Microsoft Ads
	"msclkid": {},
	// Facebook/Meta
	"fbclid": {}, "mibextid": {}, "__tn__": {}, "__xts__": {}, "__cft__": {},
	"dti": {}, "eid": {}, "comment_tracking": {}, "video_source": {},
	"rdc": {}, "rdr": {},
	// Twitter/X (KEEP `s`, `t` — too ambiguous, often functional)
	"twclid": {}, "ref_src": {}, "ref_url": {}, "cn": {},
	"__twitter_impression": {},
	// LinkedIn
	"trk": {}, "trackingId": {}, "refId": {}, "lipi": {}, "trackingTags": {},
	// Reddit
	"$deep_link": {}, "%24deep_link": {}, "correlation_id": {}, "share_id": {},
	"rdt": {}, "_branch_match_id": {}, "$3p": {}, "%243p": {},
	"$original_url": {}, "%24original_url": {},
	"ref_campaign": {}, "ref_source": {},
	// Amazon
	"tag": {}, "ascsubtag": {}, "smid": {}, "spIA": {}, "colid": {}, "coliid": {},
	"crid": {}, "sprefix": {}, "refRID": {}, "adId": {},
	// eBay
	"_trkparms": {}, "_trksid": {}, "_from": {},
	// AliExpress / Alibaba
	"spm": {}, "spm_id_from": {}, "spm-cnt": {},
	"algo_expid": {}, "algo_pvid": {}, "gps-id": {},
	"ws_ab_test": {}, "btsid": {}, "cv": {}, "af": {}, "mall_affr": {},
	// Yandex
	"yclid": {}, "_openstat": {}, "redircnt": {},
	// Email marketing
	"mkt_tok": {},
	"mc_cid":  {}, "mc_eid": {}, "mc_tc": {},
	"ml_subscriber": {}, "ml_subscriber_hash": {},
	// CRM
	"_hsenc": {}, "_hsmi": {}, "hsCtaTracking": {},
	"hsLang": {}, "hsformkey": {},
	"vmcid": {},
	// Analytics universal
	"_ga": {}, "_gl": {},
	// Generic trackers
	"wickedid": {}, "Echobox": {}, "tracking_source": {},
	"ceneo_spo": {}, "srcid": {}, "trackid": {}, "trkid": {}, "epik": {},
	// Bilibili / Chinese
	"from_source": {}, "vd_source": {}, "share_source": {}, "share_medium": {},
	"share_plat": {}, "refer_from": {}, "up_id": {},
}

// trackingParamPrefixes: any param whose name STARTS WITH one of these prefixes
// is dropped. Catches family expansions (utm_source, utm_medium, ...).
var trackingParamPrefixes = []string{
	"utm_",     // UTM (universal)
	"mtm_",     // Matomo
	"itm_",     // Item tracking
	"ga_",      // Google Analytics extras
	"mc_",      // Mailchimp extras
	"ml_",      // MailerLite extras
	"_hs",      // HubSpot _hs*
	"__hs",     // HubSpot __hs*
	"pf_rd_",   // Amazon page format
	"__mk_",    // Amazon market
	"__tn_",    // Facebook thread notification
	"__xts__",  // Facebook
	"__cft__",  // Facebook
	"scm-",     // Alibaba SCM
	"scm_",     // Alibaba SCM
	"li_",      // LinkedIn variants
	"pk_",      // Piwik (legacy Matomo)
	"hc_",      // Facebook heading comment
	"_branch_", // Branch deep link
}

// CanonicalURL normalizes a URL for consistent dedup:
// lowercase scheme+host, strip trailing slash, strip fragment, drop tracking
// params, sort remaining query params.
func CanonicalURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return strings.ToLower(rawURL)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	u.Path = strings.TrimRight(u.Path, "/")
	if u.Path == "" {
		u.Path = "/"
	}
	q := u.Query()
	for key := range q {
		if _, drop := trackingParams[key]; drop {
			q.Del(key)
			continue
		}
		for _, prefix := range trackingParamPrefixes {
			if strings.HasPrefix(key, prefix) {
				q.Del(key)
				break
			}
		}
	}
	// Sort remaining params for consistent hashing.
	u.RawQuery = q.Encode()
	return u.String()
}

// ExtractDomain extracts the domain (host without port) from a URL.
func ExtractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	return strings.ToLower(host)
}
