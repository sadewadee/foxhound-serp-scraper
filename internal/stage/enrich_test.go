//go:build playwright

package stage

import "testing"

// TestIsSkipDomain_Layer1_ExactMatch verifies the production-confirmed
// off-niche hosts (domains seen in 24h prod data) are blocked, plus a
// regression test for entries that existed before this expansion.
func TestIsSkipDomain_Layer1_ExactMatch(t *testing.T) {
	blocked := []string{
		// Existing pre-expansion (regression — must still block)
		"www.yelp.com", "www.tripadvisor.com", "www.linkedin.com",
		"www.facebook.com", "twitter.com", "x.com", "www.youtube.com",
		"www.amazon.com", "www.booking.com", "rocketreach.co",

		// New: Amazon TLD variants (variant TLD bocor di prod)
		"www.amazon.de", "www.amazon.fr", "www.amazon.co.uk",
		"www.amazon.co.jp", "www.amazon.com.br", "www.amazon.in",

		// New: Q&A platforms (top contamination per prod data)
		"www.zhihu.com", "zhuanlan.zhihu.com",
		"zhidao.baidu.com", "jingyan.baidu.com",
		"stackoverflow.com", "www.justanswer.com", "www.chegg.com",
		"answers.yahoo.com", "chiebukuro.yahoo.co.jp",

		// New: Marketplaces global
		"www.etsy.com", "www.aliexpress.com",
		"www.ebay.de", "www.kleinanzeigen.de", "www.mobile.de",
		"www.vinted.fr", "www.leboncoin.fr",

		// New: News/media
		"www.cnbc.com", "www.bloomberg.com", "www.nytimes.com",
		"edition.cnn.com", "join1440.com", "tecnobits.com",

		// New: Software / corporate help
		"www.adobe.com", "www.canva.com", "www.dell.com",
		"github.com", "gitlab.com", "apkpure.com",

		// New: Travel
		"www.expedia.com", "www.agoda.com",

		// New: Health/medical
		"www.psychologytoday.com", "www.drugs.com", "www.healthline.com",
		"insurancetoday.cc", "goodins.life",

		// New: Adult (sample dari logs)
		"sexfinder.com", "friendfinder.com", "bakecaincontrii.com",

		// New: Gaming
		"poki.com", "www.jeuxvideo.com", "www.mlb.com",

		// New: Fitness brand corporate (no public business contact)
		"www.nike.com", "www.adidas.com", "www.onepeloton.com",
		"www.lululemon.com", "www.rei.com", "www.underarmour.com",

		// New: Other
		"www.ikea.com", "www.imdb.com", "www.scribd.com",
		"www.ups.com", "www.fedex.com", "www.dhl.com",
	}
	for _, d := range blocked {
		t.Run(d, func(t *testing.T) {
			if !isSkipDomain(d) {
				t.Errorf("isSkipDomain(%q) = false, want true (in blocklist)", d)
			}
		})
	}
}

// TestIsSkipDomain_Layer2_SuffixMatch verifies gov/edu/mil suffix blocking
// for English + non-English country codes.
func TestIsSkipDomain_Layer2_SuffixMatch(t *testing.T) {
	blocked := []string{
		// English-speaking
		"harvard.edu", "whitehouse.gov", "armed.mil",
		"cam.ac.uk", "parliament.gov.uk",
		"sydney.edu.au", "act.gov.au",
		// SE Asia
		"ui.ac.id", "kemkes.go.id",
		"nus.edu.sg", "moh.gov.sg",
		"todai.ac.jp", "mhlw.go.jp",
		// EU + LATAM
		"insee.gouv.fr",
		"bundesregierung.gov.de",
		"hacienda.gob.mx", "moncloa.gob.es",
		// East Asia (use realistic subdomain — bare TLD "gov.cn" never seen
		// as a host name; "data.gov.cn" is the realistic form)
		"tsinghua.ac.cn", "data.gov.cn", "pku.edu.cn",
		"snu.ac.kr", "korea.go.kr",
	}
	for _, d := range blocked {
		t.Run(d, func(t *testing.T) {
			if !isSkipDomain(d) {
				t.Errorf("isSkipDomain(%q) = false, want true (suffix match)", d)
			}
		})
	}
}

// TestIsSkipDomain_Layer3_SubstringMatch verifies forum/community/Q&A/support
// hosts are blocked regardless of TLD via substring patterns.
func TestIsSkipDomain_Layer3_SubstringMatch(t *testing.T) {
	blocked := []string{
		// forum.* — French/German/Turkish forums in prod top 60
		"forum.donanimhaber.com", "forum.quechoisir.org",
		"forum.example.com",
		// forums.* (plural)
		"forums.commentcamarche.net", "forums.macrumors.com",
		// .forum.* (subdomain inside path)
		"abc.forum.example.com", // matches via ".forum." infix
		// community.* — known platforms
		"community.spotify.com", "community.shopify.com",
		// support.* — corporate help (off-niche)
		"support.google.com", "support.apple.com",
		"support.microsoft.com",
		// help.*
		"help.netflix.com", "help.example.com",
		// diskuse.* (Czech forum)
		"diskuse.jakpsatweb.cz",
		// discussions.*
		"discussions.apple.com",
	}
	for _, d := range blocked {
		t.Run(d, func(t *testing.T) {
			if !isSkipDomain(d) {
				t.Errorf("isSkipDomain(%q) = false, want true (substring match)", d)
			}
		})
	}
}

// TestIsSkipDomain_Allowed verifies in-niche / unknown business domains pass
// through (negative cases). Critical: we must not over-block.
func TestIsSkipDomain_Allowed(t *testing.T) {
	allowed := []string{
		// Generic business sites we WANT to scrape
		"www.localfitnessstudio.com",
		"www.acmegym.de",
		"crossfit-jakarta.id",
		"www.yogahaven.co.uk",
		"www.smallbiz.com",

		// Tricky: contains common substrings but should NOT match patterns
		// (we don't have ".forum" without a dot before, but verify "form" alone
		//  doesn't accidentally match "forum.")
		"www.formulatrade.com", // "form" not "forum."
		// "support" suffix in path won't reach domain-level matcher; domain
		// itself doesn't have "support." subdomain
		"www.apparelsupport.com",       // no "support." (only path-like word)
		"www.healthsupport-online.com", // no "support." prefix as subdomain
	}
	for _, d := range allowed {
		t.Run(d, func(t *testing.T) {
			if isSkipDomain(d) {
				t.Errorf("isSkipDomain(%q) = true, want false (should be allowed)", d)
			}
		})
	}
}

// TestIsSkipDomain_EdgeCases verifies degenerate inputs don't panic and
// produce sensible output.
func TestIsSkipDomain_EdgeCases(t *testing.T) {
	cases := []struct {
		in       string
		expected bool
		reason   string
	}{
		{"", false, "empty string never matches blocklist"},
		{"localhost", false, "localhost not blocked"},
		{"127.0.0.1", false, "IP not blocked by host-name patterns"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := isSkipDomain(c.in)
			if got != c.expected {
				t.Errorf("isSkipDomain(%q) = %v, want %v (%s)",
					c.in, got, c.expected, c.reason)
			}
		})
	}
}
