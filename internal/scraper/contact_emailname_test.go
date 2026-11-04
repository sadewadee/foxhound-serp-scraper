//go:build playwright

package scraper

import "testing"

// ExtractContacts must wire per-email names end to end (emails found by
// foxhound, names by ExtractEmailNames).
func TestExtractContactsEmailNames(t *testing.T) {
	cd := ExtractContacts([]byte(`<html><head><title>Anik Hotels</title></head><body>
		<a href="mailto:sophen@anikhotelsgroup.com">Sophen Pillay</a>
		<a href="mailto:info@anikhotelsgroup.com">Email us</a></body></html>`))
	if got := cd.EmailNames["sophen@anikhotelsgroup.com"]; got.Name != "Sophen Pillay" || got.Source != NameSourceMailto {
		t.Fatalf("sophen: got %+v (emails %v)", got, cd.Emails)
	}
	if _, ok := cd.EmailNames["info@anikhotelsgroup.com"]; ok {
		t.Fatalf("role email must not get a name: %+v", cd.EmailNames)
	}
}
