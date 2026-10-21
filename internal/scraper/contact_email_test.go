//go:build playwright

package scraper

import (
	"slices"
	"testing"
)

func TestIsCleanEmailTLD(t *testing.T) {
	cases := []struct {
		email string
		want  bool
	}{
		{"info@evofit-studio.ky", true},
		{"hi@healthy-her.rocks", true},
		{"hi@bodyworks.fit", true},
		{"hi@be-well.nyc", true},
		{"hi@coach-anna.eu", true},
		{"hi@north-path.ai", true},
		{"hi@north-path.llc", true},
		{"hi@north-path.co.uk", true},
		{"hi@north-path.com.au", true},
		{"info@studio.uk.com", true},
		{"hi@brand.us.com", true},
		{"logo@2x.png", false},
		{"info@inivie.comview", false},
		{"hi@balifitness.asiaphone", false},
		{"x@ttbeach.clubmenu", false},
		{"abcdefghijklmnopqrstuvwxyzabcdefghijklmno1@north-path.ca", false},
		{"0123456789abcdef0123456789abcdef@north-path.ca", false},
	}
	for _, c := range cases {
		if got := isCleanEmail(c.email); got != c.want {
			t.Errorf("isCleanEmail(%q) = %v, want %v", c.email, got, c.want)
		}
	}
}

func TestExtractContactsEmails(t *testing.T) {
	cases := []struct {
		name string
		html string
		want []string
	}{
		{"plain text .ky", `<html><body><p class="body">info@evofit-studio.ky</p></body></html>`, []string{"info@evofit-studio.ky"}},
		{"bare email href", `<html><body><a href="eric@north-path.ca" data-type="link">Email</a></body></html>`, []string{"eric@north-path.ca"}},
		{"bare email href escaped", `<html><body><a href=" eric%40north-path.ca ">Email</a></body></html>`, []string{"eric@north-path.ca"}},
		{"json blob mailto", `<html><body><script>{"profileUrl":"mailto:laurie@healthy-her.rocks"}</script></body></html>`, []string{"laurie@healthy-her.rocks"}},
		{"glued prefix", `<html><body><p>wellnesscommunity</p><p>info@evofit-studio.ky</p></body></html>`, []string{"info@evofit-studio.ky"}},
		{"href non-email ignored", `<html><body><a href="https://north-path.ca/a@b">x</a></body></html>`, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ExtractContacts([]byte(c.html)).Emails
			if !slices.Equal(got, c.want) {
				t.Errorf("emails = %v, want %v", got, c.want)
			}
		})
	}
}
