package prodir

import (
	"reflect"
	"testing"
)

func TestDecodeCFEmail(t *testing.T) {
	// key 0x2a, "fixture.person@example.com"
	const enc = "2a4c43525e5f584f045a4f585945446a4f524b475a464f04494547"
	got, ok := decodeCFEmail(enc)
	if !ok || got != "fixture.person@example.com" {
		t.Errorf("decodeCFEmail = %q, %v", got, ok)
	}
	for _, bad := range []string{"", "2a", "zz12", "2a4"} {
		if got, ok := decodeCFEmail(bad); ok {
			t.Errorf("decodeCFEmail(%q) = %q, want failure", bad, got)
		}
	}
}

func TestEmailsIn(t *testing.T) {
	const enc = "2a4c43525e5f584f045a4f585945446a4f524b475a464f04494547"
	doc, err := docFromBody([]byte(`<div id="x">
		<a href="mailto:Plain.One@Example.org?subject=Hi">mail</a>
		<a href="mailto:&#97;&#110;&#111;&#116;&#104;&#101;&#114;@example.net">mail</a>
		<a class="__cf_email__" href="/cdn-cgi/l/email-protection#` + enc + `" data-cfemail="` + enc + `">[email&#160;protected]</a>
		<span data-cfemail="` + enc + `">[email&#160;protected]</span>
		<a href="/cdn-cgi/l/email-protection#` + enc + `">[email&#160;protected]</a>
		<a href="mailto:">empty</a>
		<a href="https://example.com/not-an-email">link</a>
		</div>`))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"plain.one@example.org", "another@example.net", "fixture.person@example.com"}
	if got := emailsIn(doc.Find("#x")); !reflect.DeepEqual(got, want) {
		t.Errorf("emailsIn = %v, want %v", got, want)
	}
	// the anchor itself counts, not only descendants
	if got := emailsIn(doc.Find("a.__cf_email__")); !reflect.DeepEqual(got, []string{"fixture.person@example.com"}) {
		t.Errorf("emailsIn(anchor) = %v", got)
	}
}

func TestAbsURL(t *testing.T) {
	for _, c := range []struct{ base, ref, want string }{
		{"https://example.org/dir/list/", "/teacher/a/", "https://example.org/teacher/a/"},
		{"https://example.org/dir/list/", "detail/b", "https://example.org/dir/list/detail/b"},
		{"https://example.org/dir/list", "detail/b", "https://example.org/dir/detail/b"},
		{"https://example.org/dir/", "//cdn.example.com/x.png", "https://cdn.example.com/x.png"},
		{"https://example.org/dir/", "https://other.example.net/p?q=1", "https://other.example.net/p?q=1"},
		{"https://example.org/dir/?page=2", "?page=3", "https://example.org/dir/?page=3"},
		{"https://example.org/dir/", "  ", ""},
		{"https://example.org/dir/", "", ""},
	} {
		if got := absURL(c.base, c.ref); got != c.want {
			t.Errorf("absURL(%q, %q) = %q, want %q", c.base, c.ref, got, c.want)
		}
	}
}

func TestDocFromBody(t *testing.T) {
	doc, err := docFromBody([]byte(`<html><body><p class="a">one</p><p class="a">two</p></body></html>`))
	if err != nil {
		t.Fatal(err)
	}
	if n := doc.Find("p.a").Length(); n != 2 {
		t.Errorf("found %d paragraphs, want 2", n)
	}
}
