package scraper

import "testing"

func TestExtractEmailNames(t *testing.T) {
	tests := []struct {
		name     string
		html     string
		email    string
		business string
		want     string // "" = no name
		source   string
	}{
		// must accept
		{"mailto anchor name", `<p><a href="mailto:sophen@anikhotelsgroup.com">Sophen Pillay</a></p>`,
			"sophen@anikhotelsgroup.com", "Anik Hotels", "Sophen Pillay", NameSourceMailto},
		{"jsonld person", `<script type="application/ld+json">{"@context":"https://schema.org","@type":"Person","name":"Anja Keyser","email":"mailto:Anja.Keyser@stadt.erlangen.de"}</script>`,
			"anja.keyser@stadt.erlangen.de", "", "Anja Keyser", NameSourceJSONLD},
		{"jsonld person in graph, local part unrelated", `<script type="application/ld+json">{"@graph":[{"@type":"Organization","name":"Acme"},{"@type":"Person","name":"Maria Lopez","email":"studio-owner@x.com"}]}</script>`,
			"studio-owner@x.com", "Acme", "", ""}, // role local part wins even with JSON-LD
		{"jsonld nested founder", `<script type="application/ld+json">{"@type":"LocalBusiness","name":"Acme","founder":{"name":"Tim Berg","email":"t@acme.com"}}</script>`,
			"t@acme.com", "Acme", "Tim Berg", NameSourceJSONLD},
		{"adjacent dash", `<p>Mateusz Salabura – mateuszsalabura261@gmail.com</p>`,
			"mateuszsalabura261@gmail.com", "", "Mateusz Salabura", NameSourceAdjacent},
		{"adjacent angle", `<p>Jane Doe &lt;j.doe@x.com&gt;</p>`,
			"j.doe@x.com", "", "Jane Doe", NameSourceAdjacent},
		{"adjacent comma", `<p>Jane Doe, jane@x.com</p>`, "jane@x.com", "", "Jane Doe", NameSourceAdjacent},
		{"adjacent label and pipe", `<div>Contact: Jane Doe | jane@x.com</div>`, "jane@x.com", "", "Jane Doe", NameSourceAdjacent},
		{"adjacent inline markup", `<p><strong>Zoë O'Brien</strong>: <a href="mailto:zoe.obrien@x.com">zoe.obrien@x.com</a></p>`,
			"zoe.obrien@x.com", "", "Zoë O'Brien", NameSourceAdjacent},
		{"initial plus surname, short surname", `<p>Wei Li <a href="mailto:wli@x.com">wli@x.com</a></p>`,
			"wli@x.com", "", "Wei Li", NameSourceAdjacent},

		{"untyped node under employee", `<script type="application/ld+json">{"@type":"LocalBusiness","name":"Acme","employee":{"name":"Tim Berg","email":"tim@acme.com"}}</script>`,
			"tim@acme.com", "Acme", "Tim Berg", NameSourceJSONLD},

		// must reject
		{"organization under member", `<script type="application/ld+json">{"@type":"Organization","name":"Acme","member":{"@type":"Organization","name":"Bella Vita","email":"bella@x.com"}}</script>`,
			"bella@x.com", "Acme", "", ""},
		{"typed non-person array under founder", `<script type="application/ld+json">{"founder":{"@type":["Organization","LocalBusiness"],"name":"Bella Vita","email":"bella@x.com"}}</script>`,
			"bella@x.com", "Acme", "", ""},
		{"anchor Email us", `<a href="mailto:maria@x.com">Email us</a>`, "maria@x.com", "", "", ""},
		{"anchor Contact", `<a href="mailto:maria@x.com">Contact</a>`, "maria@x.com", "", "", ""},
		{"anchor is the address", `<a href="mailto:maria@x.com">maria@x.com</a>`, "maria@x.com", "", "", ""},
		{"info.limerick", `<p>Info Limerick: <a href="mailto:info.limerick@x.ie">Info Limerick</a></p>`, "info.limerick@x.ie", "", "", ""},
		{"role email next to real name", `<p>Jane Doe – info@x.com</p><a href="mailto:kontakt@x.de">Jane Doe</a>`, "info@x.com", "", "", ""},
		{"kontakt role", `<a href="mailto:kontakt@x.de">Jane Doe</a>`, "kontakt@x.de", "", "", ""},
		{"name does not match local part (mailto)", `<a href="mailto:maria@x.com">John Smith</a>`, "maria@x.com", "", "", ""},
		{"name does not match local part (adjacent)", `<p>John Smith maria@x.com</p>`, "maria@x.com", "", "", ""},
		{"business name as anchor", `<a href="mailto:acme@x.com">Acme Yoga Berg</a>`, "acme@x.com", "Acme Yoga Berg", "", ""},
		{"business name equal, no role words", `<a href="mailto:sunrise@x.com">Sunrise Garden</a>`, "sunrise@x.com", "Sunrise Garden", "", ""},
		{"lowercase tokens", `<p>please contact jane doe jane@x.com</p>`, "jane@x.com", "", "", ""},
		{"single token", `<p>Jane jane@x.com</p>`, "jane@x.com", "", "", ""},
		{"five tokens", `<p>Anna Maria Luisa Jane Doe jane@x.com</p>`, "jane@x.com", "", "", ""},
		{"digits in name", `<p>Jane D0e jane@x.com</p>`, "jane@x.com", "", "", ""},
		{"role word in name", `<p>Yoga Studio jane@x.com</p>`, "jane@x.com", "", "", ""},
		{"jsonld Organization email is not a person", `<script type="application/ld+json">{"@type":"Organization","name":"Jane Doe","email":"jane@x.com"}</script>`,
			"jane@x.com", "", "", ""},
		{"jsonld person with other email", `<script type="application/ld+json">{"@type":"Person","name":"Jane Doe","email":"other@x.com"}</script>`,
			"jane@x.com", "", "", ""},
		{"email is substring of longer address", `<p>Jane Doe xjane@x.com</p>`, "jane@x.com", "", "", ""},
		{"no evidence", `<p>write to jane@x.com</p>`, "jane@x.com", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractEmailNames([]byte(tc.html), []string{tc.email}, tc.business)[tc.email]
			if got.Name != tc.want || got.Source != tc.source {
				t.Fatalf("got %+v, want name=%q source=%q", got, tc.want, tc.source)
			}
		})
	}
}

func TestEmailNamesJSON(t *testing.T) {
	names := map[string]EmailName{"a@x.com": {"Jane Doe", NameSourceMailto}, "b@x.com": {"Tim Berg", NameSourceAdjacent}}
	if got := string(EmailNamesJSON(names, []string{"A@x.com", "c@x.com"})); got != `{"a@x.com":{"name":"Jane Doe","source":"mailto_text"}}` {
		t.Fatalf("got %s", got)
	}
	if got := EmailNamesJSON(names, []string{"c@x.com"}); got != nil {
		t.Fatalf("want nil, got %s", got)
	}
}
