package scraper

import (
	"reflect"
	"testing"
)

func TestDiscoverContactLinks(t *testing.T) {
	tests := []struct {
		name string
		page string
		html string
		max  int
		want []string
	}{
		{
			name: "relative href with keyword path",
			page: "https://studio.example/",
			html: `<a href="/kontakt-uns">Reach</a><a href="/blog">Blog</a>`,
			max:  3,
			want: []string{"https://studio.example/kontakt-uns"},
		},
		{
			name: "anchor text match, www variant allowed",
			page: "https://studio.example/",
			html: `<a href="https://www.studio.example/info/reach">Get in touch</a>`,
			max:  3,
			want: []string{"https://www.studio.example/info/reach"},
		},
		{
			name: "impressum and team by path",
			page: "https://studio.example/en/",
			html: `<a href="impressum">Legal</a><a href="/our-team/">People</a>`,
			max:  3,
			want: []string{"https://studio.example/en/impressum", "https://studio.example/our-team/"},
		},
		{
			name: "skips guessed paths, homepage, trailing slash variants",
			page: "https://studio.example/",
			html: `<a href="/contact">C</a><a href="/Contact-Us/">C</a><a href="/about">A</a><a href="/about-us/">A</a><a href="/">Home</a>`,
			max:  3,
			want: nil,
		},
		{
			name: "skips other hosts, mailto, tel, fragments, javascript",
			page: "https://studio.example/",
			html: `<a href="https://other.example/contact-team">x</a><a href="mailto:contact@studio.example">m</a>` +
				`<a href="tel:+000">contact</a><a href="#contact">contact</a><a href="javascript:void(0)">contact</a>` +
				`<a href="https://sub.studio.example/team">sub</a>`,
			max:  3,
			want: nil,
		},
		{
			name: "skips assets",
			page: "https://studio.example/",
			html: `<a href="/team.pdf">Team</a><a href="/img/contact.png">Contact</a>`,
			max:  3,
			want: nil,
		},
		{
			name: "whole word only: steam and aboutface do not match",
			page: "https://studio.example/",
			html: `<a href="/steam-room">Steam room</a><a href="/aboutface">Face</a>`,
			max:  3,
			want: nil,
		},
		{
			name: "long anchor text ignored",
			page: "https://studio.example/",
			html: `<a href="/p/1">Read all about our wonderful studio and its long history here</a>`,
			max:  3,
			want: nil,
		},
		{
			name: "contact ranks before about/team, capped, deduped",
			page: "https://studio.example/",
			html: `<a href="/about-the-studio">About</a><a href="/our-team">Team</a><a href="/our-team">Team</a>` +
				`<a href="/get-in-touch">Hi</a><a href="/contacto">Contacto</a><a href="/kontakt">K</a>`,
			max:  3,
			want: []string{"https://studio.example/kontakt", "https://studio.example/contacto", "https://studio.example/get-in-touch"},
		},
		{
			name: "query string kept, fragment dropped",
			page: "http://studio.example/home",
			html: `<a href="/?page_id=7&x=1#top">Contact</a><a href="/p?page_id=9#f">Contact us</a>`,
			max:  3,
			want: []string{"http://studio.example/p?page_id=9"},
		},
		{name: "max zero", page: "https://studio.example/", html: `<a href="/kontakt">x</a>`, max: 0, want: nil},
		{name: "bad page url", page: "://bad", html: `<a href="/kontakt">x</a>`, max: 3, want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DiscoverContactLinks(tc.page, []byte(tc.html), tc.max)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
