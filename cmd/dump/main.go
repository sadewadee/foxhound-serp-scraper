//go:build playwright

package main

import (
	"fmt"
	"strings"

	"github.com/sadewadee/serp-scraper/internal/query"
)

func main() {
	fmt.Println("BEGIN;")
	fmt.Println("CREATE TABLE IF NOT EXISTS city_country (city text PRIMARY KEY, country text NOT NULL);")
	fmt.Println("TRUNCATE city_country;")
	for country, cities := range query.Cities {
		for _, city := range cities {
			c := strings.ReplaceAll(strings.ToLower(city), "'", "''")
			cn := strings.ReplaceAll(country, "'", "''")
			fmt.Printf("INSERT INTO city_country VALUES ('%s', '%s') ON CONFLICT (city) DO NOTHING;\n", c, cn)
		}
	}
	fmt.Println("CREATE TABLE IF NOT EXISTS niches (niche text PRIMARY KEY, word_count int);")
	fmt.Println("TRUNCATE niches;")
	all := append([]string{}, query.Niches...)
	all = append(all, query.PersonalNiches...)
	for _, n := range all {
		nl := strings.ReplaceAll(strings.ToLower(n), "'", "''")
		fmt.Printf("INSERT INTO niches VALUES ('%s', %d) ON CONFLICT (niche) DO NOTHING;\n", nl, len(strings.Fields(nl)))
	}
	fmt.Println("COMMIT;")
}
