package db

// Guards the category_stats materialized view definition (2026-06-10 niche-aware
// redefinition). The categories endpoint surfaced raw schema.org @type noise
// (Organization, FAQPage, Article, ...) because the original matview aggregated
// bl.category over ALL rows. The redefinition must keep two properties:
//
//  1. off_niche rows are excluded — the denylist trigger/backfills are the
//     category-quality mechanism and the consumer surface must respect them.
//  2. The grouped key is the EFFECTIVE category — niche_category (keyword
//     classifier bucket) first, raw category as fallback.
//
// Also asserts the boot schema embeds the shared categoryStatsSelect const,
// so the schema and the versioned redefinition migration cannot drift apart
// (the 2026-05-25 trigger-vs-backfill drift class).

import (
	"strings"
	"testing"
)

func TestCategoryStatsDefinition(t *testing.T) {
	if !strings.Contains(categoryStatsSelect, "bl.off_niche IS NOT TRUE") {
		t.Error("category_stats must exclude off_niche rows — without this the categories endpoint surfaces schema.org @type noise")
	}
	if !strings.Contains(categoryStatsSelect, "COALESCE(NULLIF(bl.niche_category, ''), bl.category)") {
		t.Error("category_stats must group by the effective category (niche_category first, raw category fallback)")
	}
	if !strings.Contains(schema, categoryStatsSelect) {
		t.Error("boot schema must embed categoryStatsSelect — never inline a second copy of the matview body")
	}
}
