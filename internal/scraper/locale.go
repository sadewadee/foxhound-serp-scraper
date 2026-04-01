//go:build playwright

package scraper

// GeoLocale holds search engine locale params for a country.
type GeoLocale struct {
	GoogleGL string // Google geolocation param (e.g. "jp")
	GoogleHL string // Google host language (e.g. "ja")
	BingCC   string // Bing country code (e.g. "jp")
	BingLang string // Bing language (e.g. "ja")
	DDGKL    string // DuckDuckGo region (e.g. "jp-jp")
}

// CountryLocale maps country names (matching wellness.go Cities keys) to locale params.
var CountryLocale = map[string]*GeoLocale{
	// --- Southeast Asia ---
	"Indonesia": {GoogleGL: "id", GoogleHL: "id", BingCC: "id", BingLang: "id", DDGKL: "id-id"},
	"Thailand":  {GoogleGL: "th", GoogleHL: "th", BingCC: "th", BingLang: "th", DDGKL: "th-th"},
	"Vietnam":   {GoogleGL: "vn", GoogleHL: "vi", BingCC: "vn", BingLang: "vi", DDGKL: "vn-vi"},
	"Philippines": {GoogleGL: "ph", GoogleHL: "tl", BingCC: "ph", BingLang: "en", DDGKL: "ph-en"},
	"Malaysia":    {GoogleGL: "my", GoogleHL: "ms", BingCC: "my", BingLang: "ms", DDGKL: "my-ms"},
	"Singapore":   {GoogleGL: "sg", GoogleHL: "en", BingCC: "sg", BingLang: "en", DDGKL: "sg-en"},
	"Cambodia":    {GoogleGL: "kh", GoogleHL: "km", BingCC: "kh", BingLang: "en", DDGKL: "kh-en"},
	"Myanmar":     {GoogleGL: "mm", GoogleHL: "my", BingCC: "mm", BingLang: "en", DDGKL: "mm-en"},

	// --- English-speaking ---
	"United States":  {GoogleGL: "us", GoogleHL: "en", BingCC: "us", BingLang: "en", DDGKL: "us-en"},
	"Canada":         {GoogleGL: "ca", GoogleHL: "en", BingCC: "ca", BingLang: "en", DDGKL: "ca-en"},
	"Australia":      {GoogleGL: "au", GoogleHL: "en", BingCC: "au", BingLang: "en", DDGKL: "au-en"},
	"New Zealand":    {GoogleGL: "nz", GoogleHL: "en", BingCC: "nz", BingLang: "en", DDGKL: "nz-en"},
	"United Kingdom": {GoogleGL: "uk", GoogleHL: "en", BingCC: "gb", BingLang: "en", DDGKL: "uk-en"},
	"Ireland":        {GoogleGL: "ie", GoogleHL: "en", BingCC: "ie", BingLang: "en", DDGKL: "ie-en"},

	// --- Western Europe ---
	"Germany":     {GoogleGL: "de", GoogleHL: "de", BingCC: "de", BingLang: "de", DDGKL: "de-de"},
	"Austria":     {GoogleGL: "at", GoogleHL: "de", BingCC: "at", BingLang: "de", DDGKL: "at-de"},
	"Switzerland": {GoogleGL: "ch", GoogleHL: "de", BingCC: "ch", BingLang: "de", DDGKL: "ch-de"},
	"France":      {GoogleGL: "fr", GoogleHL: "fr", BingCC: "fr", BingLang: "fr", DDGKL: "fr-fr"},
	"Spain":       {GoogleGL: "es", GoogleHL: "es", BingCC: "es", BingLang: "es", DDGKL: "es-es"},
	"Netherlands": {GoogleGL: "nl", GoogleHL: "nl", BingCC: "nl", BingLang: "nl", DDGKL: "nl-nl"},
	"Belgium":     {GoogleGL: "be", GoogleHL: "nl", BingCC: "be", BingLang: "nl", DDGKL: "be-nl"},
	"Italy":       {GoogleGL: "it", GoogleHL: "it", BingCC: "it", BingLang: "it", DDGKL: "it-it"},
	"Portugal":    {GoogleGL: "pt", GoogleHL: "pt", BingCC: "pt", BingLang: "pt", DDGKL: "pt-pt"},

	// --- Scandinavia ---
	"Sweden":  {GoogleGL: "se", GoogleHL: "sv", BingCC: "se", BingLang: "sv", DDGKL: "se-sv"},
	"Denmark": {GoogleGL: "dk", GoogleHL: "da", BingCC: "dk", BingLang: "da", DDGKL: "dk-da"},
	"Norway":  {GoogleGL: "no", GoogleHL: "no", BingCC: "no", BingLang: "nb", DDGKL: "no-no"},
	"Finland": {GoogleGL: "fi", GoogleHL: "fi", BingCC: "fi", BingLang: "fi", DDGKL: "fi-fi"},

	// --- Eastern Europe ---
	"Poland":         {GoogleGL: "pl", GoogleHL: "pl", BingCC: "pl", BingLang: "pl", DDGKL: "pl-pl"},
	"Czech Republic": {GoogleGL: "cz", GoogleHL: "cs", BingCC: "cz", BingLang: "cs", DDGKL: "cz-cs"},
	"Hungary":        {GoogleGL: "hu", GoogleHL: "hu", BingCC: "hu", BingLang: "hu", DDGKL: "hu-hu"},
	"Romania":        {GoogleGL: "ro", GoogleHL: "ro", BingCC: "ro", BingLang: "ro", DDGKL: "ro-ro"},
	"Croatia":        {GoogleGL: "hr", GoogleHL: "hr", BingCC: "hr", BingLang: "hr", DDGKL: "hr-hr"},
	"Greece":         {GoogleGL: "gr", GoogleHL: "el", BingCC: "gr", BingLang: "el", DDGKL: "gr-el"},
	"Turkey":         {GoogleGL: "tr", GoogleHL: "tr", BingCC: "tr", BingLang: "tr", DDGKL: "tr-tr"},

	// --- South Asia ---
	"India":     {GoogleGL: "in", GoogleHL: "en", BingCC: "in", BingLang: "en", DDGKL: "in-en"},
	"Sri Lanka": {GoogleGL: "lk", GoogleHL: "si", BingCC: "lk", BingLang: "en", DDGKL: "lk-en"},
	"Nepal":     {GoogleGL: "np", GoogleHL: "ne", BingCC: "np", BingLang: "en", DDGKL: "np-en"},

	// --- Middle East ---
	"UAE":          {GoogleGL: "ae", GoogleHL: "ar", BingCC: "ae", BingLang: "ar", DDGKL: "ae-ar"},
	"Qatar":        {GoogleGL: "qa", GoogleHL: "ar", BingCC: "qa", BingLang: "ar", DDGKL: "qa-ar"},
	"Saudi Arabia": {GoogleGL: "sa", GoogleHL: "ar", BingCC: "sa", BingLang: "ar", DDGKL: "sa-ar"},
	"Bahrain":      {GoogleGL: "bh", GoogleHL: "ar", BingCC: "bh", BingLang: "ar", DDGKL: "bh-ar"},

	// --- East Asia ---
	"Japan":       {GoogleGL: "jp", GoogleHL: "ja", BingCC: "jp", BingLang: "ja", DDGKL: "jp-jp"},
	"South Korea": {GoogleGL: "kr", GoogleHL: "ko", BingCC: "kr", BingLang: "ko", DDGKL: "kr-ko"},
	"Taiwan":      {GoogleGL: "tw", GoogleHL: "zh-TW", BingCC: "tw", BingLang: "zh-Hant", DDGKL: "tw-tzh"},
	"Hong Kong":   {GoogleGL: "hk", GoogleHL: "zh-TW", BingCC: "hk", BingLang: "zh-Hant", DDGKL: "hk-tzh"},

	// --- Africa ---
	"South Africa": {GoogleGL: "za", GoogleHL: "en", BingCC: "za", BingLang: "en", DDGKL: "za-en"},
	"Kenya":        {GoogleGL: "ke", GoogleHL: "en", BingCC: "ke", BingLang: "en", DDGKL: "ke-en"},
	"Nigeria":      {GoogleGL: "ng", GoogleHL: "en", BingCC: "ng", BingLang: "en", DDGKL: "ng-en"},
	"Morocco":      {GoogleGL: "ma", GoogleHL: "fr", BingCC: "ma", BingLang: "fr", DDGKL: "ma-fr"},
	"Egypt":        {GoogleGL: "eg", GoogleHL: "ar", BingCC: "eg", BingLang: "ar", DDGKL: "eg-ar"},

	// --- Latin America ---
	"Mexico":     {GoogleGL: "mx", GoogleHL: "es", BingCC: "mx", BingLang: "es", DDGKL: "mx-es"},
	"Brazil":     {GoogleGL: "br", GoogleHL: "pt-BR", BingCC: "br", BingLang: "pt-BR", DDGKL: "br-pt"},
	"Colombia":   {GoogleGL: "co", GoogleHL: "es", BingCC: "co", BingLang: "es", DDGKL: "co-es"},
	"Argentina":  {GoogleGL: "ar", GoogleHL: "es", BingCC: "ar", BingLang: "es", DDGKL: "ar-es"},
	"Chile":      {GoogleGL: "cl", GoogleHL: "es", BingCC: "cl", BingLang: "es", DDGKL: "cl-es"},
	"Peru":       {GoogleGL: "pe", GoogleHL: "es", BingCC: "pe", BingLang: "es", DDGKL: "pe-es"},
	"Costa Rica": {GoogleGL: "cr", GoogleHL: "es", BingCC: "cr", BingLang: "es", DDGKL: "cr-es"},
	"Panama":     {GoogleGL: "pa", GoogleHL: "es", BingCC: "pa", BingLang: "es", DDGKL: "pa-es"},
}

// defaultLocale is returned when a country has no mapping.
var defaultLocale = &GeoLocale{
	GoogleGL: "us",
	GoogleHL: "en",
	BingCC:   "us",
	BingLang: "en",
	DDGKL:    "us-en",
}

// GetLocale returns the GeoLocale for a country, or a default English/US locale.
func GetLocale(country string) *GeoLocale {
	if loc, ok := CountryLocale[country]; ok {
		return loc
	}
	return defaultLocale
}
