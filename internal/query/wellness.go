//go:build playwright

package query

import (
	"fmt"
	"strings"
)

// Niches for wellness/fitness industry — business names.
var Niches = []string{
	// Yoga
	"yoga studio", "bikram yoga", "hot yoga", "vinyasa yoga",
	"ashtanga yoga", "yin yoga", "aerial yoga", "prenatal yoga",
	"power yoga", "kundalini yoga", "restorative yoga",
	// Pilates
	"pilates studio", "pilates class", "reformer pilates",
	// Gym & Fitness
	"gym", "fitness center", "health club", "crossfit", "crossfit box",
	"boxing gym", "kickboxing gym", "mma gym", "martial arts school",
	"functional fitness", "boutique gym", "women gym",
	// Wellness
	"wellness center", "wellness spa", "wellness retreat",
	"healing center", "holistic healing", "holistic wellness",
	"detox center", "ayurveda center",
	// Mind
	"meditation center", "meditation class", "mindfulness center",
	"breathwork studio", "sound healing",
	// Body
	"spa", "massage", "day spa", "thai massage", "sports massage",
	"float tank", "cryotherapy", "infrared sauna",
	// Movement
	"dance studio", "barre studio", "pole fitness",
	// Nutrition
	"nutrition clinic", "dietitian", "juice bar", "health food store",
	// Beauty-wellness crossover
	"beauty salon", "skin clinic", "aesthetics clinic",
	"hair salon", "nail salon", "barbershop",
}

// PersonalNiches target individual practitioners (more likely to have gmail/yahoo).
var PersonalNiches = []string{
	// Yoga
	"yoga instructor", "yoga teacher", "yoga therapist",
	"prenatal yoga teacher", "kids yoga teacher",
	// Pilates
	"pilates instructor", "pilates teacher",
	// Fitness
	"personal trainer", "fitness coach", "fitness instructor",
	"crossfit coach", "strength coach", "conditioning coach",
	"boxing trainer", "kickboxing instructor",
	"running coach", "triathlon coach", "swimming coach",
	// Wellness
	"wellness coach", "wellness practitioner", "wellness consultant",
	"health coach", "life coach", "nutrition coach",
	"nutritionist", "dietitian",
	// Mind
	"meditation teacher", "meditation guide",
	"breathwork facilitator", "mindfulness coach",
	"hypnotherapist", "counselor", "therapist",
	// Body
	"massage therapist", "sports massage therapist",
	"reiki practitioner", "reiki healer", "reiki master",
	"acupuncturist", "chiropractor",
	"physical therapist", "physiotherapist",
	"osteopath", "craniosacral therapist",
	// Traditional
	"naturopath", "ayurvedic practitioner", "herbalist",
	"homeopath", "traditional healer",
	// Movement
	"dance instructor", "zumba instructor", "barre instructor",
	"pole instructor", "aerial instructor",
	// Beauty
	"esthetician", "skin therapist", "beauty therapist",
	"lash technician", "makeup artist", "hair stylist",
	"barber", "nail technician",
}

// WellnessTemplates for business search query variations.
var WellnessTemplates = []string{
	"%s %s",           // "yoga studio bali"
	"%s in %s",        // "yoga studio in bali"
	"%s near %s",      // "yoga studio near bali"
	"best %s in %s",   // "best yoga studio in bali"
	"%s %s contact",   // "yoga studio bali contact"
	"%s %s email",     // "yoga studio bali email"
}

// PersonalTemplates target individuals — higher gmail/yahoo yield.
var PersonalTemplates = []string{
	"%s %s",                       // "yoga instructor bali"
	"%s in %s",                    // "yoga instructor in bali"
	"%s %s email",                 // "yoga instructor bali email"
	"%s %s contact",               // "yoga instructor bali contact"
	"%s %s \"@gmail.com\"",        // "yoga instructor bali" "@gmail.com"
	"%s %s \"@yahoo.com\"",        // "yoga instructor bali" "@yahoo.com"
	"hire %s in %s",               // "hire yoga instructor in bali"
	"freelance %s %s",             // "freelance personal trainer bali"
	"%s %s instagram",             // "yoga instructor bali instagram" (bio has email)
}

// Cities organized by country — includes neighborhoods for major metros.
var Cities = map[string][]string{
	// Indonesia
	"Indonesia": {
		"Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
		"Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
		"Denpasar", "Bali", "Ubud", "Canggu", "Seminyak", "Sanur", "Kuta", "Nusa Dua", "Uluwatu",
		"Yogyakarta", "Malang", "Solo", "Bogor", "Balikpapan",
		"Manado", "Lombok", "Batam", "Samarinda", "Pontianak",
		// Jakarta neighborhoods
		"Kemang", "Senopati", "Menteng", "Kelapa Gading", "PIK Jakarta",
		"Sudirman", "Kuningan", "Pondok Indah",
	},
	// United States
	"United States": {
		"New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
		"San Antonio", "San Diego", "Dallas", "San Jose", "Austin",
		"San Francisco", "Seattle", "Denver", "Portland", "Miami",
		"Atlanta", "Boston", "Nashville", "Las Vegas", "Minneapolis",
		"Scottsdale", "Boulder", "Santa Monica", "Sedona", "Asheville",
		"Honolulu", "Maui", "Oahu",
		// NYC neighborhoods
		"Brooklyn", "Manhattan", "Queens", "Williamsburg", "Park Slope",
		"Upper East Side", "Upper West Side", "SoHo", "Tribeca", "Harlem",
		// LA neighborhoods
		"Venice Beach", "Silver Lake", "West Hollywood", "Santa Monica",
		"Beverly Hills", "Pasadena", "Glendale", "Culver City",
		// Bay Area
		"Oakland", "Berkeley", "Palo Alto", "Mountain View", "Fremont",
		// Other metros
		"Plano", "Frisco", "Irving", "Mesa", "Tempe", "Chandler",
		"Tampa", "Orlando", "Jacksonville", "Fort Lauderdale",
		"Charlotte", "Raleigh", "Durham", "Richmond", "Virginia Beach",
		"Salt Lake City", "Boise", "Tucson", "Albuquerque",
		"Pittsburgh", "Philadelphia", "Baltimore", "Washington DC",
		"San Juan",
	},
	// Canada
	"Canada": {
		"Toronto", "Vancouver", "Montreal", "Calgary", "Edmonton",
		"Ottawa", "Winnipeg", "Quebec City", "Halifax", "Victoria",
		"Whistler", "Kelowna", "Saskatoon", "Regina",
	},
	// Australia
	"Australia": {
		"Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
		"Gold Coast", "Canberra", "Hobart", "Darwin", "Newcastle",
		"Byron Bay", "Bondi", "Manly", "Surry Hills", "Fitzroy",
		"Noosa", "Cairns", "Townsville", "Wollongong", "Geelong",
		"Sunshine Coast", "Fremantle", "Parramatta",
	},
	// New Zealand
	"New Zealand": {
		"Auckland", "Wellington", "Christchurch", "Hamilton", "Tauranga",
		"Queenstown", "Dunedin", "Nelson",
	},
	// United Kingdom
	"United Kingdom": {
		"London", "Manchester", "Birmingham", "Leeds", "Liverpool",
		"Bristol", "Edinburgh", "Glasgow", "Brighton", "Oxford",
		"Cambridge", "Bath", "Nottingham", "Sheffield", "Cardiff",
		"Belfast", "York", "Exeter", "Southampton", "Reading",
		// London neighborhoods
		"Shoreditch", "Camden", "Hackney", "Notting Hill", "Chelsea",
		"Islington", "Fulham", "Brixton", "Clapham", "Richmond",
	},
	// Ireland
	"Ireland": {
		"Dublin", "Cork", "Galway", "Limerick", "Kilkenny",
	},
	// Germany
	"Germany": {
		"Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne",
		"Stuttgart", "Düsseldorf", "Leipzig", "Dresden", "Hannover",
		"Nuremberg", "Freiburg", "Heidelberg", "Bonn", "Essen",
		"Dortmund", "Bremen", "Augsburg", "Potsdam",
	},
	// Austria
	"Austria": {
		"Vienna", "Salzburg", "Innsbruck", "Graz", "Linz",
	},
	// Switzerland
	"Switzerland": {
		"Zurich", "Geneva", "Basel", "Bern", "Lausanne", "Lucerne",
	},
	// France
	"France": {
		"Paris", "Lyon", "Marseille", "Toulouse", "Nice",
		"Bordeaux", "Strasbourg", "Nantes", "Montpellier", "Lille",
		"Aix-en-Provence", "Cannes", "Biarritz", "Annecy",
	},
	// Spain
	"Spain": {
		"Madrid", "Barcelona", "Valencia", "Seville", "Malaga",
		"Bilbao", "Ibiza", "Palma de Mallorca", "Granada", "Alicante",
		"San Sebastian", "Marbella", "Tenerife", "Las Palmas", "Tarragona",
	},
	// Netherlands
	"Netherlands": {
		"Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven",
		"Groningen", "Haarlem", "Leiden", "Delft", "Maastricht",
	},
	// Belgium
	"Belgium": {
		"Brussels", "Antwerp", "Ghent", "Bruges", "Leuven",
	},
	// Italy
	"Italy": {
		"Rome", "Milan", "Florence", "Naples", "Turin",
		"Venice", "Bologna", "Genoa", "Verona", "Palermo",
		"Catania", "Bari", "Bergamo", "Padua", "Trieste",
	},
	// Portugal
	"Portugal": {
		"Lisbon", "Porto", "Faro", "Braga", "Coimbra",
		"Cascais", "Lagos", "Ericeira", "Funchal", "Sintra",
	},
	// Scandinavia
	"Sweden": {
		"Stockholm", "Gothenburg", "Malmö", "Uppsala", "Lund",
	},
	"Denmark": {
		"Copenhagen", "Aarhus", "Odense",
	},
	"Norway": {
		"Oslo", "Bergen", "Trondheim", "Stavanger",
	},
	"Finland": {
		"Helsinki", "Tampere", "Turku",
	},
	// Eastern Europe
	"Poland": {
		"Warsaw", "Krakow", "Wroclaw", "Gdansk", "Poznan", "Lodz",
	},
	"Czech Republic": {
		"Prague", "Brno", "Ostrava",
	},
	"Hungary": {
		"Budapest", "Debrecen",
	},
	"Romania": {
		"Bucharest", "Cluj-Napoca", "Timisoara", "Brasov",
	},
	"Croatia": {
		"Zagreb", "Split", "Dubrovnik",
	},
	"Greece": {
		"Athens", "Thessaloniki", "Crete", "Mykonos", "Santorini",
	},
	"Turkey": {
		"Istanbul", "Ankara", "Izmir", "Antalya", "Bodrum",
	},
	// Southeast Asia
	"Thailand": {
		"Bangkok", "Chiang Mai", "Phuket", "Koh Samui", "Pattaya",
		"Krabi", "Hua Hin", "Pai", "Koh Phangan", "Koh Lanta",
		"Chiang Rai", "Kanchanaburi",
	},
	"Vietnam": {
		"Ho Chi Minh City", "Hanoi", "Da Nang", "Hoi An", "Nha Trang",
		"Phu Quoc", "Dalat", "Hue",
	},
	"Philippines": {
		"Manila", "Cebu", "Davao", "Makati", "Quezon City",
		"Siargao", "Palawan", "Boracay", "Tagaytay", "Baguio",
	},
	"Malaysia": {
		"Kuala Lumpur", "Penang", "Johor Bahru", "Kota Kinabalu",
		"Langkawi", "Malacca", "Ipoh", "Kuching",
	},
	"Singapore": {
		"Singapore",
	},
	"Cambodia": {
		"Phnom Penh", "Siem Reap", "Kampot",
	},
	"Myanmar": {
		"Yangon", "Mandalay",
	},
	// South Asia
	"India": {
		"Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
		"Kolkata", "Pune", "Ahmedabad", "Jaipur", "Goa",
		"Rishikesh", "Mysore", "Kochi", "Thiruvananthapuram",
		"Chandigarh", "Lucknow", "Indore", "Noida", "Gurgaon",
	},
	"Sri Lanka": {
		"Colombo", "Kandy", "Galle", "Ella", "Negombo",
	},
	"Nepal": {
		"Kathmandu", "Pokhara",
	},
	// Middle East
	"UAE": {
		"Dubai", "Abu Dhabi", "Sharjah",
	},
	"Qatar": {
		"Doha",
	},
	"Saudi Arabia": {
		"Riyadh", "Jeddah", "Dammam",
	},
	"Bahrain": {
		"Manama",
	},
	// East Asia
	"Japan": {
		"Tokyo", "Osaka", "Kyoto", "Yokohama", "Fukuoka",
		"Sapporo", "Kobe", "Nagoya", "Okinawa",
	},
	"South Korea": {
		"Seoul", "Busan", "Incheon", "Daegu", "Jeju",
	},
	"Taiwan": {
		"Taipei", "Kaohsiung", "Taichung",
	},
	"Hong Kong": {
		"Hong Kong",
	},
	// Africa
	"South Africa": {
		"Cape Town", "Johannesburg", "Durban", "Pretoria",
	},
	"Kenya": {
		"Nairobi", "Mombasa",
	},
	"Nigeria": {
		"Lagos", "Abuja",
	},
	"Morocco": {
		"Marrakech", "Casablanca", "Rabat", "Fez",
	},
	"Egypt": {
		"Cairo", "Alexandria", "Hurghada", "Sharm El Sheikh",
	},
	// Latin America
	"Mexico": {
		"Mexico City", "Cancun", "Playa del Carmen", "Tulum",
		"Guadalajara", "Monterrey", "Puerto Vallarta", "Oaxaca",
		"San Miguel de Allende", "Merida",
	},
	"Brazil": {
		"Sao Paulo", "Rio de Janeiro", "Florianopolis", "Brasilia",
		"Salvador", "Curitiba", "Belo Horizonte", "Fortaleza",
	},
	"Colombia": {
		"Bogota", "Medellin", "Cartagena", "Cali", "Santa Marta",
	},
	"Argentina": {
		"Buenos Aires", "Mendoza", "Cordoba", "Bariloche",
	},
	"Chile": {
		"Santiago", "Valparaiso", "Vina del Mar",
	},
	"Peru": {
		"Lima", "Cusco", "Arequipa",
	},
	"Costa Rica": {
		"San Jose", "Tamarindo", "Santa Teresa", "Nosara",
	},
	"Panama": {
		"Panama City",
	},
}

// GenerateKeywords produces all {niche} x {city} x {template} combinations.
// Returns a deduplicated, lowercased keyword list.
func GenerateKeywords(niches, cities []string, templates []string) []string {
	if len(templates) == 0 {
		templates = WellnessTemplates
	}

	seen := make(map[string]bool)
	var keywords []string

	for _, niche := range niches {
		for _, city := range cities {
			for _, tmpl := range templates {
				kw := strings.ToLower(fmt.Sprintf(tmpl, niche, city))
				if !seen[kw] {
					seen[kw] = true
					keywords = append(keywords, kw)
				}
			}
		}
	}
	return keywords
}

// GenerateAllKeywords generates keywords for ALL cities in ALL countries.
// Combines business templates + personal templates for maximum coverage.
func GenerateAllKeywords() []string {
	var allCities []string
	for _, c := range Cities {
		allCities = append(allCities, c...)
	}
	// Business queries (yoga studio bali, etc).
	business := GenerateKeywords(Niches, allCities, WellnessTemplates)
	// Personal queries (yoga instructor bali @gmail.com, etc).
	personal := GenerateKeywords(PersonalNiches, allCities, PersonalTemplates)
	return dedupStrings(append(business, personal...))
}

// GenerateKeywordsForCountry generates keywords for a specific country.
func GenerateKeywordsForCountry(country string) []string {
	cities, ok := Cities[country]
	if !ok {
		return nil
	}
	business := GenerateKeywords(Niches, cities, WellnessTemplates)
	personal := GenerateKeywords(PersonalNiches, cities, PersonalTemplates)
	return dedupStrings(append(business, personal...))
}

// dedupStrings removes duplicate strings (case-insensitive).
func dedupStrings(items []string) []string {
	seen := make(map[string]bool, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		key := strings.ToLower(item)
		if !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}
	return result
}

// CountryList returns all available country names.
func CountryList() []string {
	countries := make([]string, 0, len(Cities))
	for c := range Cities {
		countries = append(countries, c)
	}
	return countries
}
