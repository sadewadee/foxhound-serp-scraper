//go:build playwright

package query

import (
	"fmt"
	"strings"
)

// Niches — yoga/wellness/fitness core + on-target expansions.
var Niches = []string{
	// Yoga core
	"yoga", "yoga studio", "yoga class", "yoga school",
	"yoga center", "yoga shala", "yoga ashram",
	"yoga teacher training", "yoga ttc", "yoga retreat",
	"yogafx", "yoga academy", "online yoga",
	// Yoga styles
	"bikram yoga", "hot yoga", "vinyasa yoga",
	"ashtanga yoga", "yin yoga", "aerial yoga",
	"prenatal yoga", "postnatal yoga", "power yoga",
	"kundalini yoga", "iyengar yoga", "hatha yoga",
	"restorative yoga", "jivamukti yoga", "rocket yoga",
	"acro yoga", "rope yoga", "kids yoga", "chair yoga",
	// Pilates
	"pilates", "pilates studio", "pilates class",
	"reformer pilates", "mat pilates", "clinical pilates",
	"pilates teacher training",
	// Fitness core
	"gym", "fitness center", "fitness studio", "fitness club",
	"boutique gym", "crossfit", "crossfit box",
	"functional fitness", "bootcamp", "hiit class",
	"barre studio", "spin studio", "boxing gym",
	// Wellness / healing
	"wellness", "wellness center", "wellness studio",
	"wellness retreat", "wellness resort", "wellness clinic",
	"healing", "healing center", "healing retreat",
	"holistic wellness", "holistic healing", "holistic center",
	"mindfulness center", "mindfulness retreat",
	"meditation", "meditation center", "meditation retreat",
	"breathwork", "breathwork studio", "sound healing",
	"sound bath", "reiki healing", "energy healing",
	"ayurveda", "ayurvedic center", "ayurvedic retreat",
	// Spa / body wellness
	"day spa", "wellness spa", "health spa", "thermal spa",
	"thai massage", "therapeutic massage",
}

// PersonalNiches target individual practitioners (more likely to have gmail/yahoo).
// Tight focus on yoga/wellness/fitness — no beauty/hair/nail crossover.
var PersonalNiches = []string{
	// Yoga
	"yoga instructor", "yoga teacher", "yoga therapist",
	"yoga trainer", "yoga coach", "yoga guru",
	"prenatal yoga teacher", "postnatal yoga teacher",
	"kids yoga teacher", "aerial yoga instructor",
	"hot yoga instructor", "vinyasa yoga teacher",
	"ashtanga yoga teacher", "kundalini yoga teacher",
	"iyengar yoga teacher",
	// Pilates
	"pilates instructor", "pilates teacher",
	"reformer pilates instructor", "clinical pilates instructor",
	// Fitness
	"personal trainer", "fitness coach", "fitness instructor",
	"fitness trainer", "online personal trainer",
	"crossfit coach", "strength coach", "conditioning coach",
	"functional trainer", "bootcamp instructor",
	"hiit instructor", "barre instructor", "spin instructor",
	"boxing trainer",
	// Wellness
	"wellness coach", "wellness practitioner", "wellness consultant",
	"wellness therapist", "health coach", "holistic health coach",
	"holistic wellness coach", "holistic practitioner",
	// Mind
	"meditation teacher", "meditation guide", "meditation instructor",
	"mindfulness teacher", "mindfulness coach",
	"breathwork facilitator", "breathwork coach",
	// Body / healing
	"reiki practitioner", "reiki healer", "reiki master",
	"energy healer", "energy worker", "crystal healer",
	"sound healer", "sound therapist",
	// Traditional
	"ayurvedic practitioner", "ayurvedic therapist",
	"ayurvedic doctor", "ayurvedic consultant",
}

// WellnessTemplates — "<keyword> <city> <operator>" variants.
var WellnessTemplates = []string{
	"%s %s contact",
	"%s %s email",
	"%s %s \"@gmail.com\"",
	"%s %s \"@yahoo.com\"",
	"%s %s \"@hotmail.com\"",
	"%s %s \"@outlook.com\"",
	"%s %s \"@icloud.com\"",
	"%s %s \"@aol.com\"",
	"%s %s \"@protonmail.com\"",
}

// PersonalTemplates — same operator set as business templates.
var PersonalTemplates = []string{
	"%s %s contact",
	"%s %s email",
	"%s %s \"@gmail.com\"",
	"%s %s \"@yahoo.com\"",
	"%s %s \"@hotmail.com\"",
	"%s %s \"@outlook.com\"",
	"%s %s \"@icloud.com\"",
	"%s %s \"@aol.com\"",
	"%s %s \"@protonmail.com\"",
}

// Cities organized by country — includes neighborhoods for major metros.
var Cities = map[string][]string{
	// Indonesia
	"Indonesia": {
		"Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
		"Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
		"Tangerang Selatan", "Bogor", "Serang", "Cilegon",
		// Bali — high wellness tourism
		"Denpasar", "Bali", "Ubud", "Canggu", "Seminyak",
		"Sanur", "Kuta", "Nusa Dua", "Uluwatu", "Jimbaran",
		"Pererenan", "Berawa", "Kerobokan", "Umalas", "Sidemen",
		"Amed", "Lovina", "Munduk", "Tabanan", "Gianyar",
		// Java
		"Yogyakarta", "Malang", "Solo", "Surakarta", "Cirebon",
		"Purwokerto", "Kediri", "Madiun", "Probolinggo",
		// Other islands
		"Balikpapan", "Manado", "Batam", "Samarinda", "Pontianak",
		"Banjarmasin", "Pekanbaru", "Padang", "Jambi", "Bengkulu",
		// Lombok / NTB
		"Lombok", "Senggigi", "Kuta Lombok", "Gili Trawangan",
		// Jakarta neighborhoods
		"Kemang", "Senopati", "Menteng", "Kelapa Gading", "PIK Jakarta",
		"Sudirman", "Kuningan", "Pondok Indah", "Cipete", "Tebet",
		"Cilandak", "Pasar Minggu", "Kebayoran", "SCBD",
		"BSD", "Alam Sutera", "Gading Serpong",
	},
	// United States — highest priority
	"United States": {
		// Top 30 metros
		"New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
		"San Antonio", "San Diego", "Dallas", "San Jose", "Austin",
		"Jacksonville", "Fort Worth", "Columbus", "Indianapolis",
		"Charlotte", "San Francisco", "Seattle", "Denver", "Washington DC",
		"Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City",
		"Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore",
		"Milwaukee", "Albuquerque", "Tucson", "Fresno", "Sacramento",
		"Kansas City", "Mesa", "Atlanta", "Omaha", "Colorado Springs",
		"Miami", "Raleigh", "Long Beach", "Virginia Beach", "Oakland",
		"Minneapolis", "Tulsa", "Arlington", "Tampa", "New Orleans",
		"Cleveland",
		// Wellness capitals
		"Scottsdale", "Boulder", "Santa Monica", "Sedona", "Asheville",
		"Austin", "Portland", "Boulder", "Park City", "Aspen",
		"Palm Springs", "Ojai", "Big Sur", "Carmel", "Napa",
		"Sonoma", "Jackson Hole", "Vail", "Taos", "Santa Fe",
		"Telluride", "Marfa", "Key West", "Martha's Vineyard",
		// Hawaii
		"Honolulu", "Maui", "Oahu", "Kauai", "Big Island",
		"Lahaina", "Kihei", "Wailea", "Paia", "Hanalei",
		// NYC neighborhoods
		"Brooklyn", "Manhattan", "Queens", "Bronx", "Williamsburg",
		"Park Slope", "Upper East Side", "Upper West Side", "SoHo",
		"Tribeca", "Harlem", "DUMBO", "Greenpoint", "Bushwick",
		"Long Island City", "Astoria", "Chelsea", "West Village",
		"East Village", "NoHo", "Gramercy", "Flatiron",
		// LA neighborhoods
		"Venice Beach", "Silver Lake", "West Hollywood",
		"Beverly Hills", "Pasadena", "Glendale", "Culver City",
		"Los Feliz", "Echo Park", "Highland Park", "Malibu",
		"Marina del Rey", "Manhattan Beach", "Hermosa Beach",
		"Redondo Beach", "Topanga", "Studio City", "Sherman Oaks",
		// Bay Area
		"Oakland", "Berkeley", "Palo Alto", "Mountain View", "Fremont",
		"San Mateo", "Redwood City", "Menlo Park", "Marin",
		"Mill Valley", "Sausalito", "Tiburon",
		// Florida
		"Tampa", "Orlando", "Fort Lauderdale", "West Palm Beach",
		"Boca Raton", "Delray Beach", "Sarasota", "Naples",
		"Fort Myers", "Destin", "St Petersburg", "Clearwater",
		// Texas
		"Plano", "Frisco", "Irving", "McKinney", "Round Rock",
		"Sugar Land", "The Woodlands", "Katy",
		// Arizona / West
		"Mesa", "Tempe", "Chandler", "Gilbert", "Flagstaff",
		// South
		"Durham", "Chapel Hill", "Richmond", "Charleston",
		"Savannah", "Tallahassee",
		// Mountain
		"Salt Lake City", "Boise", "Missoula", "Bozeman",
		// NE / MidAtlantic
		"Pittsburgh", "Philadelphia", "Hartford", "Providence",
		"Princeton", "Hoboken", "Jersey City", "Stamford",
		"New Haven", "Arlington VA", "Alexandria",
	},
	// Canada
	"Canada": {
		"Toronto", "Vancouver", "Montreal", "Calgary", "Edmonton",
		"Ottawa", "Winnipeg", "Quebec City", "Halifax", "Victoria",
		"Whistler", "Kelowna", "Saskatoon", "Regina",
		"Mississauga", "Brampton", "Hamilton", "London Ontario",
		"Surrey BC", "Burnaby", "Richmond BC", "Langley",
		"Kitchener", "Waterloo", "Oakville", "Burlington",
		"Markham", "Vaughan", "Richmond Hill", "Oshawa",
		"Gatineau", "Sherbrooke", "Trois-Rivieres", "Laval",
		"North Vancouver", "West Vancouver", "Squamish",
		"Banff", "Canmore", "Jasper", "Tofino", "Ucluelet",
		"Niagara Falls", "Kingston", "Guelph",
	},
	// Australia — high priority wellness market
	"Australia": {
		// Major cities
		"Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
		"Gold Coast", "Canberra", "Hobart", "Darwin", "Newcastle",
		// Wellness hotspots
		"Byron Bay", "Noosa", "Mornington", "Port Macquarie",
		"Margaret River", "Yamba", "Bellingen", "Kiama",
		// Sydney suburbs
		"Bondi", "Bondi Junction", "Manly", "Surry Hills",
		"Paddington", "Newtown", "Mosman", "Double Bay",
		"Chatswood", "Parramatta", "Cronulla", "Coogee",
		// Melbourne suburbs
		"Fitzroy", "Collingwood", "Brunswick", "Richmond",
		"St Kilda", "South Yarra", "Carlton", "Prahran",
		"Hawthorn", "Toorak", "Elwood", "Windsor",
		// Brisbane / QLD
		"New Farm", "Fortitude Valley", "West End", "Teneriffe",
		"Cairns", "Townsville", "Port Douglas", "Airlie Beach",
		"Sunshine Coast", "Maroochydore", "Caloundra", "Mooloolaba",
		// WA
		"Fremantle", "Cottesloe", "Subiaco", "Scarborough",
		// Other
		"Wollongong", "Geelong", "Bendigo", "Ballarat",
		"Launceston", "Toowoomba",
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
		"Newcastle", "Plymouth", "Portsmouth", "Aberdeen", "Dundee",
		"Inverness", "Stirling", "Swansea", "Newport", "Derby",
		"Leicester", "Coventry", "Milton Keynes", "Northampton",
		"Ipswich", "Norwich", "Canterbury", "Bournemouth",
		"Cheltenham", "Harrogate", "Chester", "Durham", "Lancaster",
		"St Albans", "Guildford", "Tunbridge Wells", "Winchester",
		// London neighborhoods
		"Shoreditch", "Camden", "Hackney", "Notting Hill", "Chelsea",
		"Islington", "Fulham", "Brixton", "Clapham", "Richmond",
		"Kensington", "Mayfair", "Marylebone", "Fitzrovia",
		"Soho", "Covent Garden", "Bloomsbury", "Greenwich",
		"Hampstead", "Highgate", "Stoke Newington", "Dalston",
		"Peckham", "Dulwich", "Wimbledon", "Putney", "Wandsworth",
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
		"Münster", "Karlsruhe", "Mannheim", "Wiesbaden", "Mainz",
		"Aachen", "Kiel", "Lübeck", "Regensburg", "Tübingen",
		"Weimar", "Erfurt", "Jena", "Konstanz", "Rostock",
		"Kreuzberg", "Mitte", "Prenzlauer Berg", "Friedrichshain",
		"Neukölln", "Charlottenburg", "Schwabing",
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
		"Rennes", "Reims", "Le Havre", "Saint-Étienne",
		"Toulon", "Grenoble", "Dijon", "Angers", "Nîmes",
		"Villeurbanne", "Tours", "Clermont-Ferrand", "Orléans",
		"Caen", "Rouen", "Avignon", "Saint-Tropez", "Monaco",
		"Le Marais", "Montmartre", "Saint-Germain", "Bastille",
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
	// Southeast Asia — big wellness tourism markets
	"Thailand": {
		"Bangkok", "Chiang Mai", "Phuket", "Koh Samui", "Pattaya",
		"Krabi", "Hua Hin", "Pai", "Koh Phangan", "Koh Lanta",
		"Chiang Rai", "Kanchanaburi", "Koh Tao", "Ao Nang",
		"Rawai", "Kata", "Karon", "Kamala", "Patong",
		"Sukhumvit", "Thonglor", "Ekkamai", "Sathorn", "Silom",
		"Ari", "Nimman", "Old City Chiang Mai",
	},
	"Vietnam": {
		"Ho Chi Minh City", "Hanoi", "Da Nang", "Hoi An", "Nha Trang",
		"Phu Quoc", "Dalat", "Hue", "Mui Ne", "Can Tho",
		"Vung Tau", "Sapa", "Ha Long",
		"District 1", "District 2", "District 7", "Thao Dien",
	},
	"Philippines": {
		"Manila", "Cebu", "Davao", "Makati", "Quezon City",
		"Siargao", "Palawan", "Boracay", "Tagaytay", "Baguio",
		"Bohol", "Coron", "El Nido", "Dumaguete", "Iloilo",
		"BGC", "Taguig", "Alabang", "Ortigas",
	},
	"Malaysia": {
		"Kuala Lumpur", "Penang", "Johor Bahru", "Kota Kinabalu",
		"Langkawi", "Malacca", "Ipoh", "Kuching",
		"Shah Alam", "Petaling Jaya", "Subang Jaya", "Cyberjaya",
		"George Town", "Kota Damansara", "Mont Kiara", "Bangsar",
	},
	"Singapore": {
		"Singapore", "Orchard", "Tanjong Pagar", "Bugis",
		"Holland Village", "Tiong Bahru", "Katong", "Joo Chiat",
		"Sentosa", "Jurong", "Woodlands",
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
