package edgar

import "strings"

// EDGAR's country strings are not clean country names (portfolio.md §15
// Phase 15): a Canadian filer is "Ontario, Canada", a Korean one "Korea,
// Republic of", a Turkish one "Turkey", and a US domestic filer is a two-letter
// state code. No single split rule covers them, so this is a lookup. A string
// the table does not know is a drift finding, not a guess.

// countryISO maps EDGAR's stateOrCountryDescription for a non-US filer to an
// ISO 3166 alpha-2 code. Keys are lower-cased on lookup.
var countryISO = map[string]string{
	// The thirteen sensors' countries, as EDGAR spells them.
	"canada": "CA", "brazil": "BR", "mexico": "MX", "chile": "CL", "colombia": "CO",
	"peru": "PE", "united kingdom": "GB", "japan": "JP", "korea, republic of": "KR",
	"india": "IN", "indonesia": "ID", "turkey": "TR", "netherlands": "NL",
	// The other Atlas countries.
	"south africa": "ZA", "china": "CN", "poland": "PL", "italy": "IT", "france": "FR",
	"new zealand": "NZ", "israel": "IL", "germany": "DE", "ireland": "IE",
	"australia": "AU", "norway": "NO", "united states": "US",
	// Places filers are commonly registered in that are not a sensor's country.
	// BAP files in Bermuda and senses Peru (an override on its row).
	"bermuda": "BM", "cayman islands": "KY", "british virgin islands": "VG",
	"hong kong": "HK", "taiwan": "TW", "switzerland": "CH", "singapore": "SG",
	"luxembourg": "LU", "spain": "ES", "sweden": "SE", "denmark": "DK",
	"jersey": "JE", "guernsey": "GG", "panama": "PA", "argentina": "AR",
	"russian federation": "RU", "korea, democratic people's republic of": "KP",
	// Canadian provinces and territories: EDGAR prefixes the subdivision.
	"alberta, canada": "CA", "british columbia, canada": "CA", "manitoba, canada": "CA",
	"new brunswick, canada": "CA", "newfoundland and labrador, canada": "CA",
	"nova scotia, canada": "CA", "ontario, canada": "CA", "prince edward island, canada": "CA",
	"quebec, canada": "CA", "saskatchewan, canada": "CA", "northwest territories, canada": "CA",
	"nunavut, canada": "CA", "yukon, canada": "CA",
}

// usStates are the codes EDGAR uses for a domestic filer's state.
var usStates = map[string]bool{
	"AL": true, "AK": true, "AZ": true, "AR": true, "CA": true, "CO": true, "CT": true,
	"DE": true, "DC": true, "FL": true, "GA": true, "HI": true, "ID": true, "IL": true,
	"IN": true, "IA": true, "KS": true, "KY": true, "LA": true, "ME": true, "MD": true,
	"MA": true, "MI": true, "MN": true, "MS": true, "MO": true, "MT": true, "NE": true,
	"NV": true, "NH": true, "NJ": true, "NM": true, "NY": true, "NC": true, "ND": true,
	"OH": true, "OK": true, "OR": true, "PA": true, "RI": true, "SC": true, "SD": true,
	"TN": true, "TX": true, "UT": true, "VT": true, "VA": true, "WA": true, "WV": true,
	"WI": true, "WY": true, "PR": true, "GU": true, "VI": true,
}

// Normalise returns the ISO 3166 alpha-2 code for an EDGAR country string.
// ok is false for a string the table does not know.
func Normalise(desc string) (iso string, ok bool) {
	d := strings.TrimSpace(desc)
	if d == "" {
		return "", false
	}
	if usStates[d] {
		return "US", true
	}
	iso, ok = countryISO[strings.ToLower(d)]
	return iso, ok
}
