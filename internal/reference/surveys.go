package reference

import (
	"context"
	"fmt"
)

// SurveysName is the dataset the city survey records are stored and embedded as.
const SurveysName = "surveys"

// Housing tags: the plain-words assumption a city page states under the figure
// (research/city-surveys.md, "Housing on the city page"). A closed set, so a typo
// cannot invent a tag the page has no wording for.
const (
	HousingPrivateRent          = "private_rent"           // market rent, at the level the note names
	HousingWelfareCeiling       = "welfare_ceiling"        // paid at actual rent up to a ceiling
	HousingImputedRent          = "imputed_rent"           // owner-occupiers' imputed rent is counted
	HousingImputedRentExcluded  = "imputed_rent_excluded"  // the survey leaves imputed rent out
	HousingAveragedAllResidents = "averaged_all_residents" // averaged over owners and renters alike
	HousingDerivedRent          = "derived_rent"           // the survey omits housing; a rent is added
	HousingSurveyComponent      = "survey_component"       // a component of the budget, level not stated
	HousingIncludedNotSeparate  = "included_not_separate"  // inside the total, not shown on its own
	HousingNotInSurvey          = "not_in_survey"          // housing is not part of the survey
)

// HousingTags is the closed set.
var HousingTags = []string{
	HousingPrivateRent, HousingWelfareCeiling, HousingImputedRent, HousingImputedRentExcluded,
	HousingAveragedAllResidents, HousingDerivedRent, HousingSurveyComponent, HousingIncludedNotSeparate, HousingNotInSurvey,
}

// Survey kinds: what sort of figure a city's survey publishes, so the page can
// name it ("Poverty line") instead of calling every one "cost of living" (M22).
// A closed set, so a typo cannot invent a kind the page has no wording for.
const (
	KindPovertyLine           = "poverty_line"
	KindMinimumIncomeStandard = "minimum_income_standard"
	KindLivingWage            = "living_wage"
	KindBenefitOrBudget       = "benefit_or_reference_budget"
	KindAverageSpending       = "average_spending"
)

// KindInfo is a kind's heading and its one-line plain gloss.
type KindInfo struct {
	Label string
	Gloss string
}

// SurveyKinds is the closed set, keyed by kind.
var SurveyKinds = map[string]KindInfo{
	KindPovertyLine:           {"Poverty line", "The least a household needs to spend to avoid being counted as poor."},
	KindMinimumIncomeStandard: {"Minimum income standard", "A budget built from what members of the public say a household needs for a decent minimum."},
	KindLivingWage:            {"Living wage", "The pay one worker needs to cover a modest budget with no help from benefits."},
	KindBenefitOrBudget:       {"Benefit standard or reference budget", "What the state pays as a basic benefit, or prices a basket of essentials at."},
	KindAverageSpending:       {"Average spending", "What households there actually spend on average, which is not a minimum."},
}

// MeasureAverage marks a survey that reports average consumption.
const MeasureAverage = "average"

// SurveyHeadline is the survey's own total: the figure a city page leads with.
type SurveyHeadline struct {
	Value    float64 `json:"value"`    // a month, in Currency
	Currency string  `json:"currency"` // ISO 4217
	Year     int     `json:"year"`     // the survey's price year, which the carry-forward starts from
	Label    string  `json:"label"`    // who the figure is for, and where
	Method   string  `json:"method"`
	// Derived says how the figure was derived where the survey does not publish it
	// as stated (São Paulo's division, Oslo's added rent); empty otherwise.
	Derived string `json:"derived,omitempty"`
}

// SecondLine is a labelled figure beside the headline: "Excluding housing", "Before tax".
type SecondLine struct {
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

// SurveyHousing names the housing assumption.
type SurveyHousing struct {
	Tag  string `json:"tag"`
	Note string `json:"note"`
}

// SurveyPart is one slice of the split. Amount is a month, in the headline's
// currency; Share is a percentage of the headline.
type SurveyPart struct {
	Label           string  `json:"label"`
	Amount          float64 `json:"amount"`
	Share           float64 `json:"share"`
	Derived         string  `json:"derived,omitempty"` // how the amount was reached, where it is not published as stated
	Hatched         bool    `json:"hatched,omitempty"` // a ceiling, not a spend
	IncludesHousing bool    `json:"includesHousing,omitempty"`
}

// SurveySource is a primary source: where the figure was read, and when.
type SurveySource struct {
	Label     string `json:"label"`
	URL       string `json:"url"`
	Detail    string `json:"detail"`
	Retrieved string `json:"retrieved"`
}

// Survey is one city's cost-of-living record. Hand-maintained and committed
// (research/city-surveys.md is the working file); Verified is false while an item
// the owner must close is open, and an unverified city gets no page.
type Survey struct {
	Slug string `json:"slug"`
	// Kind is one of SurveyKinds: poverty line, minimum income standard, living wage,
	// benefit or reference budget, average spending.
	Kind       string   `json:"kind"`
	Verified   bool     `json:"verified"`
	OpenItems  []string `json:"openItems"`
	Instrument string   `json:"instrument"`
	Publisher  string   `json:"publisher"`
	// Measure is MeasureAverage where the figure is what residents actually spend
	// on average (Shanghai, Mumbai) and empty where it is a floor or a priced budget.
	// The affordability block words the two differently.
	Measure     string         `json:"measure,omitempty"`
	Headline    SurveyHeadline `json:"headline"`
	SecondLines []SecondLine   `json:"secondLines"`
	Housing     SurveyHousing  `json:"housing"`
	// Parts is nil, with SplitNote saying why, where the survey publishes a total
	// and not a split: no split, no bar.
	Parts       []SurveyPart   `json:"parts"`
	SplitNote   *string        `json:"splitNote"`
	Sources     []SurveySource `json:"sources"`
	NextRelease *string        `json:"nextRelease"`
}

// SurveyDataset is the blob stored as "surveys".
type SurveyDataset struct {
	GeneratedAt string   `json:"generatedAt"`
	Rows        []Survey `json:"rows"`
}

// Surveys returns the city survey records. The embedded file is the data: there
// is nothing to refresh it from, so it is committed and edited by hand.
func (s *Store) Surveys(ctx context.Context) ([]Survey, error) {
	var d SurveyDataset
	if _, err := s.Get(ctx, SurveysName, &d); err != nil {
		return nil, err
	}
	return d.Rows, nil
}

// Survey returns the record for a country slug.
func (s *Store) Survey(ctx context.Context, slug string) (Survey, bool, error) {
	rows, err := s.Surveys(ctx)
	if err != nil {
		return Survey{}, false, fmt.Errorf("reference: surveys: %w", err)
	}
	for _, r := range rows {
		if r.Slug == slug {
			return r, true, nil
		}
	}
	return Survey{}, false, nil
}
