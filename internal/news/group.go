package news

import (
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode"
)

// Grouping follows bearing.md #91 and #111. Every outlet is shown without
// ranking: items are grouped by outlet, at most two headlines each (the
// earliest two, which is not a relevance ranking), outlets earliest first, and
// each item is placed before or after the close by its own timestamp. Story
// clustering was rejected as an editorial judgement made in code. The machine-
// generated outlets of #111 are dropped before grouping and counted.

// Side says where an item falls against the close.
const (
	SideBefore = "before" // a possible explanation
	SideAfter  = "after"  // a reaction
)

// MaxPerOutlet is how many headlines an outlet contributes.
const MaxPerOutlet = 2

// minLatin is the share of an English title's letters that must be Latin
// script. Google's US edition returns almost only English; this catches the
// stray Cyrillic, CJK or Arabic headline. It does not catch Spanish or
// Portuguese, which are Latin script (phase15.md step 6, decision 4).
const minLatin = 0.8

// Entry is one excluded outlet in the committed list (reference "outlets").
type Entry struct {
	Name  string   `json:"name"`
	Hosts []string `json:"hosts"`
}

// ExcludedList is the reference dataset of machine-generated outlets.
type ExcludedList struct {
	Source   string  `json:"source"`
	AsOf     string  `json:"asOf"`
	Criteria string  `json:"criterion"`
	Excluded []Entry `json:"excluded"`
}

// Excludes reports whether an item's outlet is on the list. The source's host
// is matched first (the name varies by edition and punctuation); the name is the
// fallback for an item whose <source> has no url.
func (l ExcludedList) Excludes(it Item) bool {
	host := hostOf(it.OutletURL)
	name := strings.ToLower(strings.TrimSpace(it.Outlet))
	for _, e := range l.Excluded {
		for _, h := range e.Hosts {
			if host != "" && (host == h || strings.HasSuffix(host, "."+h)) {
				return true
			}
		}
		if host == "" && name != "" && name == strings.ToLower(e.Name) {
			return true
		}
	}
	return false
}

func hostOf(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
}

// GroupedItem is one headline in a group.
type GroupedItem struct {
	Title     string    `json:"title"`
	Link      string    `json:"link"`
	Published time.Time `json:"publishedAt"`
	Side      string    `json:"side"`
}

// Outlet is one outlet's headlines.
type Outlet struct {
	Name  string        `json:"name"`
	URL   string        `json:"url,omitempty"`
	Items []GroupedItem `json:"items"`
}

// Result is a query's items, grouped, with what was left out counted.
type Result struct {
	Outlets    []Outlet `json:"outlets"`
	Total      int      `json:"total"`      // items the query returned
	Excluded   int      `json:"excluded"`   // from the machine-generated outlets
	NonEnglish int      `json:"nonEnglish"` // titles that are mostly not Latin script
	Undated    int      `json:"undated"`    // items with no usable timestamp, which cannot be placed
}

// Group groups items. closedAt is the session's close: an item published before
// it is a possible explanation, after it a reaction.
func Group(items []Item, closedAt time.Time, excluded ExcludedList) Result {
	res := Result{Total: len(items), Outlets: []Outlet{}}
	byOutlet := map[string][]GroupedItem{}
	urls := map[string]string{}
	for _, it := range items {
		switch {
		case excluded.Excludes(it):
			res.Excluded++
			continue
		case !latinEnough(it.Title):
			res.NonEnglish++
			continue
		case it.Published.IsZero():
			res.Undated++
			continue
		}
		name := strings.TrimSpace(it.Outlet)
		if name == "" {
			name = hostOf(it.OutletURL)
		}
		if name == "" {
			name = "Unknown outlet"
		}
		side := SideBefore
		if !it.Published.Before(closedAt) {
			side = SideAfter
		}
		byOutlet[name] = append(byOutlet[name], GroupedItem{Title: it.Title, Link: it.Link, Published: it.Published.UTC(), Side: side})
		if urls[name] == "" {
			urls[name] = it.OutletURL
		}
	}
	for name, gi := range byOutlet {
		slices.SortStableFunc(gi, func(a, b GroupedItem) int { return a.Published.Compare(b.Published) })
		if len(gi) > MaxPerOutlet {
			gi = gi[:MaxPerOutlet]
		}
		res.Outlets = append(res.Outlets, Outlet{Name: name, URL: urls[name], Items: gi})
	}
	// Outlets earliest first. A name breaks a tie so the order is stable.
	slices.SortFunc(res.Outlets, func(a, b Outlet) int {
		if c := a.Items[0].Published.Compare(b.Items[0].Published); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	return res
}

// latinEnough reports whether at least minLatin of the title's letters are Latin
// script. A title with no letters passes: there is nothing to judge.
func latinEnough(title string) bool {
	var letters, latin int
	for _, r := range title {
		if !unicode.IsLetter(r) {
			continue
		}
		letters++
		if unicode.Is(unicode.Latin, r) {
			latin++
		}
	}
	return letters == 0 || float64(latin)/float64(letters) >= minLatin
}
