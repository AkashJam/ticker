package edgar

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Filing is one entry of a filer's recent filings.
type Filing struct {
	Form        string    // "6-K", "8-K", ...
	Accession   string    // 0001193125-26-123456
	Filed       time.Time // the filing date, midnight UTC
	Accepted    time.Time // when EDGAR accepted it, UTC: places it before or after the close
	Items       []string  // 8-K item codes ("2.02"); empty for a 6-K
	Description string    // primaryDocDescription: "FORM 6-K" for every 6-K
	Document    string    // the primary document's file name
	URL         string    // the document on sec.gov
}

// Recent returns the filer's recent filings, newest first, as EDGAR lists them.
func (c *Client) Recent(ctx context.Context, cik int) ([]Filing, error) {
	b, err := c.get(ctx, fmt.Sprintf("%s/CIK%010d.json", c.submissionsURL, cik))
	if err != nil {
		return nil, err
	}
	return parseRecent(cik, b)
}

func parseRecent(cik int, b []byte) ([]Filing, error) {
	var raw struct {
		Filings struct {
			Recent struct {
				Accession   []string `json:"accessionNumber"`
				FilingDate  []string `json:"filingDate"`
				Accepted    []string `json:"acceptanceDateTime"`
				Form        []string `json:"form"`
				Items       []string `json:"items"`
				Document    []string `json:"primaryDocument"`
				Description []string `json:"primaryDocDescription"`
			} `json:"recent"`
		} `json:"filings"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("edgar: parse filings for CIK %d: %w", cik, err)
	}
	r := raw.Filings.Recent
	n := len(r.Form)
	if len(r.Accession) != n || len(r.FilingDate) != n || len(r.Accepted) != n || len(r.Items) != n || len(r.Document) != n || len(r.Description) != n {
		return nil, fmt.Errorf("edgar: filings for CIK %d have columns of different lengths", cik)
	}
	out := make([]Filing, 0, n)
	for i := range n {
		filed, err := time.Parse(time.DateOnly, r.FilingDate[i])
		if err != nil {
			continue
		}
		accepted, err := time.Parse("2006-01-02T15:04:05.000Z", r.Accepted[i])
		if err != nil {
			continue
		}
		f := Filing{
			Form: r.Form[i], Accession: r.Accession[i], Filed: filed, Accepted: accepted.UTC(),
			Description: r.Description[i], Document: r.Document[i],
		}
		for _, code := range strings.Split(r.Items[i], ",") {
			if code = strings.TrimSpace(code); code != "" {
				f.Items = append(f.Items, code)
			}
		}
		f.URL = fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%d/%s/%s", cik, strings.ReplaceAll(f.Accession, "-", ""), f.Document)
		out = append(out, f)
	}
	return out, nil
}

// Select keeps the filings of the given forms (an amendment, "6-K/A", counts as
// its form) filed from the day before through the day after, inclusive, oldest
// first. from and to are dates; the time of day is ignored.
func Select(filings []Filing, forms []string, from, to time.Time) []Filing {
	from, to = from.UTC().Truncate(24*time.Hour), to.UTC().Truncate(24*time.Hour)
	var out []Filing
	for _, f := range filings {
		base := strings.TrimSuffix(f.Form, "/A")
		if !slices.Contains(forms, base) || f.Filed.Before(from) || f.Filed.After(to) {
			continue
		}
		out = append(out, f)
	}
	slices.SortFunc(out, func(a, b Filing) int { return a.Accepted.Compare(b.Accepted) })
	return out
}

// ItemTitle is the SEC's title for an 8-K item code, from Form 8-K's General
// Instructions. A code it does not know comes back empty.
func ItemTitle(code string) string { return itemTitles[code] }

var itemTitles = map[string]string{
	"1.01": "Entry into a Material Definitive Agreement",
	"1.02": "Termination of a Material Definitive Agreement",
	"1.03": "Bankruptcy or Receivership",
	"1.04": "Mine Safety – Reporting of Shutdowns and Patterns of Violations",
	"1.05": "Material Cybersecurity Incidents",
	"2.01": "Completion of Acquisition or Disposition of Assets",
	"2.02": "Results of Operations and Financial Condition",
	"2.03": "Creation of a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement",
	"2.04": "Triggering Events That Accelerate or Increase a Direct Financial Obligation",
	"2.05": "Costs Associated with Exit or Disposal Activities",
	"2.06": "Material Impairments",
	"3.01": "Notice of Delisting or Failure to Satisfy a Continued Listing Rule or Standard",
	"3.02": "Unregistered Sales of Equity Securities",
	"3.03": "Material Modification to Rights of Security Holders",
	"4.01": "Changes in Registrant's Certifying Accountant",
	"4.02": "Non-Reliance on Previously Issued Financial Statements or a Related Audit Report",
	"5.01": "Changes in Control of Registrant",
	"5.02": "Departure of Directors or Certain Officers; Election of Directors; Appointment of Certain Officers",
	"5.03": "Amendments to Articles of Incorporation or Bylaws; Change in Fiscal Year",
	"5.04": "Temporary Suspension of Trading Under Registrant's Employee Benefit Plans",
	"5.05": "Amendments to the Registrant's Code of Ethics, or Waiver of a Provision of the Code of Ethics",
	"5.06": "Change in Shell Company Status",
	"5.07": "Submission of Matters to a Vote of Security Holders",
	"5.08": "Shareholder Director Nominations",
	"6.01": "ABS Informational and Computational Material",
	"6.02": "Change of Servicer or Trustee",
	"6.03": "Change in Credit Enhancement or Other External Support",
	"6.04": "Failure to Make a Required Distribution",
	"6.05": "Securities Act Updating Disclosure",
	"6.06": "Static Pool",
	"7.01": "Regulation FD Disclosure",
	"8.01": "Other Events",
	"9.01": "Financial Statements and Exhibits",
}
