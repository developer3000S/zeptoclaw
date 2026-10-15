package triggers

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func mustParse(t *testing.T, expr string) *Schedule {
	t.Helper()
	s, err := ParseSchedule(expr)
	if err != nil {
		t.Fatalf("ParseSchedule(%q): %v", expr, err)
	}
	return s
}

// at builds the UTC instant the cron tests reason about.
func at(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
}

func TestParseAcceptsTheCommonShapes(t *testing.T) {
	cases := []struct {
		expr string
		// probe minutes to check, with the expected match
		matchAt []time.Time
		skipAt  []time.Time
	}{
		{
			expr:    "* * * * *",
			matchAt: []time.Time{at(2026, 1, 1, 0, 0), at(2026, 12, 31, 23, 59)},
		},
		{
			expr:    "0 9 * * *",
			matchAt: []time.Time{at(2026, 3, 5, 9, 0)},
			skipAt:  []time.Time{at(2026, 3, 5, 9, 1), at(2026, 3, 5, 8, 0)},
		},
		{
			// Step over a list: every 15th minute of 8 and 9.
			expr:    "*/15 8,9 * * *",
			matchAt: []time.Time{at(2026, 3, 5, 8, 0), at(2026, 3, 5, 9, 45)},
			skipAt:  []time.Time{at(2026, 3, 5, 7, 0), at(2026, 3, 5, 10, 0), at(2026, 3, 5, 8, 7)},
		},
		{
			expr:    "30 4 1 * *",
			matchAt: []time.Time{at(2026, 7, 1, 4, 30)},
			skipAt:  []time.Time{at(2026, 7, 2, 4, 30)},
		},
		{
			// Names are case-insensitive and accepted for month and weekday.
			expr:    "0 12 * JAN,JUL mon-fri",
			matchAt: []time.Time{at(2026, 1, 5, 12, 0)},                        // Monday
			skipAt:  []time.Time{at(2026, 2, 5, 12, 0), at(2026, 1, 3, 12, 0)}, // Feb; Saturday
		},
		{
			// Sunday as 0 and as 7: both names the same day.
			expr:    "0 0 * * 7",
			matchAt: []time.Time{at(2026, 1, 4, 0, 0)}, // a Sunday
			skipAt:  []time.Time{at(2026, 1, 5, 0, 0)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			s := mustParse(t, tc.expr)
			for _, when := range tc.matchAt {
				if !s.Matches(when) {
					t.Fatalf("%q must match %s", tc.expr, when.Format(time.RFC3339))
				}
			}
			for _, when := range tc.skipAt {
				if s.Matches(when) {
					t.Fatalf("%q must not match %s", tc.expr, when.Format(time.RFC3339))
				}
			}
		})
	}
}

// The two day fields combine the way operators read them, not the way a naive
// AND does: "0 9 * * 1-5" is weekdays only, even though day-of-month is also
// unrestricted. Restricting BOTH is the only case that ORs.
func TestDayFieldCombinationFollowsCron(t *testing.T) {
	// 2026-01-05 is a Monday; 2026-01-13 is a Tuesday.
	weekdays := mustParse(t, "0 9 * * 1-5")
	if !weekdays.Matches(at(2026, 1, 5, 9, 0)) {
		t.Fatal("weekday must match a Monday")
	}
	if !weekdays.Matches(at(2026, 1, 13, 9, 0)) {
		t.Fatal("weekday must match a Tuesday regardless of day-of-month")
	}
	if weekdays.Matches(at(2026, 1, 3, 9, 0)) { // Saturday
		t.Fatal("weekday must not match a Saturday")
	}

	// Both restricted: either qualifies (classic cron behaviour).
	either := mustParse(t, "0 9 13 * 1")
	if !either.Matches(at(2026, 1, 5, 9, 0)) { // Monday, not the 13th
		t.Fatal("day-of-week half of a restricted pair must still qualify")
	}
	if !either.Matches(at(2026, 1, 13, 9, 0)) { // the 13th, a Tuesday
		t.Fatal("day-of-month half of a restricted pair must still qualify")
	}
	if either.Matches(at(2026, 1, 14, 9, 0)) {
		t.Fatal("neither half matched, the expression must be false")
	}

	// Only day-of-month restricted (weekday is *): that one decides.
	dom := mustParse(t, "0 0 1 * *")
	if !dom.Matches(at(2026, 1, 1, 0, 0)) {
		t.Fatal("first of month must match")
	}
	if dom.Matches(at(2026, 1, 2, 0, 0)) {
		t.Fatal("second of month must not match")
	}
}

// Matches is documented to work per minute; a second hand must not decide.
func TestMatchesIgnoresSeconds(t *testing.T) {
	s := mustParse(t, "*/5 * * * *")
	withSeconds := at(2026, 1, 1, 10, 15).Add(37 * time.Second)
	if !s.Matches(withSeconds) {
		t.Fatalf("a match at :15 must hold at :15:37 (%v)", withSeconds)
	}
	if s.Matches(withSeconds.Add(45 * time.Second)) {
		t.Fatal("stepping into the next minute must re-evaluate")
	}
}

func TestNextFindsTheFollowingMinute(t *testing.T) {
	s := mustParse(t, "0 9 * * *")
	cases := []struct {
		from time.Time
		want time.Time
	}{
		{at(2026, 3, 5, 8, 59), at(2026, 3, 5, 9, 0)},
		{at(2026, 3, 5, 9, 0), at(2026, 3, 6, 9, 0)},    // strictly after
		{at(2026, 3, 5, 9, 1), at(2026, 3, 6, 9, 0)},    // the next day
		{at(2026, 12, 31, 23, 0), at(2027, 1, 1, 9, 0)}, // across the year
	}
	for _, tc := range cases {
		got, ok := s.Next(tc.from)
		if !ok {
			t.Fatalf("Next(%v) found nothing", tc.from)
		}
		if !got.Equal(tc.want) {
			t.Fatalf("Next(%v) = %v, want %v", tc.from, got, tc.want)
		}
	}
}

// A leap-day schedule must land on a real date and skip years without one.
func TestNextAcrossLeapDay(t *testing.T) {
	s := mustParse(t, "0 0 29 2 *")
	// 2026 is not a leap year: the next 29 February is 2028.
	got, ok := s.Next(at(2026, 3, 1, 0, 0))
	if !ok {
		t.Fatal("29 February never came within the search window")
	}
	if want := at(2028, 2, 29, 0, 0); !got.Equal(want) {
		t.Fatalf("next leap day = %v, want %v", got, want)
	}
}

// An impossible date must be reported as "can never fire", not as a hang or a
// silent schedule that never runs.
func TestNextRejectsImpossibleSchedules(t *testing.T) {
	s := mustParse(t, "0 0 30 2 *") // February 30
	if _, ok := s.Next(at(2026, 1, 1, 0, 0)); ok {
		t.Fatal("February 30 was reported as reachable")
	}
}

// A schedule that must search across a year boundary (and could otherwise walk
// out of range) still resolves.
func TestNextStaysInRange(t *testing.T) {
	// Fires once every four years on a leap day, late in December — worst case
	// for a minute-by-minute walk; the day-skip keeps it bounded.
	s := mustParse(t, "0 12 29 2 *")
	got, ok := s.Next(at(2026, 2, 28, 12, 0))
	if !ok || got.Year() != 2028 {
		t.Fatalf("got %v ok=%v, want a 2028 date", got, ok)
	}
}

func TestParseRejectsMalformedExpressions(t *testing.T) {
	bad := []struct {
		expr string
		why  string
	}{
		{"* * *", "too few fields"},
		{"* * * * * *", "too many fields"},
		{"60 * * * *", "minute out of range"},
		{"* 24 * * *", "hour out of range"},
		{"* * 0 * *", "day of month starts at 1"},
		{"* * * 13 *", "month out of range"},
		{"* * * * 8", "weekday out of range"},
		{"*/0 * * * *", "zero step"},
		{"5-1 * * * *", "reversed range"},
		{"a * * * *", "not a number"},
		{"* * * foo *", "unknown month name"},
		{"1,,2 * * * *", "empty list entry"},
		{"*/2/3 * * * *", "double step"},
		{"/5 * * * *", "step without a range"},
		{"* * * * mon-foo", "unknown day name"},
	}
	for _, tc := range bad {
		t.Run(tc.why, func(t *testing.T) {
			_, err := ParseSchedule(tc.expr)
			if err == nil {
				t.Fatalf("ParseSchedule(%q) accepted an invalid expression", tc.expr)
			}
			if !errors.Is(err, ErrBadSchedule) {
				t.Fatalf("error must wrap ErrBadSchedule, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.expr) && tc.why != "too many fields" {
				t.Fatalf("error must name the expression: %v", err)
			}
		})
	}
}

// Field names in the error tell the operator which column of the five is wrong.
func TestParseErrorsNameTheField(t *testing.T) {
	cases := map[string]string{
		"60 * * * *": "minute",
		"* 24 * * *": "hour",
		"* * 32 * *": "day-of-month",
		"* * * 13 *": "month",
		"* * * * 8":  "day-of-week",
	}
	for expr, want := range cases {
		_, err := ParseSchedule(expr)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("ParseSchedule(%q) = %v, want it to mention %q", expr, err, want)
		}
	}
}

// Step on a bare number means "from N to the end of the field, every n"
// (classic V7 cron), not "only N".
func TestStepFromSingleValueRunsToEndOfField(t *testing.T) {
	s := mustParse(t, "5/20 * * * *")
	for _, mi := range []int{5, 25, 45} {
		if !s.Matches(at(2026, 1, 1, 0, mi)) {
			t.Fatalf("minute %d must match 5/20", mi)
		}
	}
	if s.Matches(at(2026, 1, 1, 0, 6)) {
		t.Fatal("minute 6 must not match a step of 20 from 5")
	}
}

// String reports the canonical form, which is what gets stored and shown.
func TestScheduleStringNormalisesWhitespace(t *testing.T) {
	s := mustParse(t, "  0   9  *  *  * ")
	if got := s.String(); got != "0 9 * * *" {
		t.Fatalf("String() = %q", got)
	}
}
