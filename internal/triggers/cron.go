// Package triggers implements the fourth task source of ТЗ 6.6.1: a task can
// originate from a scheduled trigger on the node itself, not only from the
// admin API, a local UI or another peer.
//
// A trigger is a cron expression plus the task it injects. Triggers persist in
// the node's own key-value store, so a restart does not silently stop the work
// an operator scheduled days ago — and does not double-run it either: the last
// fire time is stored with the schedule.
//
// The cron dialect is deliberately the narrow, well-known subset (five fields:
// minute hour day-of-month month day-of-week) rather than a dependency on a
// third-party scheduler: everything the mesh needs is expressible there, and an
// expression the node cannot understand is rejected at creation time instead of
// quietly never firing.
package triggers

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrBadSchedule reports a cron expression this package cannot parse.
var ErrBadSchedule = errors.New("triggers: malformed schedule")

// Fields are the five cron positions, in order.
var Fields = []string{"minute", "hour", "day-of-month", "month", "day-of-week"}

// Schedule is a parsed cron expression: for each field, the set of allowed
// values. A nil set means "*" (every value in the field's range).
type Schedule struct {
	expr string

	minute     map[int]bool
	hour       map[int]bool
	dayOfMonth map[int]bool
	month      map[int]bool
	dayOfWeek  map[int]bool

	// dayOfMonthStar / dayOfWeekStar record whether a day field was written as
	// "*". Cron combines them specially (see Next), and the distinction is lost
	// once both sets are expanded, so it is kept explicitly.
	dayOfMonthStar bool
	dayOfWeekStar  bool
}

// ranges bound each field and give the natural-language error messages.
var ranges = []struct {
	lo, hi int
}{
	{0, 59}, // minute
	{0, 23}, // hour
	{1, 31}, // day of month
	{1, 12}, // month
	{0, 6},  // day of week (0 = Sunday)
}

// ParseSchedule reads a five-field cron expression.
//
// Supported per field: "*", a number, a range "a-b", a list "a,b,c", a step
// "*/n" or "a-b/n", and the names jan..dec / sun..sat. Ranges and lists may be
// combined ("1-5,20"), as in classic cron.
func ParseSchedule(expr string) (*Schedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != len(Fields) {
		return nil, fmt.Errorf("%w: %q has %d fields, want %d (%s)",
			ErrBadSchedule, expr, len(fields), len(Fields), strings.Join(Fields, " "))
	}
	s := &Schedule{expr: strings.Join(fields, " ")}
	targets := []*map[int]bool{&s.minute, &s.hour, &s.dayOfMonth, &s.month, &s.dayOfWeek}
	for i, f := range fields {
		set, star, err := parseField(f, i)
		if err != nil {
			// Name the whole expression too: an operator reads a schedule line as
			// a unit, and "day-of-week value 8" alone does not say which of the
			// stored triggers is broken.
			return nil, fmt.Errorf("%w (in %q)", err, expr)
		}
		*targets[i] = set
		switch i {
		case 2:
			s.dayOfMonthStar = star
		case 4:
			s.dayOfWeekStar = star
		}
	}
	return s, nil
}

// String returns the canonical (whitespace-normalised) expression.
func (s *Schedule) String() string {
	if s == nil {
		return ""
	}
	return s.expr
}

// parseField expands one cron field. star reports whether the field was "*".
func parseField(field string, idx int) (set map[int]bool, star bool, err error) {
	lo, hi := ranges[idx].lo, ranges[idx].hi
	set = make(map[int]bool, hi-lo+1)
	spec := field
	step := 1
	if slash := strings.Index(field, "/"); slash >= 0 {
		spec = field[:slash]
		d, cerr := strconv.Atoi(field[slash+1:])
		if cerr != nil || d <= 0 {
			return nil, false, fmt.Errorf("%w: %s %q has a bad step", ErrBadSchedule, Fields[idx], field)
		}
		step = d
		if spec == "" {
			return nil, false, fmt.Errorf("%w: %s %q is missing a range before '/'", ErrBadSchedule, Fields[idx], field)
		}
	}
	switch spec {
	case "*":
		star = true
		spec = strconv.Itoa(lo) + "-" + strconv.Itoa(hi)
	}
	for _, part := range strings.Split(spec, ",") {
		if part == "" {
			return nil, false, fmt.Errorf("%w: empty entry in %s %q", ErrBadSchedule, Fields[idx], field)
		}
		a, b, hasRange := part, part, false
		if dash := strings.Index(part, "-"); dash > 0 {
			a, b, hasRange = part[:dash], part[dash+1:], true
		}
		start, err := value(a, idx, lo, hi)
		if err != nil {
			return nil, false, err
		}
		end, err := value(b, idx, lo, hi)
		if err != nil {
			return nil, false, err
		}
		if hasRange {
			if start > end {
				return nil, false, fmt.Errorf("%w: %s range %q counts backwards", ErrBadSchedule, Fields[idx], part)
			}
		} else if step > 1 && !star {
			// "5/10" means "from 5 to the end of the field, every 10" in classic cron.
			end = hi
		}
		// A named day may exceed 6 ("7" is Sunday as well); map it back without
		// touching the loop counter, which would restart the step sequence.
		for v := start; v <= end; v += step {
			key := v
			if idx == 4 && key == 7 {
				key = 0
			}
			set[key] = true
		}
	}
	if len(set) == 0 {
		return nil, false, fmt.Errorf("%w: %s %q matches nothing", ErrBadSchedule, Fields[idx], field)
	}
	return set, star, nil
}

// value reads one numeric or named token of a field.
func value(tok string, idx, lo, hi int) (int, error) {
	if n, err := strconv.Atoi(tok); err == nil {
		if idx == 4 && n == 7 {
			return n, nil // Sunday; normalized when the set is built
		}
		if n < lo || n > hi {
			return 0, fmt.Errorf("%w: %s value %d is outside %d..%d", ErrBadSchedule, Fields[idx], n, lo, hi)
		}
		return n, nil
	}
	// Only the month and day-of-week fields have names; months are field 3.
	names := monthNames
	if idx == 4 {
		names = dayNames
	}
	if idx != 3 && idx != 4 {
		return 0, fmt.Errorf("%w: %s value %q is not a number", ErrBadSchedule, Fields[idx], tok)
	}
	if v, ok := names[strings.ToLower(tok)]; ok {
		return v, nil
	}
	return 0, fmt.Errorf("%w: %s value %q is neither a number nor a known name", ErrBadSchedule, Fields[idx], tok)
}

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dayNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// Matches reports whether t falls in the schedule, truncated to the minute.
func (s *Schedule) Matches(t time.Time) bool {
	if s == nil {
		return false
	}
	t = t.UTC()
	if !s.minute[t.Minute()] || !s.hour[t.Hour()] || !s.month[int(t.Month())] {
		return false
	}
	return s.dayMatches(t)
}

// dayMatches applies the classic cron rule: when both day fields are restricted,
// the day qualifies if *either* matches; when only one is restricted, that one
// decides. Operators write "0 9 * * 1-5" expecting weekdays only, and would get
// "weekdays or the 13th" from a naive AND.
func (s *Schedule) dayMatches(t time.Time) bool {
	dom := s.dayOfMonth[t.Day()]
	dow := s.dayOfWeek[int(t.Weekday())]
	switch {
	case s.dayOfMonthStar && s.dayOfWeekStar:
		return true
	case s.dayOfMonthStar:
		return dow
	case s.dayOfWeekStar:
		return dom
	default:
		return dom || dow
	}
}

// maxYear bounds the search in Next: a schedule that cannot fire within four
// years is not a schedule but a mistake (say, February 30).
const maxYears = 4

// Next returns the first instant strictly after t that the schedule matches,
// truncated to the minute. It reports ok=false when the expression can never
// fire, which callers must treat as an error rather than as "not yet".
func (s *Schedule) Next(t time.Time) (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	// Stepping minute by minute over four years is up to ~2.1M iterations; the
	// day-level skip below keeps the common cases far below that.
	cur := t.UTC().Truncate(time.Minute).Add(time.Minute)
	limit := cur.AddDate(maxYears, 0, 0)
	for cur.Before(limit) {
		if !s.month[int(cur.Month())] {
			cur = startOfMonth(cur).AddDate(0, 1, 0)
			continue
		}
		if !s.dayMatches(cur) {
			cur = startOfDay(cur).AddDate(0, 0, 1)
			continue
		}
		if !s.hour[cur.Hour()] {
			cur = startOfHour(cur).Add(time.Hour)
			continue
		}
		if !s.minute[cur.Minute()] {
			cur = cur.Add(time.Minute)
			continue
		}
		return cur, true
	}
	return time.Time{}, false
}

func startOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func startOfHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
}

func startOfMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}
