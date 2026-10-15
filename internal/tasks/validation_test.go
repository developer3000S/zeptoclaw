package tasks

import (
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// ТЗ 17.1 requires task validation to be unit-tested. These are the rules the
// mesh enforces at its two boundaries — Submit (own task) and OnTask (received
// one) — so every rejection below is one an operator will see in a log or an
// error message, and each must name its own reason rather than a generic one.

// validEnv builds an envelope that passes validation: Build stamps the content
// digest, so the baseline must be digested exactly as a real origin sends it.
func validEnv(t *testing.T, now time.Time) *pb.TaskEnvelope {
	t.Helper()
	env := Build("12D3KooWOrigin", NewID(), BuildRequest{
		Instruction:    "summarise the attached report",
		RequiredSkills: []string{"coding"},
		TTL:            5,
		Priority:       5,
	}, now)
	if err := Validate(env, 0, now); err != nil {
		t.Fatalf("baseline envelope rejected: %v", err)
	}
	return env
}

// mutate edits the authored fields and restamps the digest, so a failure can be
// attributed to the rule under test rather than to a stale digest. Fields
// outside the signed content (ttl, sender, route_stack) need no restamping.
func mutate(env *pb.TaskEnvelope, edit func(*pb.TaskEnvelope)) *pb.TaskEnvelope {
	edit(env)
	env.ContextDigest = ContentDigest(env)
	return env
}

func TestValidateRejectsEachRule(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name string
		env  func() *pb.TaskEnvelope
		want error
	}{
		{"empty task id", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) { e.TaskId = "" })
		}, ErrEmptyTaskID},
		{"missing origin", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) { e.OriginPeerId = "" })
		}, ErrMissingOrigin},
		{"missing sender", func() *pb.TaskEnvelope {
			e := validEnv(t, now)
			e.SenderPeerId = "" // not part of the signed content
			return e
		}, ErrMissingSender},
		{"whitespace instruction is no instruction", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) { e.Payload.Instruction = "   " })
		}, ErrNoInstruction},
		{"oversized instruction", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) {
				e.Payload.Instruction = strings.Repeat("a", MaxInstructionLen+1)
			})
		}, ErrPayloadTooLarge},
		{"negative ttl", func() *pb.TaskEnvelope {
			e := validEnv(t, now)
			e.Ttl = -1 // ttl travels in the clear, so no restamp
			return e
		}, ErrTTLExhausted},
		{"priority below range", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) { e.Priority = 0 })
		}, ErrBadPriority},
		{"priority above range", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) { e.Priority = 10 })
		}, ErrBadPriority},
		{"too many required skills", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) {
				for i := 0; i < MaxRequiredSkills+1; i++ {
					e.RequiredSkills = append(e.RequiredSkills, "skill"+string(rune('a'+i)))
				}
			})
		}, ErrTooManySkills},
		{"route stack longer than the bound", func() *pb.TaskEnvelope {
			e := validEnv(t, now)
			for i := 0; i < MaxRouteStack+1; i++ {
				e.RouteStack = append(e.RouteStack, "peer") // route fields are unsigned
			}
			return e
		}, ErrRouteTooLong},
		{"payload over the configured byte limit", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) {
				e.Payload.Instruction = strings.Repeat("b", 4096)
			})
		}, ErrPayloadTooLarge},
		{"created a day too long ago", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) {
				e.CreatedAt = now.Add(-MaxAge - time.Hour).Unix()
			})
		}, ErrStale},
		{"created far in the future", func() *pb.TaskEnvelope {
			return mutate(validEnv(t, now), func(e *pb.TaskEnvelope) {
				e.CreatedAt = now.Add(ClockSkewTolerance + time.Hour).Unix()
			})
		}, ErrTooFarAhead},
		{"digest recomputed over edited content", func() *pb.TaskEnvelope {
			e := validEnv(t, now)
			e.Payload.Instruction = "an entirely different instruction"
			e.ContextDigest = ContentDigest(e) // honest recompute: content really is this
			return e
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := tc.env()
			limit := int64(0)
			if tc.name == "payload over the configured byte limit" {
				limit = 512
			}
			err := Validate(env, limit, now)
			switch {
			case tc.want == nil && err != nil:
				t.Fatalf("unexpected rejection: %v", err)
			case tc.want == nil:
			case err == nil:
				t.Fatalf("accepted, want %v", tc.want)
			case !errors.Is(err, tc.want):
				t.Fatalf("err = %v, want it to report %v", err, tc.want)
			}
		})
	}
}

// A nil envelope is a programming error, not a rejected task: it has no
// violation to report, and no reason string to send back to a peer.
func TestValidateNilEnvelope(t *testing.T) {
	if err := Validate(nil, 0, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("nil envelope: %v", err)
	}
}

// A forged digest is the replay/tampering case the signatures exist for: the
// content must not be swappable behind a valid transport signature.
func TestValidateRejectsTamperedContent(t *testing.T) {
	now := time.Now().UTC()
	e := validEnv(t, now)
	e.Payload.Instruction = "delete the production database"
	if err := Validate(e, 0, now); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("edited instruction without a new digest: err = %v", err)
	}
	// Labels and constraints are inside the same claim, so each edit is caught.
	for _, edit := range []func(*pb.TaskEnvelope){
		func(x *pb.TaskEnvelope) { x.Payload.Labels = map[string]string{"x": "y"} },
		func(x *pb.TaskEnvelope) { x.Constraints.AllowShell = true },
		func(x *pb.TaskEnvelope) { x.Constraints.MaxDurationSeconds = 999 },
		func(x *pb.TaskEnvelope) { x.RequiredSkills = []string{"coding", "shell"} },
		func(x *pb.TaskEnvelope) { x.Priority = 9 },
	} {
		env := validEnv(t, now)
		edit(env)
		if err := Validate(env, 0, now); !errors.Is(err, ErrDigestMismatch) {
			t.Fatalf("content edit not covered by the digest: %v", err)
		}
	}
}

// A missing digest must be distinguishable from a wrong one: "the sender never
// signed the claim" and "the claim was edited" want different operator answers.
func TestVerifyDigestDistinguishesMissingFromWrong(t *testing.T) {
	now := time.Now().UTC()
	env := validEnv(t, now)
	if err := VerifyDigest(env); err != nil {
		t.Fatalf("digest of a built envelope: %v", err)
	}
	env.ContextDigest = ""
	err := VerifyDigest(env)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("missing digest: %v", err)
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing digest must say so: %v", err)
	}
	env.ContextDigest = "sha256:" + strings.Repeat("0", 64)
	if err := VerifyDigest(env); !errors.Is(err, ErrDigestMismatch) || strings.Contains(err.Error(), "missing") {
		t.Fatalf("wrong digest reported as missing: %v", err)
	}
}

// Validate reports every rule an envelope breaks, not just the first: an
// operator fixing a rejected task should not have to rediscover one violation
// per round trip.
func TestValidateReportsEveryViolation(t *testing.T) {
	now := time.Now().UTC()
	env := validEnv(t, now)
	env.TaskId = ""
	env.Priority = 42
	env.ContextDigest = ContentDigest(env)
	err := Validate(env, 0, now)
	if err == nil {
		t.Fatal("envelope with two violations accepted")
	}
	for _, want := range []error{ErrEmptyTaskID, ErrBadPriority} {
		if !errors.Is(err, want) {
			t.Fatalf("%v not reported: %v", want, err)
		}
	}
	if n := strings.Count(err.Error(), "task: "); n < 2 {
		t.Fatalf("reasons are not all joined: %v", err)
	}
}

// The limits are the mesh's own budget, not the sender's: route_stack and ttl
// are rewritten by relays, so they are checked separately from the signature.
func TestCheckRouteStopsLoopsAndSpentTTL(t *testing.T) {
	self := "12D3KooWSelf"
	other := "12D3KooWOther"

	env := &pb.TaskEnvelope{Ttl: 3, RouteStack: []string{other}}
	if err := CheckRoute(env, self); err != nil {
		t.Fatalf("clean route refused: %v", err)
	}
	// A task that already visited us must not be forwarded back to us.
	env.RouteStack = []string{other, self}
	if err := CheckRoute(env, self); !errors.Is(err, ErrRouteLoop) {
		t.Fatalf("loop not detected: %v", err)
	}
	// TTL is decremented per hop: 0 means it may not travel further.
	for _, ttl := range []int32{0, -1} {
		e := &pb.TaskEnvelope{Ttl: ttl, RouteStack: []string{other}}
		if err := CheckRoute(e, self); !errors.Is(err, ErrTTLExhausted) {
			t.Fatalf("ttl %d accepted: %v", ttl, err)
		}
	}
}

// AppendRoute is the loop guard's bookkeeping: it records hops, skips an empty
// id, and does not duplicate the peer that just forwarded the task.
func TestAppendRouteRecordsEachHopOnce(t *testing.T) {
	env := &pb.TaskEnvelope{Ttl: 5}
	AppendRoute(env, "")
	if len(env.GetRouteStack()) != 0 {
		t.Fatalf("empty peer id recorded: %v", env.GetRouteStack())
	}
	AppendRoute(env, "a")
	AppendRoute(env, "a")
	if got := strings.Join(env.GetRouteStack(), ","); got != "a" {
		t.Fatalf("re-adding the tail duplicated it: %v", got)
	}
	// A legitimate revisit of an earlier hop is still recorded: CheckRoute is what
	// decides whether that is a loop, AppendRoute only reports the path taken.
	AppendRoute(env, "b")
	AppendRoute(env, "a")
	if got := strings.Join(env.GetRouteStack(), ","); got != "a,b,a" {
		t.Fatalf("route = %v, want a,b,a", got)
	}
	if err := CheckRoute(env, "a"); !errors.Is(err, ErrRouteLoop) {
		t.Fatalf("CheckRoute must see the repeat: %v", err)
	}
}

// Build is the origin's own first validator: it must not hand the node an
// envelope its own Validate would reject.
func TestBuildProducesValidEnvelope(t *testing.T) {
	now := time.Now().UTC()
	env := Build("12D3KooWO", "", BuildRequest{Instruction: "work"}, now)
	if env.GetTaskId() == "" {
		t.Fatal("no task id generated")
	}
	if env.GetTtl() <= 0 {
		t.Fatalf("default ttl = %d, must be spendable", env.GetTtl())
	}
	if env.GetPriority() < 1 || env.GetPriority() > 9 {
		t.Fatalf("default priority = %d", env.GetPriority())
	}
	if env.GetConstraints().GetAllowDelegation() != true {
		t.Fatal("delegation must default to allowed")
	}
	if env.GetConstraints().GetAllowSubtasks() != false {
		t.Fatal("subtasks must default to disallowed")
	}
	if env.GetSenderPeerId() != env.GetOriginPeerId() {
		t.Fatal("a root task's sender is its origin")
	}
	if err := Validate(env, 0, now); err != nil {
		t.Fatalf("Build produced an invalid envelope: %v", err)
	}
	// Two builds of the same request differ only by id/time — never by digest
	// shape, which is what makes dedup and signatures dependable.
	one := Build("o", "fixed-id", BuildRequest{Instruction: "i", TTL: 2}, now)
	two := Build("o", "fixed-id", BuildRequest{Instruction: "i", TTL: 2}, now)
	if one.GetContextDigest() != two.GetContextDigest() {
		t.Fatalf("digest is not reproducible: %q vs %q", one.GetContextDigest(), two.GetContextDigest())
	}
}

// SkillsMatch is the routing predicate: an over-permissive version makes a node
// accept work it cannot do, an over-strict one starves the mesh.
func TestSkillsMatchCoversAndRefuses(t *testing.T) {
	cases := []struct {
		name       string
		have, want []string
		matched    int
		ok         bool
	}{
		{"general covers anything", []string{"general"}, []string{"ocr", "coding"}, 2, true},
		{"any is an alias of general", []string{"any"}, []string{"ocr"}, 1, true},
		{"no skills requested is satisfiable", []string{"coding"}, nil, 0, true},
		{"exact match", []string{"coding"}, []string{"coding"}, 1, true},
		{"case and spacing do not matter", []string{"  Coding "}, []string{"CODING"}, 1, true},
		{"partial coverage is not enough", []string{"coding", "ocr"}, []string{"ocr", "translate"}, 1, false},
		{"unrelated skills", []string{"coding"}, []string{"ocr"}, 0, false},
		{"general node with nothing required", []string{"general"}, []string{}, 0, true},
		{"nothing advertised, something asked", nil, []string{"ocr"}, 0, false},
		{"duplicates in want count twice", []string{"general"}, []string{"ocr", "ocr"}, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			matched, ok := SkillsMatch(tc.have, tc.want)
			if ok != tc.ok {
				t.Fatalf("SkillsMatch(%v, %v) ok = %v, want %v", tc.have, tc.want, ok, tc.ok)
			}
			if matched != tc.matched {
				t.Fatalf("SkillsMatch(%v, %v) matched = %d, want %d", tc.have, tc.want, matched, tc.matched)
			}
		})
	}
}

// A task must survive the wire round trip unchanged, or validation would reject
// envelopes the node itself authored.
func TestValidateAfterProtoRoundTrip(t *testing.T) {
	now := time.Now().UTC()
	env := validEnv(t, now)
	back := proto.Clone(env).(*pb.TaskEnvelope)
	if err := Validate(back, 0, now); err != nil {
		t.Fatalf("cloned envelope failed validation: %v", err)
	}
	if back.GetContextDigest() != env.GetContextDigest() {
		t.Fatal("digest changed across the wire")
	}
}
