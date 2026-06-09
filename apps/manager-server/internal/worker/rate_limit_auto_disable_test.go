package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	collectorpkg "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestQuotaAutoDisableCandidateRequiresQuotaResetAndFile(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	event := usage.Event{
		EventHash:        "evt-1",
		Failed:           true,
		FailStatusCode:   http.StatusTooManyRequests,
		FailBody:         `{"error":{"code":"quota_exhausted","message":"quota exhausted","reset_at":1700000360}}`,
		AuthFileSnapshot: "codex-auth.json",
		AccountSnapshot:  "user@example.com",
		Provider:         "codex",
	}

	candidate, ok := quotaAutoDisableCandidateFromEvent(event, "http://cpa", "key", now)
	if !ok {
		t.Fatalf("candidate not detected")
	}
	if candidate.FileName != "codex-auth.json" || candidate.DisplayAccount != "user@example.com" {
		t.Fatalf("candidate identity = %#v", candidate)
	}
	if got := candidate.ResetAt.Unix(); got != 1_700_000_360 {
		t.Fatalf("reset unix = %d", got)
	}

	event.AuthFileSnapshot = ""
	if _, ok := quotaAutoDisableCandidateFromEvent(event, "http://cpa", "key", now); ok {
		t.Fatalf("candidate should require auth file snapshot")
	}
	event.AuthFileSnapshot = "codex-auth.json"
	event.FailBody = `{"error":{"code":"quota_exhausted"}}`
	if _, ok := quotaAutoDisableCandidateFromEvent(event, "http://cpa", "key", now); ok {
		t.Fatalf("candidate should require reset time")
	}
}

func TestQuotaResetTimeParsesCommonShapes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cases := []struct {
		name string
		text string
		want int64
	}{
		{
			name: "codex nested reset_at",
			text: `{"rate_limit":{"primary":{"reset_at":1700000600}}}`,
			want: 1_700_000_600,
		},
		{
			name: "retry after header array",
			text: `{"response_headers":{"Retry-After":["30"]}}`,
			want: 1_700_000_030,
		},
		{
			name: "milliseconds",
			text: `{"resetAt":1700000900000}`,
			want: 1_700_000_900,
		},
		{
			name: "plain text",
			text: `quota exhausted, reset_at: 1700001200`,
			want: 1_700_001_200,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resetTimeFromText(tc.text, now)
			if !ok {
				t.Fatalf("reset time not parsed")
			}
			if got.Unix() != tc.want {
				t.Fatalf("reset unix = %d, want %d", got.Unix(), tc.want)
			}
		})
	}
}

func TestRateLimitAutoDisableWorkerDisablesThenEnablesAtReset(t *testing.T) {
	var mu sync.Mutex
	type action struct {
		Name     string `json:"name"`
		Disabled bool   `json:"disabled"`
	}
	actions := make([]action, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/auth-files" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-management-key" {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}
		var item action
		if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		actions = append(actions, item)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	worker := NewRateLimitAutoDisableWorker(nil)
	worker.enableCheckInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)

	worker.HandleUsageEvents(ctx, collectorpkg.RuntimeConfig{
		CPAUpstreamURL: server.URL,
		ManagementKey:  "test-management-key",
	}, []usage.Event{{
		EventHash:        "evt-quota",
		Failed:           true,
		FailStatusCode:   http.StatusTooManyRequests,
		FailBody:         `{"error":{"code":"quota_exhausted","reset_at":1}}`,
		AuthFileSnapshot: "codex-auth.json",
		AccountSnapshot:  "user@example.com",
	}})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(actions)
		mu.Unlock()
		if count >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(actions) < 2 {
		t.Fatalf("actions = %#v, want disable and enable", actions)
	}
	if actions[0].Name != "codex-auth.json" || !actions[0].Disabled {
		t.Fatalf("disable action = %#v", actions[0])
	}
	if actions[1].Name != "codex-auth.json" || actions[1].Disabled {
		t.Fatalf("enable action = %#v", actions[1])
	}
}
