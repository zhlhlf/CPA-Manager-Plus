package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	collectorpkg "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/cpa"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

const (
	quotaAutoDisableQueueSize     = 256
	quotaAutoDisableDefaultTick   = 15 * time.Second
	quotaAutoDisableActionTimeout = 15 * time.Second
)

// RateLimitAutoDisableWorker reacts to request-monitoring events in near real time.
// When CPA reports a quota-exhausted response with a reset time, the worker disables
// the corresponding auth file immediately and re-enables that same file when the
// reported reset time arrives.
type RateLimitAutoDisableWorker struct {
	store  *store.Store
	client *http.Client

	jobs chan quotaAutoDisableCandidate

	mu                  sync.Mutex
	scheduledEnables    map[string]scheduledQuotaEnable
	enableCheckInterval time.Duration
}

type quotaAutoDisableCandidate struct {
	BaseURL        string
	ManagementKey  string
	FileName       string
	DisplayAccount string
	Provider       string
	ResetAt        time.Time
	EventHash      string
	Reason         string
}

type scheduledQuotaEnable struct {
	BaseURL           string
	ManagementKey     string
	FileName          string
	DisplayAccount    string
	Provider          string
	DisabledAt        time.Time
	ResetAt           time.Time
	NextEnableAttempt time.Time
	LastEventHash     string
	Reason            string
}

func NewRateLimitAutoDisableWorker(st *store.Store) *RateLimitAutoDisableWorker {
	return &RateLimitAutoDisableWorker{
		store:               st,
		client:              &http.Client{Timeout: quotaAutoDisableActionTimeout},
		jobs:                make(chan quotaAutoDisableCandidate, quotaAutoDisableQueueSize),
		scheduledEnables:    make(map[string]scheduledQuotaEnable),
		enableCheckInterval: quotaAutoDisableDefaultTick,
	}
}

func (w *RateLimitAutoDisableWorker) Start(ctx context.Context) {
	go w.run(ctx)
}

// HandleUsageEvents is called by the request-monitoring collector after raw CPA
// usage events are normalized and enriched with auth-file snapshots. It does not
// poll historical events; it only reacts to newly observed request failures.
func (w *RateLimitAutoDisableWorker) HandleUsageEvents(ctx context.Context, cfg collectorpkg.RuntimeConfig, events []usage.Event) {
	if w == nil || len(events) == 0 {
		return
	}
	baseURL := strings.TrimSpace(cfg.CPAUpstreamURL)
	managementKey := strings.TrimSpace(cfg.ManagementKey)
	if baseURL == "" || managementKey == "" {
		return
	}
	now := time.Now()
	for _, event := range events {
		candidate, ok := quotaAutoDisableCandidateFromEvent(event, baseURL, managementKey, now)
		if !ok {
			continue
		}
		select {
		case w.jobs <- candidate:
		case <-ctx.Done():
			return
		default:
			log.Printf("[quota-auto-disable] job queue full, dropped auth file %q event=%q", candidate.FileName, candidate.EventHash)
		}
	}
}

func (w *RateLimitAutoDisableWorker) run(ctx context.Context) {
	interval := w.enableCheckInterval
	if interval <= 0 {
		interval = quotaAutoDisableDefaultTick
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case candidate := <-w.jobs:
			w.handleCandidate(ctx, candidate)
		case <-ticker.C:
			w.enableDue(ctx, time.Now())
		}
	}
}

func (w *RateLimitAutoDisableWorker) handleCandidate(ctx context.Context, candidate quotaAutoDisableCandidate) {
	if candidate.FileName == "" || candidate.BaseURL == "" || candidate.ManagementKey == "" {
		return
	}
	now := time.Now()
	if !candidate.ResetAt.After(now) {
		log.Printf("[quota-auto-disable] quota event for auth file %q has non-future reset time %s, skip auto disable", candidate.FileName, candidate.ResetAt.Format(time.RFC3339))
		return
	}

	w.mu.Lock()
	existing, alreadyScheduled := w.scheduledEnables[candidate.FileName]
	if alreadyScheduled {
		if candidate.ResetAt.After(existing.ResetAt) {
			existing.ResetAt = candidate.ResetAt
			existing.NextEnableAttempt = candidate.ResetAt
			existing.LastEventHash = candidate.EventHash
			existing.Reason = candidate.Reason
			w.scheduledEnables[candidate.FileName] = existing
			log.Printf("[quota-auto-disable] extended auth file %q auto-enable time to %s", candidate.FileName, candidate.ResetAt.Format(time.RFC3339))
		}
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()

	log.Printf("[quota-auto-disable] quota exhausted for auth file %q account=%q provider=%q resetAt=%s, disabling", candidate.FileName, candidate.DisplayAccount, candidate.Provider, candidate.ResetAt.Format(time.RFC3339))
	if err := w.patchAuthFile(ctx, candidate.BaseURL, candidate.ManagementKey, candidate.FileName, true); err != nil {
		log.Printf("[quota-auto-disable] failed to disable auth file %q: %v", candidate.FileName, err)
		return
	}

	w.mu.Lock()
	if existing, ok := w.scheduledEnables[candidate.FileName]; ok && existing.ResetAt.After(candidate.ResetAt) {
		w.mu.Unlock()
		return
	}
	w.scheduledEnables[candidate.FileName] = scheduledQuotaEnable{
		BaseURL:           candidate.BaseURL,
		ManagementKey:     candidate.ManagementKey,
		FileName:          candidate.FileName,
		DisplayAccount:    candidate.DisplayAccount,
		Provider:          candidate.Provider,
		DisabledAt:        now,
		ResetAt:           candidate.ResetAt,
		NextEnableAttempt: candidate.ResetAt,
		LastEventHash:     candidate.EventHash,
		Reason:            candidate.Reason,
	}
	w.mu.Unlock()
	log.Printf("[quota-auto-disable] disabled auth file %q; scheduled auto-enable at %s", candidate.FileName, candidate.ResetAt.Format(time.RFC3339))
}

func (w *RateLimitAutoDisableWorker) enableDue(ctx context.Context, now time.Time) {
	w.mu.Lock()
	due := make([]scheduledQuotaEnable, 0)
	for _, item := range w.scheduledEnables {
		if !item.NextEnableAttempt.After(now) {
			due = append(due, item)
		}
	}
	w.mu.Unlock()

	for _, item := range due {
		w.mu.Lock()
		current, ok := w.scheduledEnables[item.FileName]
		if !ok || !current.ResetAt.Equal(item.ResetAt) || current.NextEnableAttempt.After(now) {
			w.mu.Unlock()
			continue
		}
		w.mu.Unlock()

		log.Printf("[quota-auto-disable] reset time reached for auth file %q account=%q, enabling", item.FileName, item.DisplayAccount)
		if err := w.patchAuthFile(ctx, item.BaseURL, item.ManagementKey, item.FileName, false); err != nil {
			log.Printf("[quota-auto-disable] failed to enable auth file %q: %v", item.FileName, err)
			w.mu.Lock()
			current, ok := w.scheduledEnables[item.FileName]
			if ok && current.ResetAt.Equal(item.ResetAt) {
				current.NextEnableAttempt = time.Now().Add(30 * time.Second)
				w.scheduledEnables[item.FileName] = current
			}
			w.mu.Unlock()
			continue
		}
		w.mu.Lock()
		current, ok = w.scheduledEnables[item.FileName]
		if ok && current.ResetAt.Equal(item.ResetAt) {
			delete(w.scheduledEnables, item.FileName)
		}
		w.mu.Unlock()
		log.Printf("[quota-auto-disable] enabled auth file %q after quota reset", item.FileName)
	}
}

func quotaAutoDisableCandidateFromEvent(event usage.Event, baseURL string, managementKey string, now time.Time) (quotaAutoDisableCandidate, bool) {
	if !isQuotaExhaustedEvent(event) {
		return quotaAutoDisableCandidate{}, false
	}
	fileName := strings.TrimSpace(event.AuthFileSnapshot)
	if fileName == "" {
		log.Printf("[quota-auto-disable] quota event %q has no auth file snapshot, skip auto disable", event.EventHash)
		return quotaAutoDisableCandidate{}, false
	}
	resetAt, ok := quotaResetTimeFromEvent(event, now)
	if !ok {
		log.Printf("[quota-auto-disable] quota event for auth file %q has no parseable reset time, skip auto disable", fileName)
		return quotaAutoDisableCandidate{}, false
	}
	return quotaAutoDisableCandidate{
		BaseURL:        baseURL,
		ManagementKey:  managementKey,
		FileName:       fileName,
		DisplayAccount: firstNonEmpty(event.AccountSnapshot, event.AuthLabelSnapshot, event.Source, fileName),
		Provider:       event.Provider,
		ResetAt:        resetAt,
		EventHash:      event.EventHash,
		Reason:         event.FailSummary,
	}, true
}

func isQuotaExhaustedEvent(event usage.Event) bool {
	if !event.Failed {
		return false
	}
	statusCode := event.FailStatusCode
	body := strings.ToLower(strings.Join([]string{event.FailSummary, event.FailBody, event.RawJSON}, "\n"))
	if strings.Contains(body, "quota_exhausted") ||
		strings.Contains(body, "quota exhausted") ||
		strings.Contains(body, "quota exceeded") ||
		strings.Contains(body, "limit reached") ||
		strings.Contains(body, "payment_required") ||
		strings.Contains(body, "rate_limit_exceeded") ||
		strings.Contains(body, "rate limit exceeded") ||
		strings.Contains(body, "rate limit reached") ||
		strings.Contains(body, "usage limit") {
		return true
	}
	return statusCode == http.StatusPaymentRequired && strings.Contains(body, "quota")
}

func quotaResetTimeFromEvent(event usage.Event, now time.Time) (time.Time, bool) {
	candidates := []string{event.FailBody, event.FailSummary, event.RawJSON}
	for _, candidate := range candidates {
		if resetAt, ok := resetTimeFromText(candidate, now); ok {
			return resetAt, true
		}
	}
	return time.Time{}, false
}

func resetTimeFromText(text string, now time.Time) (time.Time, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return time.Time{}, false
	}
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err == nil {
		if resetAt, ok := resetTimeFromJSON(decoded, now, ""); ok {
			return resetAt, true
		}
	}
	if resetAt, ok := resetTimeFromPlainText(text, now); ok {
		return resetAt, true
	}
	return time.Time{}, false
}

func resetTimeFromJSON(value any, now time.Time, keyHint string) (time.Time, bool) {
	switch typed := value.(type) {
	case map[string]any:
		preferred := []string{
			"reset_at", "resetAt", "reset_time", "resetTime", "resets_at", "resetsAt",
			"rate_limit_reset_at", "rateLimitResetAt", "retry-after", "Retry-After", "retry_after", "retryAfter",
			"x-ratelimit-reset", "X-RateLimit-Reset", "x_rate_limit_reset", "xRateLimitReset",
		}
		for _, key := range preferred {
			if raw, ok := typed[key]; ok {
				if resetAt, ok := parseResetValue(raw, now, key); ok {
					return resetAt, true
				}
			}
		}
		for key, child := range typed {
			if resetAt, ok := resetTimeFromJSON(child, now, key); ok {
				return resetAt, true
			}
		}
	case []any:
		if isResetKey(keyHint) || isRetryAfterKey(keyHint) {
			for _, item := range typed {
				if resetAt, ok := parseResetValue(item, now, keyHint); ok {
					return resetAt, true
				}
			}
		}
		for _, child := range typed {
			if resetAt, ok := resetTimeFromJSON(child, now, keyHint); ok {
				return resetAt, true
			}
		}
	default:
		if isResetKey(keyHint) || isRetryAfterKey(keyHint) {
			return parseResetValue(typed, now, keyHint)
		}
	}
	return time.Time{}, false
}

func parseResetValue(value any, now time.Time, keyHint string) (time.Time, bool) {
	if value == nil {
		return time.Time{}, false
	}
	if isRetryAfterKey(keyHint) {
		return parseRetryAfterValue(value, now)
	}
	switch typed := value.(type) {
	case json.Number:
		return parseResetNumberString(typed.String(), now, false)
	case float64:
		return resetTimeFromNumber(typed, now, false)
	case int:
		return resetTimeFromNumber(float64(typed), now, false)
	case int64:
		return resetTimeFromNumber(float64(typed), now, false)
	case string:
		return parseResetNumberString(strings.TrimSpace(typed), now, false)
	default:
		return parseResetNumberString(strings.TrimSpace(fmt.Sprint(typed)), now, false)
	}
}

func parseRetryAfterValue(value any, now time.Time) (time.Time, bool) {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if resetAt, ok := parseRetryAfterValue(item, now); ok {
				return resetAt, true
			}
		}
		return time.Time{}, false
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			return time.Time{}, false
		}
		if seconds, err := strconv.ParseFloat(text, 64); err == nil && seconds > 0 {
			return now.Add(time.Duration(seconds * float64(time.Second))), true
		}
		if parsed, err := http.ParseTime(text); err == nil {
			return parsed, true
		}
		return time.Time{}, false
	case json.Number:
		seconds, err := strconv.ParseFloat(typed.String(), 64)
		if err == nil && seconds > 0 {
			return now.Add(time.Duration(seconds * float64(time.Second))), true
		}
	case float64:
		if typed > 0 {
			return now.Add(time.Duration(typed * float64(time.Second))), true
		}
	case int:
		if typed > 0 {
			return now.Add(time.Duration(typed) * time.Second), true
		}
	case int64:
		if typed > 0 {
			return now.Add(time.Duration(typed) * time.Second), true
		}
	}
	return time.Time{}, false
}

func parseResetNumberString(text string, now time.Time, relative bool) (time.Time, bool) {
	if text == "" || strings.EqualFold(text, "null") {
		return time.Time{}, false
	}
	if parsed, ok := parseCommonTime(text); ok {
		return parsed, true
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil || value <= 0 {
		return time.Time{}, false
	}
	return resetTimeFromNumber(value, now, relative)
}

func resetTimeFromNumber(value float64, now time.Time, relative bool) (time.Time, bool) {
	if value <= 0 {
		return time.Time{}, false
	}
	if relative {
		return now.Add(time.Duration(value * float64(time.Second))), true
	}
	// Unix milliseconds, e.g. JavaScript timestamps.
	if value > 1_000_000_000_000 {
		return time.UnixMilli(int64(value)), true
	}
	// Unix seconds, e.g. Codex rate_limit.reset_at.
	if value > 1_000_000_000 {
		return time.Unix(int64(value), 0), true
	}
	// Some providers return seconds-until-reset in a reset field.
	if value < 366*24*60*60 {
		return now.Add(time.Duration(value * float64(time.Second))), true
	}
	return time.Time{}, false
}

func parseCommonTime(text string) (time.Time, bool) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		time.RFC1123,
		time.RFC1123Z,
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

var plainResetPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(retry-after|retry_after)\D{0,20}(\d+(?:\.\d+)?)`),
	regexp.MustCompile(`(?i)(reset(?:s)?(?:[_ -]?at|[_ -]?time)?)\D{0,40}(\d{10,13}|\d+(?:\.\d+)?)`),
}

func resetTimeFromPlainText(text string, now time.Time) (time.Time, bool) {
	for _, pattern := range plainResetPatterns {
		match := pattern.FindStringSubmatch(text)
		if len(match) < 3 {
			continue
		}
		keyHint := match[1]
		if isRetryAfterKey(keyHint) {
			return parseRetryAfterValue(match[2], now)
		}
		if resetAt, ok := parseResetNumberString(match[2], now, false); ok {
			return resetAt, true
		}
	}
	return time.Time{}, false
}

func isResetKey(key string) bool {
	normalized := normalizeKey(key)
	return normalized == "resetat" ||
		normalized == "resettime" ||
		normalized == "resetsat" ||
		normalized == "ratelimitresetat" ||
		normalized == "ratelimitreset" ||
		normalized == "xratelimitreset"
}

func isRetryAfterKey(key string) bool {
	normalized := normalizeKey(key)
	return normalized == "retryafter"
}

func normalizeKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	var b strings.Builder
	for _, r := range key {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (w *RateLimitAutoDisableWorker) disableAuthFile(ctx context.Context, baseURL string, managementKey string, fileName string) error {
	return w.patchAuthFile(ctx, baseURL, managementKey, fileName, true)
}

func (w *RateLimitAutoDisableWorker) enableAuthFile(ctx context.Context, baseURL string, managementKey string, fileName string) error {
	return w.patchAuthFile(ctx, baseURL, managementKey, fileName, false)
}

func (w *RateLimitAutoDisableWorker) patchAuthFile(ctx context.Context, baseURL string, managementKey string, fileName string, disabled bool) error {
	payload := map[string]any{"name": fileName, "disabled": disabled}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	base := cpa.NormalizeBaseURL(baseURL)
	paths := []string{
		base + "/auth-files",
		base + "/auth-files/status",
		base + "/v0/management/auth-files",
		base + "/v0/management/auth-files/status",
	}

	client := w.client
	if client == nil {
		client = http.DefaultClient
	}

	var endpointErrors []string
	for _, endpoint := range paths {
		reqCtx, cancel := context.WithTimeout(ctx, quotaAutoDisableActionTimeout)
		req, reqErr := http.NewRequestWithContext(reqCtx, http.MethodPatch, endpoint, bytes.NewReader(data))
		if reqErr != nil {
			cancel()
			endpointErrors = append(endpointErrors, fmt.Sprintf("%s: %v", endpoint, reqErr))
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+managementKey)

		res, doErr := client.Do(req)
		cancel()
		if doErr != nil {
			endpointErrors = append(endpointErrors, fmt.Sprintf("%s: %v", endpoint, doErr))
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		_ = res.Body.Close()

		if res.StatusCode >= 200 && res.StatusCode < 300 {
			return nil
		}
		endpointErrors = append(endpointErrors, fmt.Sprintf("%s: HTTP %d %s", endpoint, res.StatusCode, strings.TrimSpace(string(body))))
	}
	if len(endpointErrors) == 0 {
		return errors.New("no auth-file status endpoint attempted")
	}
	return fmt.Errorf("all auth-file status endpoints failed: %s", strings.Join(endpointErrors, "; "))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// NormalizeBaseURL is exported for legacy tests.
var NormalizeBaseURL = cpa.NormalizeBaseURL
