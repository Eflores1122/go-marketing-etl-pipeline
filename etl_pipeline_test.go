package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// Tests share the pipeline's package-level maps and must not call t.Parallel.

func resetPipeline() {
	trackedUsers = make(map[string]*TrackedUser)
	events = nil
	wg = sync.WaitGroup{}
}

func writeRaw(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeCSV(t *testing.T, name string, rows [][]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func runProcessor(t *testing.T, process func(string), path string) {
	t.Helper()
	resetPipeline()
	wg.Add(1)
	process(path)
	wg.Wait()
}

func userBySession(session string) *TrackedUser {
	for _, user := range trackedUsers {
		if user.SessionKey == session {
			return user
		}
	}
	return nil
}

func nearly(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestGenerateTrackingHash(t *testing.T) {
	tests := []struct {
		name    string
		session string
		device  string
		want    string
	}{
		{
			name:    "sample session",
			session: "sess-001",
			device:  "mobile",
			want:    "8febfb777a3edd7d2fb20f27b692082915412bea685c7187c6c8bc911822717a",
		},
		{
			name:    "same session other device",
			session: "sess-001",
			device:  "desktop",
			want:    "e324e406eb6133fac7bef86b59db7d5c298b4e6635b634800f32a513d8a824c7",
		},
		{
			name:    "empty inputs",
			session: "",
			device:  "",
			want:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateTrackingHash(tt.session, tt.device)
			if got != tt.want {
				t.Fatalf("hash = %s, want %s", got, tt.want)
			}
			if again := generateTrackingHash(tt.session, tt.device); again != got {
				t.Fatalf("hash changed across calls: %s vs %s", got, again)
			}
		})
	}

	// The hash concatenates the two strings with no delimiter, so these collide.
	if generateTrackingHash("ab", "c") != generateTrackingHash("a", "bc") {
		t.Fatal("expected session_key and device_type to be concatenated with no delimiter")
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "utc clock time", input: "2023-01-01 12:00:00", want: "2023-01-01 12:00:00"},
		{name: "single digit rejected", input: "2023-1-01 12:00:00", wantErr: true},
		{name: "rfc3339 rejected", input: "2023-01-01T12:00:00Z", wantErr: true},
		{name: "empty", input: "", wantErr: true},
		{name: "date only", input: "2023-01-01", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTimestamp(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAdClickTransforms(t *testing.T) {
	const (
		googleHeader   = "timestamp,user_id,session_key,device_type,campaign,ad_group,cost,utm_channel,gclid"
		facebookHeader = "timestamp,user_id,session_key,device_type,campaign,ad_set,spend,utm_channel,fbclid"
	)
	tests := []struct {
		name      string
		process   func(string)
		header    string
		row       string
		wantSkip  bool
		device    string
		wantEvent Event
		wantUser  TrackedUser
	}{
		{
			name:    "google click uses campaign column for ad_group metadata",
			process: processGoogleClicks,
			header:  googleHeader,
			device:  "mobile",
			row:     "2023-01-01 12:00:00,user-1,sess-001,mobile,Campaign A,Group 1,0.50,google,GCLID123",
			wantEvent: Event{
				EventTime: "2023-01-01 12:00:00",
				EventType: "click",
				Source:    "Google Ads",
				Campaign:  "Campaign A",
				Cost:      0.50,
				Revenue:   0,
				Metadata:  `{"ad_group":"Campaign A","gclid":"GCLID123"}`,
			},
			wantUser: TrackedUser{
				UserID:       "user-1",
				SessionKey:   "sess-001",
				UTMChannel:   "google",
				Attributes:   "{}",
				EarliestTime: "2023-01-01 12:00:00",
				LatestTime:   "2023-01-01 12:00:00",
			},
		},
		{
			name:    "google invalid cost becomes zero and the click is kept",
			process: processGoogleClicks,
			header:  googleHeader,
			device:  "desktop",
			row:     "2023-01-01 12:00:00,,sess-002,desktop,Campaign B,Group 2,not-a-number,google,GCLID456",
			wantEvent: Event{
				EventTime: "2023-01-01 12:00:00",
				EventType: "click",
				Source:    "Google Ads",
				Campaign:  "Campaign B",
				Cost:      0,
				Revenue:   0,
				Metadata:  `{"ad_group":"Campaign B","gclid":"GCLID456"}`,
			},
			wantUser: TrackedUser{
				UserID:       "",
				SessionKey:   "sess-002",
				UTMChannel:   "google",
				Attributes:   "{}",
				EarliestTime: "2023-01-01 12:00:00",
				LatestTime:   "2023-01-01 12:00:00",
			},
		},
		{
			name:     "google invalid timestamp is skipped",
			process:  processGoogleClicks,
			header:   googleHeader,
			row:      "01/01/2023 12:00:00,user-1,sess-001,mobile,Campaign A,Group 1,0.50,google,GCLID123",
			wantSkip: true,
		},
		{
			name:     "google missing device is skipped",
			process:  processGoogleClicks,
			header:   googleHeader,
			row:      "2023-01-01 12:00:00,user-1,sess-001,,Campaign A,Group 1,0.50,google,GCLID123",
			wantSkip: true,
		},
		{
			name:    "facebook click uses campaign column for ad_set metadata",
			process: processFacebookClicks,
			header:  facebookHeader,
			device:  "mobile",
			row:     "2023-01-01 14:00:00,,sess-005,mobile,FB Campaign X,Set 1,1.00,facebook,FBCLID123",
			wantEvent: Event{
				EventTime: "2023-01-01 14:00:00",
				EventType: "click",
				Source:    "Facebook Ads",
				Campaign:  "FB Campaign X",
				Cost:      1,
				Revenue:   0,
				Metadata:  `{"ad_set":"FB Campaign X","fbclid":"FBCLID123"}`,
			},
			wantUser: TrackedUser{
				UserID:       "",
				SessionKey:   "sess-005",
				UTMChannel:   "facebook",
				Attributes:   "{}",
				EarliestTime: "2023-01-01 14:00:00",
				LatestTime:   "2023-01-01 14:00:00",
			},
		},
		{
			name:    "facebook empty spend becomes zero",
			process: processFacebookClicks,
			header:  facebookHeader,
			device:  "mobile",
			row:     "2023-01-02 11:00:00,user-abc,sess-008,mobile,FB Campaign Z,Set 3,,facebook,FBCLID987",
			wantEvent: Event{
				EventTime: "2023-01-02 11:00:00",
				EventType: "click",
				Source:    "Facebook Ads",
				Campaign:  "FB Campaign Z",
				Cost:      0,
				Revenue:   0,
				Metadata:  `{"ad_set":"FB Campaign Z","fbclid":"FBCLID987"}`,
			},
			wantUser: TrackedUser{
				UserID:       "user-abc",
				SessionKey:   "sess-008",
				UTMChannel:   "facebook",
				Attributes:   "{}",
				EarliestTime: "2023-01-02 11:00:00",
				LatestTime:   "2023-01-02 11:00:00",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeRaw(t, "clicks.csv", tt.header+"\n"+tt.row+"\n")
			runProcessor(t, tt.process, path)
			if tt.wantSkip {
				if len(events) != 0 || len(trackedUsers) != 0 {
					t.Fatalf("skipped row produced users=%d events=%d", len(trackedUsers), len(events))
				}
				return
			}
			if len(events) != 1 || len(trackedUsers) != 1 {
				t.Fatalf("users=%d events=%d", len(trackedUsers), len(events))
			}
			got := events[0]
			nearly(t, got.Cost, tt.wantEvent.Cost)
			nearly(t, got.Revenue, tt.wantEvent.Revenue)
			if got.EventTime != tt.wantEvent.EventTime || got.EventType != tt.wantEvent.EventType ||
				got.Source != tt.wantEvent.Source || got.Campaign != tt.wantEvent.Campaign ||
				got.Metadata != tt.wantEvent.Metadata {
				t.Fatalf("event = %+v", got)
			}
			user := userBySession(tt.wantUser.SessionKey)
			if user == nil {
				t.Fatal("missing user")
			}
			wantHash := generateTrackingHash(tt.wantUser.SessionKey, tt.device)
			if user.UserID != tt.wantUser.UserID || user.UTMChannel != tt.wantUser.UTMChannel ||
				user.Attributes != tt.wantUser.Attributes || user.EarliestTime != tt.wantUser.EarliestTime ||
				user.LatestTime != tt.wantUser.LatestTime || user.TrackingHash != wantHash ||
				got.TrackingHash != wantHash {
				t.Fatalf("user = %+v", user)
			}
		})
	}
}

func TestWebsiteTransforms(t *testing.T) {
	header := []string{"timestamp", "user_id", "session_key", "device_type", "action_type", "action_details", "utm_channel"}
	tests := []struct {
		name        string
		actionType  string
		details     string
		wantType    string
		wantRevenue float64
		wantMeta    string
	}{
		{name: "view keeps action type", actionType: "view", details: `{"path":"/home"}`, wantType: "view", wantMeta: `{"path":"/home"}`},
		{name: "view ignores value", actionType: "view", details: `{"value":9}`, wantType: "view", wantRevenue: 0, wantMeta: `{"value":9}`},
		{name: "purchase number becomes conversion", actionType: "purchase", details: `{"value":100}`, wantType: "conversion", wantRevenue: 100, wantMeta: `{"value":100}`},
		{name: "signup string value", actionType: "signup", details: `{"value":"12.5"}`, wantType: "conversion", wantRevenue: 12.5, wantMeta: `{"value":"12.5"}`},
		{name: "signup unparseable value", actionType: "signup", details: `{"value":"free"}`, wantType: "conversion", wantRevenue: 0, wantMeta: `{"value":"free"}`},
		{name: "purchase without value", actionType: "purchase", details: `{"type":"purchase"}`, wantType: "conversion", wantMeta: `{"type":"purchase"}`},
		{name: "invalid json still converts purchase", actionType: "purchase", details: `not-json`, wantType: "conversion", wantMeta: `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeCSV(t, "site.csv", [][]string{
				header,
				{"2023-01-01 12:05:00", "user-1", "sess-001", "mobile", tt.actionType, tt.details, "google"},
			})
			runProcessor(t, processSiteEvents, path)
			if len(events) != 1 {
				t.Fatalf("events = %d", len(events))
			}
			got := events[0]
			if got.EventType != tt.wantType || got.Source != "Website" || got.Campaign != "" || got.Metadata != tt.wantMeta {
				t.Fatalf("event = %+v", got)
			}
			nearly(t, got.Revenue, tt.wantRevenue)
			nearly(t, got.Cost, 0)
		})
	}
}

func TestDedupMergeRules(t *testing.T) {
	header := []string{"timestamp", "user_id", "session_key", "device_type", "action_type", "action_details", "utm_channel"}
	path := writeCSV(t, "site.csv", [][]string{
		header,
		{"2023-01-01 10:00:00", "", "sess-m", "mobile", "view", `{}`, "google"},
		{"2023-01-01 12:00:00", "user-1", "sess-m", "mobile", "view", `{}`, "facebook"},
		{"2023-01-01 11:00:00", "user-other", "sess-m", "mobile", "view", `{}`, "email"},
		{"2023-01-01 11:00:00", "user-1", "sess-m", "desktop", "view", `{}`, "google"},
	})
	runProcessor(t, processSiteEvents, path)

	if len(trackedUsers) != 2 {
		t.Fatalf("users = %d, want 2 (mobile and desktop stay separate)", len(trackedUsers))
	}
	if len(events) != 4 {
		t.Fatalf("events = %d", len(events))
	}
	mobile := trackedUsers[generateTrackingHash("sess-m", "mobile")]
	if mobile == nil {
		t.Fatal("missing mobile user")
	}
	if mobile.UserID != "user-1" {
		t.Fatalf("user id = %q, want the first non-empty id to stick", mobile.UserID)
	}
	if mobile.UTMChannel != "google" {
		t.Fatalf("utm = %q, want the first writer's channel", mobile.UTMChannel)
	}
	if mobile.EarliestTime != "2023-01-01 10:00:00" || mobile.LatestTime != "2023-01-01 12:00:00" {
		t.Fatalf("window = %s .. %s", mobile.EarliestTime, mobile.LatestTime)
	}
	desktop := trackedUsers[generateTrackingHash("sess-m", "desktop")]
	if desktop == nil || desktop.UserID != "user-1" || desktop.UTMChannel != "google" {
		t.Fatalf("desktop user = %+v", desktop)
	}
}

func TestSiteFieldRejoin(t *testing.T) {
	// The comma inside action_details splits the row. The loader joins every
	// field except the last back into JSON, and keeps the last field as utm_channel.
	body := strings.Join([]string{
		"timestamp,user_id,session_key,device_type,action_type,action_details,utm_channel",
		`2023-03-01 00:00:00,user-x,sess-x,tablet,purchase,{"value": 1, "extra": 2},facebook`,
		"2023-03-01 01:00:00,user-y,sess-y,tablet,view,only-six-fields",
		"",
	}, "\n")
	path := writeRaw(t, "site.csv", body)
	runProcessor(t, processSiteEvents, path)

	if len(events) != 1 || len(trackedUsers) != 1 {
		t.Fatalf("users=%d events=%d, short row should be skipped", len(trackedUsers), len(events))
	}
	got := events[0]
	if got.EventType != "conversion" || got.Metadata != `{"extra":2,"value":1}` {
		t.Fatalf("event = %+v", got)
	}
	nearly(t, got.Revenue, 1)
	user := userBySession("sess-x")
	if user == nil || user.UTMChannel != "facebook" {
		t.Fatalf("user = %+v", user)
	}
}

func TestMissingFileAndHeaderOnly(t *testing.T) {
	runProcessor(t, processGoogleClicks, filepath.Join(t.TempDir(), "missing.csv"))
	if len(events) != 0 || len(trackedUsers) != 0 {
		t.Fatal("missing file should produce no rows")
	}

	headerOnly := writeRaw(t, "google.csv", "timestamp,user_id,session_key,device_type,campaign,ad_group,cost,utm_channel,gclid\n")
	runProcessor(t, processGoogleClicks, headerOnly)
	if len(events) != 0 || len(trackedUsers) != 0 {
		t.Fatal("header-only file should produce no rows")
	}
}

func TestDatabaseFileIsCreated(t *testing.T) {
	resetPipeline()
	path := filepath.Join(t.TempDir(), "marketing_data.db")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("database file should not exist before load")
	}
	loadToDatabase(path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("database file is empty")
	}

	db := openDB(t, path)
	var users, eventsCount int
	if err := db.QueryRow(`select count(*) from tracked_user`).Scan(&users); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select count(*) from event`).Scan(&eventsCount); err != nil {
		t.Fatal(err)
	}
	if users != 0 || eventsCount != 0 {
		t.Fatalf("empty load inserted users=%d events=%d", users, eventsCount)
	}
}

func TestReloadAppendsRows(t *testing.T) {
	resetPipeline()
	hash := generateTrackingHash("sess-001", "mobile")
	upsertTrackedUser(hash, "user-1", "sess-001", "google", "2023-01-01 12:00:00")
	appendEvent(Event{
		TrackingHash: hash,
		EventTime:    "2023-01-01 12:00:00",
		EventType:    "click",
		Source:       "Google Ads",
		Campaign:     "Campaign A",
		Cost:         0.5,
		Metadata:     `{"ad_group":"Campaign A","gclid":"GCLID123"}`,
	})

	path := filepath.Join(t.TempDir(), "marketing_data.db")
	loadToDatabase(path)
	loadToDatabase(path)

	db := openDB(t, path)
	var users, eventsCount int
	if err := db.QueryRow(`select count(*) from tracked_user`).Scan(&users); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select count(*) from event`).Scan(&eventsCount); err != nil {
		t.Fatal(err)
	}
	if users != 2 || eventsCount != 2 {
		t.Fatalf("second load users=%d events=%d, want appended copies", users, eventsCount)
	}
}

func TestConcurrentUpsertAndLoad(t *testing.T) {
	resetPipeline()
	const workers = 64
	hash := generateTrackingHash("sess-race", "mobile")
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			ts := fmt.Sprintf("2023-01-01 12:%02d:00", i%60)
			userID := ""
			channel := "facebook"
			if i%2 == 0 {
				userID = "user-even"
				channel = "google"
			}
			upsertTrackedUser(hash, userID, "sess-race", channel, ts)
			appendEvent(Event{
				TrackingHash: hash,
				EventTime:    ts,
				EventType:    "click",
				Source:       "Google Ads",
				Cost:         0.25,
			})
		}(i)
	}
	wg.Wait()

	if len(trackedUsers) != 1 {
		t.Fatalf("users = %d", len(trackedUsers))
	}
	if len(events) != workers {
		t.Fatalf("events = %d", len(events))
	}
	user := trackedUsers[hash]
	if user.UserID != "user-even" {
		t.Fatalf("user id = %q", user.UserID)
	}
	if user.UTMChannel != "google" && user.UTMChannel != "facebook" {
		t.Fatalf("utm = %q", user.UTMChannel)
	}
	if user.EarliestTime != "2023-01-01 12:00:00" || user.LatestTime != "2023-01-01 12:59:00" {
		t.Fatalf("window = %s .. %s", user.EarliestTime, user.LatestTime)
	}

	path := filepath.Join(t.TempDir(), "marketing_data.db")
	loadToDatabase(path)
	db := openDB(t, path)
	var users, eventsCount int
	var cost float64
	if err := db.QueryRow(`select count(*) from tracked_user`).Scan(&users); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select count(*), coalesce(sum(cost), 0) from event`).Scan(&eventsCount, &cost); err != nil {
		t.Fatal(err)
	}
	if users != 1 || eventsCount != workers {
		t.Fatalf("db users=%d events=%d", users, eventsCount)
	}
	nearly(t, cost, 0.25*workers)
}

func TestConcurrentSampleLoad(t *testing.T) {
	resetPipeline()
	wg.Add(3)
	go processGoogleClicks("google_clicks.csv")
	go processFacebookClicks("facebook_clicks.csv")
	go processSiteEvents("site_events.csv")
	wg.Wait()

	assertSampleMemory(t)

	path := filepath.Join(t.TempDir(), "marketing_data.db")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("load should create the database file")
	}
	loadToDatabase(path)
	assertSampleDatabase(t, path)
}

func assertSampleMemory(t *testing.T) {
	t.Helper()
	if len(trackedUsers) != 8 {
		t.Fatalf("users = %d, want 8", len(trackedUsers))
	}
	if len(events) != 12 {
		t.Fatalf("events = %d, want 12", len(events))
	}

	stable := []struct {
		session  string
		device   string
		userID   string
		channel  string
		earliest string
		latest   string
	}{
		{"sess-001", "mobile", "", "google", "2023-01-01 12:00:00", "2023-01-01 12:05:00"},
		{"sess-002", "desktop", "", "google", "2023-01-01 13:00:00", "2023-01-01 13:00:00"},
		{"sess-003", "tablet", "user-ghi", "google", "2023-01-02 09:00:00", "2023-01-02 09:00:00"},
		{"sess-004", "mobile", "user-abc", "google", "2023-01-02 10:30:00", "2023-01-02 10:30:00"},
		{"sess-005", "mobile", "user-abc", "facebook", "2023-01-01 14:00:00", "2023-01-01 15:00:00"},
		{"sess-007", "tablet", "user-ghi", "facebook", "2023-01-02 10:00:00", "2023-01-02 11:00:00"},
		{"sess-008", "mobile", "user-abc", "facebook", "2023-01-02 11:00:00", "2023-01-02 11:00:00"},
	}
	for _, tt := range stable {
		user := trackedUsers[generateTrackingHash(tt.session, tt.device)]
		if user == nil {
			t.Fatalf("missing %s", tt.session)
		}
		if user.UserID != tt.userID || user.UTMChannel != tt.channel || user.Attributes != "{}" ||
			user.EarliestTime != tt.earliest || user.LatestTime != tt.latest {
			t.Fatalf("%s = %+v", tt.session, user)
		}
	}

	// sess-006 is touched by Facebook Ads (utm facebook) and the site file (utm google).
	// The first writer keeps the channel. Times and user_id do not depend on that race.
	shared := trackedUsers[generateTrackingHash("sess-006", "desktop")]
	if shared == nil {
		t.Fatal("missing sess-006")
	}
	if shared.UserID != "user-def" || shared.Attributes != "{}" ||
		shared.EarliestTime != "2023-01-01 14:30:00" || shared.LatestTime != "2023-01-01 16:00:00" {
		t.Fatalf("sess-006 = %+v", shared)
	}
	if shared.UTMChannel != "google" && shared.UTMChannel != "facebook" {
		t.Fatalf("sess-006 utm = %q", shared.UTMChannel)
	}

	var cost, revenue float64
	var clicks, views, conversions, websiteEmpty int
	for _, event := range events {
		cost += event.Cost
		revenue += event.Revenue
		switch event.EventType {
		case "click":
			clicks++
		case "view":
			views++
		case "conversion":
			conversions++
		}
		if event.Source == "Website" && event.Metadata == "{}" {
			websiteEmpty++
		}
	}
	nearly(t, cost, 5.55)
	nearly(t, revenue, 0)
	if clicks != 8 || views != 1 || conversions != 3 || websiteEmpty != 4 {
		t.Fatalf("clicks=%d views=%d conversions=%d websiteEmpty=%d", clicks, views, conversions, websiteEmpty)
	}
}

func assertSampleDatabase(t *testing.T, path string) {
	t.Helper()
	db := openDB(t, path)

	var users, eventsCount, conversions int
	var cost, revenue float64
	if err := db.QueryRow(`select count(*) from tracked_user`).Scan(&users); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select count(*), coalesce(sum(cost), 0), coalesce(sum(revenue), 0) from event`).Scan(&eventsCount, &cost, &revenue); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select count(*) from event where event_type = 'conversion'`).Scan(&conversions); err != nil {
		t.Fatal(err)
	}
	if users != 8 || eventsCount != 12 || conversions != 3 {
		t.Fatalf("db users=%d events=%d conversions=%d", users, eventsCount, conversions)
	}
	nearly(t, cost, 5.55)
	nearly(t, revenue, 0)

	var googleCost, facebookCost float64
	if err := db.QueryRow(`select coalesce(sum(cost), 0) from event where source = 'Google Ads'`).Scan(&googleCost); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select coalesce(sum(cost), 0) from event where source = 'Facebook Ads'`).Scan(&facebookCost); err != nil {
		t.Fatal(err)
	}
	nearly(t, googleCost, 2.30)
	nearly(t, facebookCost, 3.25)

	var multi int64
	if err := db.QueryRow(bonusSQL(t)).Scan(&multi); err != nil {
		t.Fatal(err)
	}
	if multi != 0 {
		t.Fatalf("multi_channel_converters = %d", multi)
	}

	var hash string
	err := db.QueryRow(`select tracking_hash from tracked_user where session_key = 'sess-001'`).Scan(&hash)
	if err != nil {
		t.Fatal(err)
	}
	if hash != generateTrackingHash("sess-001", "mobile") {
		t.Fatalf("stored hash %s", hash)
	}
}

func bonusSQL(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("bonus_query.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(body)
	if i := strings.Index(sqlText, "/*"); i >= 0 {
		sqlText = sqlText[:i]
	}
	return sqlText
}

func TestRunPipelineSampleMatchesBonusSQL(t *testing.T) {
	google := readRepoFile(t, "google_clicks.csv")
	facebook := readRepoFile(t, "facebook_clicks.csv")
	site := readRepoFile(t, "site_events.csv")

	first := runPipeline(google, facebook, site)
	second := runPipeline(google, facebook, site)
	if len(second.Users) != 8 || len(second.Events) != 12 {
		t.Fatalf("second run users=%d events=%d, want a fresh load", len(second.Users), len(second.Events))
	}
	if len(first.Users) != len(second.Users) || len(first.Events) != len(second.Events) {
		t.Fatal("sample shape changed between runs")
	}
	assertRunSample(t, second)
	if got := bonusOnSnapshot(t, second); got != int64(second.Summary.MultiChannelConverters) {
		t.Fatalf("sql multi=%d go multi=%d", got, second.Summary.MultiChannelConverters)
	}
}

func TestRunPipelineMultiChannelPredicate(t *testing.T) {
	googleHeader := []string{"timestamp", "user_id", "session_key", "device_type", "campaign", "ad_group", "cost", "utm_channel", "gclid"}
	facebookHeader := []string{"timestamp", "user_id", "session_key", "device_type", "campaign", "ad_set", "spend", "utm_channel", "fbclid"}
	siteHeader := []string{"timestamp", "user_id", "session_key", "device_type", "action_type", "action_details", "utm_channel"}

	bothBefore := runPipeline(
		csvText(t, [][]string{
			googleHeader,
			{"2023-01-01 10:00:00", "user-z", "sess-z", "mobile", "Camp", "Group", "1.25", "google", "g1"},
		}),
		csvText(t, [][]string{
			facebookHeader,
			{"2023-01-01 11:00:00", "user-z", "sess-z", "mobile", "FB", "Set", "0.75", "facebook", "f1"},
		}),
		csvText(t, [][]string{
			siteHeader,
			{"2023-01-01 12:00:00", "user-z", "sess-z", "mobile", "purchase", `{"value":10}`, "google"},
		}),
	)
	if bothBefore.Summary.Users != 1 || bothBefore.Summary.Events != 3 || bothBefore.Summary.Conversions != 1 {
		t.Fatalf("summary = %+v", bothBefore.Summary)
	}
	nearly(t, bothBefore.Summary.Revenue, 10)
	nearly(t, bothBefore.Summary.GoogleCost, 1.25)
	nearly(t, bothBefore.Summary.FacebookCost, 0.75)
	if bothBefore.Summary.MultiChannelConverters != 1 {
		t.Fatalf("multi = %d", bothBefore.Summary.MultiChannelConverters)
	}
	if got := bonusOnSnapshot(t, bothBefore); got != 1 {
		t.Fatalf("sql multi = %d", got)
	}

	facebookAfter := runPipeline(
		csvText(t, [][]string{
			googleHeader,
			{"2023-01-01 10:00:00", "user-z", "sess-z", "mobile", "Camp", "Group", "1.25", "google", "g1"},
		}),
		csvText(t, [][]string{
			facebookHeader,
			{"2023-01-01 13:00:00", "user-z", "sess-z", "mobile", "FB", "Set", "0.75", "facebook", "f1"},
		}),
		csvText(t, [][]string{
			siteHeader,
			{"2023-01-01 12:00:00", "user-z", "sess-z", "mobile", "purchase", `{"value":10}`, "google"},
		}),
	)
	if facebookAfter.Summary.MultiChannelConverters != 0 {
		t.Fatalf("late facebook click counted as %d", facebookAfter.Summary.MultiChannelConverters)
	}
	if got := bonusOnSnapshot(t, facebookAfter); got != 0 {
		t.Fatalf("sql multi = %d", got)
	}
}

func TestRunPipelineRecoversShortAdRow(t *testing.T) {
	got := runPipeline(
		"timestamp,user_id,session_key\n2023-01-01 12:00:00,user,sess\n",
		"timestamp,user_id,session_key,device_type,campaign,ad_set,spend,utm_channel,fbclid\n",
		"timestamp,user_id,session_key,device_type,action_type,action_details,utm_channel\n",
	)
	if len(got.Users) != 0 || len(got.Events) != 0 {
		t.Fatalf("short row produced users=%d events=%d", len(got.Users), len(got.Events))
	}
	found := false
	for _, line := range got.Logs {
		if strings.Contains(line, "panic en google_clicks.csv") {
			found = true
		}
	}
	if !found {
		t.Fatalf("logs = %#v", got.Logs)
	}
}

func assertRunSample(t *testing.T, got PipelineResult) {
	t.Helper()
	s := got.Summary
	if s.Users != 8 || s.Events != 12 || s.Clicks != 8 || s.Views != 1 || s.Conversions != 3 {
		t.Fatalf("summary = %+v", s)
	}
	if s.GoogleClicks != 4 || s.FacebookClicks != 4 || s.WebsiteEvents != 4 {
		t.Fatalf("sources = %+v", s)
	}
	nearly(t, s.GoogleCost, 2.30)
	nearly(t, s.FacebookCost, 3.25)
	nearly(t, s.WebsiteCost, 0)
	nearly(t, s.Revenue, 0)
	if s.EmptyWebsiteMetadata != 4 || s.EmptyUserIDs != 2 || s.MultiChannelConverters != 0 {
		t.Fatalf("summary = %+v", s)
	}
	if len(got.Logs) != 0 {
		t.Fatalf("logs = %#v", got.Logs)
	}
	if len(s.Sess006) != 1 {
		t.Fatalf("sess-006 = %+v", s.Sess006)
	}
	if s.Sess006[0].UTMChannel != "google" && s.Sess006[0].UTMChannel != "facebook" {
		t.Fatalf("sess-006 channel = %q", s.Sess006[0].UTMChannel)
	}
	var sess001 *PipelineUser
	for i := range got.Users {
		if got.Users[i].SessionKey == "sess-001" {
			sess001 = &got.Users[i]
			break
		}
	}
	if sess001 == nil || sess001.TrackingHash != generateTrackingHash("sess-001", "mobile") {
		t.Fatalf("sess-001 = %+v", sess001)
	}
	ids := map[int64]bool{}
	for _, user := range got.Users {
		if user.TrackedUserID == 0 || ids[user.TrackedUserID] {
			t.Fatalf("user id %+v", user)
		}
		ids[user.TrackedUserID] = true
	}
	for _, event := range got.Events {
		if !ids[event.TrackedUserID] {
			t.Fatalf("event user %d missing", event.TrackedUserID)
		}
	}
}

func bonusOnSnapshot(t *testing.T, got PipelineResult) int64 {
	t.Helper()
	db := openDB(t, filepath.Join(t.TempDir(), "snapshot.db"))
	if _, err := db.Exec(`CREATE TABLE tracked_user (
		tracked_user_id INTEGER PRIMARY KEY,
		user_id TEXT,
		session_key TEXT,
		tracking_hash TEXT,
		utm_channel TEXT,
		attributes TEXT,
		earliest_time TEXT,
		latest_time TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE event (
		event_id INTEGER PRIMARY KEY,
		tracked_user_id INTEGER,
		event_time TEXT,
		event_type TEXT,
		source TEXT,
		campaign TEXT,
		cost REAL,
		revenue REAL,
		metadata TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	userStmt, err := db.Prepare(`INSERT INTO tracked_user (
		tracked_user_id, user_id, session_key, tracking_hash, utm_channel, attributes, earliest_time, latest_time
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range got.Users {
		if _, err := userStmt.Exec(user.TrackedUserID, user.UserID, user.SessionKey, user.TrackingHash, user.UTMChannel, user.Attributes, user.EarliestTime, user.LatestTime); err != nil {
			t.Fatal(err)
		}
	}
	_ = userStmt.Close()
	eventStmt, err := db.Prepare(`INSERT INTO event (
		event_id, tracked_user_id, event_time, event_type, source, campaign, cost, revenue, metadata
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range got.Events {
		if _, err := eventStmt.Exec(event.EventID, event.TrackedUserID, event.EventTime, event.EventType, event.Source, event.Campaign, event.Cost, event.Revenue, event.Metadata); err != nil {
			t.Fatal(err)
		}
	}
	_ = eventStmt.Close()
	var multi int64
	if err := db.QueryRow(bonusSQL(t)).Scan(&multi); err != nil {
		t.Fatal(err)
	}
	var cost float64
	if err := db.QueryRow(`select coalesce(sum(cost), 0) from event`).Scan(&cost); err != nil {
		t.Fatal(err)
	}
	nearly(t, cost, got.Summary.GoogleCost+got.Summary.FacebookCost+got.Summary.WebsiteCost)
	return multi
}

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func csvText(t *testing.T, rows [][]string) string {
	t.Helper()
	var b strings.Builder
	w := csv.NewWriter(&b)
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func openDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}
