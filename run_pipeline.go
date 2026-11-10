package main

import (
	"sort"
	"strings"
	"sync"
)

// PipelineUser is one tracked_user row, with the id assigned the same way the
// native SQLite load does: range the map and take the next integer.
// Map iteration order is not stable across processes.
type PipelineUser struct {
	TrackedUserID int64  `json:"tracked_user_id"`
	UserID        string `json:"user_id"`
	SessionKey    string `json:"session_key"`
	TrackingHash  string `json:"tracking_hash"`
	UTMChannel    string `json:"utm_channel"`
	Attributes    string `json:"attributes"`
	EarliestTime  string `json:"earliest_time"`
	LatestTime    string `json:"latest_time"`
}

// PipelineEvent is one event row. EventID follows successful insert order,
// which is the order of the in-memory slice.
type PipelineEvent struct {
	EventID       int64   `json:"event_id"`
	TrackedUserID int64   `json:"tracked_user_id"`
	EventTime     string  `json:"event_time"`
	EventType     string  `json:"event_type"`
	Source        string  `json:"source"`
	Campaign      string  `json:"campaign"`
	Cost          float64 `json:"cost"`
	Revenue       float64 `json:"revenue"`
	Metadata      string  `json:"metadata"`
}

// SessionChannel is the utm_channel kept for one hash of a named session.
type SessionChannel struct {
	SessionKey   string `json:"session_key"`
	TrackingHash string `json:"tracking_hash"`
	UTMChannel   string `json:"utm_channel"`
}

// DailyMetric is one calendar day of the in-memory events.
// Day is the YYYY-MM-DD prefix of event_time. ROAS is revenue/cost and is
// left to the caller so a day with no cost is not stored as a fake ratio.
type DailyMetric struct {
	Day         string  `json:"day"`
	Cost        float64 `json:"cost"`
	Revenue     float64 `json:"revenue"`
	Conversions int     `json:"conversions"`
	Clicks      int     `json:"clicks"`
}

// PipelineSummary holds counts derived from the in-memory rows.
// MultiChannelConverters uses the same predicate as bonus_query.sql.
type PipelineSummary struct {
	Users                  int              `json:"users"`
	Events                 int              `json:"events"`
	Clicks                 int              `json:"clicks"`
	Views                  int              `json:"views"`
	Conversions            int              `json:"conversions"`
	GoogleClicks           int              `json:"google_clicks"`
	GoogleCost             float64          `json:"google_cost"`
	FacebookClicks         int              `json:"facebook_clicks"`
	FacebookCost           float64          `json:"facebook_cost"`
	WebsiteEvents          int              `json:"website_events"`
	WebsiteCost            float64          `json:"website_cost"`
	Revenue                float64          `json:"revenue"`
	EmptyWebsiteMetadata   int              `json:"empty_website_metadata"`
	EmptyUserIDs           int              `json:"empty_user_ids"`
	MultiChannelConverters int              `json:"multi_channel_converters"`
	SkippedRows            int              `json:"skipped_rows"`
	Sess006                []SessionChannel `json:"sess006"`
	Days                   []DailyMetric    `json:"days"`
}

// PipelineResult is what the browser receives from runMarketingETL.
type PipelineResult struct {
	Users   []PipelineUser  `json:"users"`
	Events  []PipelineEvent `json:"events"`
	Logs    []string        `json:"logs"`
	Summary PipelineSummary `json:"summary"`
}

// runPipeline resets shared state, runs the three workers on CSV text, and
// returns rows with ids. A short ad row that would panic the native binary is
// reported in Logs so the browser process stays up.
func runPipeline(googleCSV, facebookCSV, siteCSV string) PipelineResult {
	resetRunState()
	wg.Add(3)
	go ingestCSV("google_clicks.csv", googleCSV, 0, ingestGoogle)
	go ingestCSV("facebook_clicks.csv", facebookCSV, 0, ingestFacebook)
	go ingestCSV("site_events.csv", siteCSV, -1, ingestSite)
	wg.Wait()

	users, evs := snapshotRows()
	logMu.Lock()
	logs := make([]string, len(logLines))
	copy(logs, logLines)
	logMu.Unlock()

	return PipelineResult{
		Users:   users,
		Events:  evs,
		Logs:    logs,
		Summary: summarize(users, evs),
	}
}

func resetRunState() {
	usersMutex.Lock()
	trackedUsers = make(map[string]*TrackedUser)
	usersMutex.Unlock()
	eventsMutex.Lock()
	events = nil
	eventsMutex.Unlock()
	logMu.Lock()
	logLines = nil
	logMu.Unlock()
	resetSkipped()
	wg = sync.WaitGroup{}
}

func ingestCSV(label, body string, fields int, ingest func(string, [][]string)) {
	defer wg.Done()
	defer func() {
		if r := recover(); r != nil {
			pipelineLog("panic en %s: %v", label, r)
		}
	}()
	records, ok := readCSVReader(strings.NewReader(body), label, fields)
	if !ok {
		return
	}
	ingest(label, records)
}

// snapshotRows assigns ids the way loadToDatabase does on an empty file:
// users in map order starting at 1, then events whose hash was inserted.
func snapshotRows() ([]PipelineUser, []PipelineEvent) {
	users := make([]PipelineUser, 0, len(trackedUsers))
	hashToID := make(map[string]int64, len(trackedUsers))
	var next int64 = 1
	for hash, user := range trackedUsers {
		hashToID[hash] = next
		users = append(users, PipelineUser{
			TrackedUserID: next,
			UserID:        user.UserID,
			SessionKey:    user.SessionKey,
			TrackingHash:  user.TrackingHash,
			UTMChannel:    user.UTMChannel,
			Attributes:    user.Attributes,
			EarliestTime:  user.EarliestTime,
			LatestTime:    user.LatestTime,
		})
		next++
	}

	evs := make([]PipelineEvent, 0, len(events))
	var eventID int64 = 1
	for _, event := range events {
		userID, found := hashToID[event.TrackingHash]
		if !found {
			continue
		}
		evs = append(evs, PipelineEvent{
			EventID:       eventID,
			TrackedUserID: userID,
			EventTime:     event.EventTime,
			EventType:     event.EventType,
			Source:        event.Source,
			Campaign:      event.Campaign,
			Cost:          event.Cost,
			Revenue:       event.Revenue,
			Metadata:      event.Metadata,
		})
		eventID++
	}
	return users, evs
}

func summarize(users []PipelineUser, evs []PipelineEvent) PipelineSummary {
	summary := PipelineSummary{
		Users:   len(users),
		Events:  len(evs),
		Sess006: []SessionChannel{},
	}
	for _, user := range users {
		if user.UserID == "" {
			summary.EmptyUserIDs++
		}
		if user.SessionKey == "sess-006" {
			summary.Sess006 = append(summary.Sess006, SessionChannel{
				SessionKey:   user.SessionKey,
				TrackingHash: user.TrackingHash,
				UTMChannel:   user.UTMChannel,
			})
		}
	}
	sort.Slice(summary.Sess006, func(i, j int) bool {
		return summary.Sess006[i].TrackingHash < summary.Sess006[j].TrackingHash
	})
	for _, event := range evs {
		summary.Revenue += event.Revenue
		switch event.EventType {
		case "click":
			summary.Clicks++
		case "view":
			summary.Views++
		case "conversion":
			summary.Conversions++
		}
		switch event.Source {
		case "Google Ads":
			if event.EventType == "click" {
				summary.GoogleClicks++
			}
			summary.GoogleCost += event.Cost
		case "Facebook Ads":
			if event.EventType == "click" {
				summary.FacebookClicks++
			}
			summary.FacebookCost += event.Cost
		case "Website":
			summary.WebsiteEvents++
			summary.WebsiteCost += event.Cost
			if event.Metadata == "{}" {
				summary.EmptyWebsiteMetadata++
			}
		}
	}
	summary.MultiChannelConverters = multiChannelConverters(users, evs)
	summary.SkippedRows = skippedCount()
	summary.Days = dailyMetrics(evs)
	return summary
}

func dailyMetrics(evs []PipelineEvent) []DailyMetric {
	byDay := map[string]*DailyMetric{}
	for _, event := range evs {
		if len(event.EventTime) < 10 {
			continue
		}
		day := event.EventTime[:10]
		row := byDay[day]
		if row == nil {
			row = &DailyMetric{Day: day}
			byDay[day] = row
		}
		row.Cost += event.Cost
		row.Revenue += event.Revenue
		if event.EventType == "conversion" {
			row.Conversions++
		}
		if event.EventType == "click" {
			row.Clicks++
		}
	}
	days := make([]DailyMetric, 0, len(byDay))
	for _, row := range byDay {
		days = append(days, *row)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Day < days[j].Day })
	return days
}

// multiChannelConverters counts distinct users who have a conversion strictly
// after both a Google Ads click and a Facebook Ads click. That is the
// predicate in bonus_query.sql.
func multiChannelConverters(users []PipelineUser, evs []PipelineEvent) int {
	byUser := make(map[int64][]PipelineEvent, len(users))
	for _, event := range evs {
		byUser[event.TrackedUserID] = append(byUser[event.TrackedUserID], event)
	}
	n := 0
	seen := make(map[int64]struct{}, len(users))
	for _, user := range users {
		if _, ok := seen[user.TrackedUserID]; ok {
			continue
		}
		seen[user.TrackedUserID] = struct{}{}
		if conversionAfterBothAds(byUser[user.TrackedUserID]) {
			n++
		}
	}
	return n
}

func conversionAfterBothAds(evs []PipelineEvent) bool {
	for _, conv := range evs {
		if conv.EventType != "conversion" {
			continue
		}
		google := false
		facebook := false
		for _, event := range evs {
			if event.EventType != "click" || event.EventTime >= conv.EventTime {
				continue
			}
			switch event.Source {
			case "Google Ads":
				google = true
			case "Facebook Ads":
				facebook = true
			}
		}
		if google && facebook {
			return true
		}
	}
	return false
}
