// Command go-marketing-etl-pipeline reads three marketing CSVs, deduplicates
// visitors in memory, and, on a native build, loads them into a SQLite file.
// The js/wasm build does not link SQLite: modernc.org/libc has no GOOS=js files.
package main

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TrackedUser is one visitor after records that share a tracking hash are merged.
type TrackedUser struct {
	UserID       string // Empty string when no source supplied an id. Filled in later only while still empty.
	SessionKey   string
	TrackingHash string
	UTMChannel   string // Channel from the worker that created the row. Later rows do not replace it.
	Attributes   string // Always "{}" in this pipeline.
	EarliestTime string // Minimum event timestamp, compared as text.
	LatestTime   string // Maximum event timestamp, compared as text.
}

// Event is one click or site action. TrackingHash links it to a user until load,
// when it is stored under that user's new tracked_user_id.
type Event struct {
	TrackingHash string
	EventTime    string
	EventType    string
	Source       string
	Campaign     string
	Cost         float64
	Revenue      float64
	Metadata     string
}

// Shared pipeline state. File workers run concurrently; load runs only after they finish.
var (
	usersMutex   sync.RWMutex
	trackedUsers = make(map[string]*TrackedUser)
	eventsMutex  sync.Mutex
	events       []Event
	wg           sync.WaitGroup
	logMu        sync.Mutex
	logLines     []string
	skipMu       sync.Mutex
	skippedRows  int
)

// pipelineLog writes the same line the CLI already printed and keeps a copy
// for the browser result. Workers call it without holding the user or event lock.
func pipelineLog(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Print(msg)
	logMu.Lock()
	logLines = append(logLines, msg)
	logMu.Unlock()
}

// skipRow logs a rejected data row and counts it. The native load used to
// panic when an ad row was shorter than the indexes it reads.
func skipRow(filePath, reason string) {
	pipelineLog("Skipping row en %s: %s", filePath, reason)
	skipMu.Lock()
	skippedRows++
	skipMu.Unlock()
}

func skippedCount() int {
	skipMu.Lock()
	defer skipMu.Unlock()
	return skippedRows
}

func resetSkipped() {
	skipMu.Lock()
	skippedRows = 0
	skipMu.Unlock()
}

// generateTrackingHash is hex(SHA-256(sessionKey + deviceType)) with no separator.
func generateTrackingHash(sessionKey, deviceType string) string {
	sum := sha256.Sum256([]byte(sessionKey + deviceType))
	return hex.EncodeToString(sum[:])
}

// parseTimestamp accepts "YYYY-MM-DD HH:MM:SS" and formats it in UTC.
// The layout has no zone, so time.Parse already treats the clock time as UTC.
func parseTimestamp(ts string) (string, error) {
	t, err := time.Parse("2006-01-02 15:04:05", ts)
	if err != nil {
		return "", err
	}
	return t.UTC().Format("2006-01-02 15:04:05"), nil
}

// readCSV opens path and returns every record. fieldsPerRecord 0 requires a
// consistent column count; -1 allows the ragged website rows.
// The bool is false after a logged open or parse error.
func readCSV(filePath string, fieldsPerRecord int) ([][]string, bool) {
	file, err := os.Open(filePath)
	if err != nil {
		pipelineLog("Error al abrir archivo %s: %v", filePath, err)
		return nil, false
	}
	defer file.Close()
	return readCSVReader(file, filePath, fieldsPerRecord)
}

func readCSVReader(r io.Reader, filePath string, fieldsPerRecord int) ([][]string, bool) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = fieldsPerRecord
	records, err := reader.ReadAll()
	if err != nil {
		pipelineLog("Error al leer CSV %s: %v", filePath, err)
		return nil, false
	}
	return records, true
}

// prepareRow validates the columns shared by every source and returns the tracking hash.
// Rows with a bad timestamp or an empty device_type are skipped.
func prepareRow(filePath string, record []string) (ts, sessionKey, deviceType, hash string, ok bool) {
	ts, err := parseTimestamp(record[0])
	if err != nil {
		skipRow(filePath, "invalid timestamp")
		return "", "", "", "", false
	}
	sessionKey = record[2]
	deviceType = record[3]
	if deviceType == "" {
		skipRow(filePath, "missing device_type")
		return "", "", "", "", false
	}
	return ts, sessionKey, deviceType, generateTrackingHash(sessionKey, deviceType), true
}

// upsertTrackedUser inserts a visitor or merges this row into the existing one.
// user_id is written only when the stored id is still empty. utm_channel is the
// channel on the earliest timestamp. Equal timestamps keep the channel that
// sorts first, so the result does not depend on which goroutine runs first.
// earliest_time and latest_time expand to include ts.
func upsertTrackedUser(hash, userID, sessionKey, utmChannel, ts string) {
	usersMutex.Lock()
	defer usersMutex.Unlock()

	if existing, found := trackedUsers[hash]; found {
		if existing.UserID == "" && userID != "" {
			existing.UserID = userID
		}
		if ts < existing.EarliestTime || (ts == existing.EarliestTime && utmChannel < existing.UTMChannel) {
			existing.EarliestTime = ts
			existing.UTMChannel = utmChannel
		}
		if ts > existing.LatestTime {
			existing.LatestTime = ts
		}
		return
	}

	trackedUsers[hash] = &TrackedUser{
		UserID:       userID,
		SessionKey:   sessionKey,
		TrackingHash: hash,
		UTMChannel:   utmChannel,
		Attributes:   "{}",
		EarliestTime: ts,
		LatestTime:   ts,
	}
}

func appendEvent(event Event) {
	eventsMutex.Lock()
	events = append(events, event)
	eventsMutex.Unlock()
}

func parseCost(raw string) float64 {
	cost, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return cost
}

func marshalJSON(v any) string {
	encoded, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

// processGoogleClicks reads a Google Ads click export.
// Column 4 is the campaign. Metadata stores that same value under ad_group;
// the ad_group column (index 5) is not read.
func processGoogleClicks(filePath string) {
	defer wg.Done()
	records, ok := readCSV(filePath, -1)
	if !ok {
		return
	}
	ingestGoogle(filePath, records)
}

func ingestGoogle(filePath string, records [][]string) {
	for i, record := range records {
		if i == 0 {
			continue
		}
		if len(record) < 9 {
			skipRow(filePath, "columnas insuficientes")
			continue
		}
		ts, sessionKey, _, hash, ok := prepareRow(filePath, record)
		if !ok {
			continue
		}
		upsertTrackedUser(hash, record[1], sessionKey, record[7], ts)

		meta := map[string]string{
			"ad_group": record[4],
			"gclid":    record[8],
		}
		appendEvent(Event{
			TrackingHash: hash,
			EventTime:    ts,
			EventType:    "click",
			Source:       "Google Ads",
			Campaign:     record[4],
			Cost:         parseCost(record[6]),
			Revenue:      0,
			Metadata:     marshalJSON(meta),
		})
	}
}

// processFacebookClicks reads a Facebook Ads click export.
// Column 4 is the campaign. Metadata stores that same value under ad_set;
// the ad_set column (index 5) is not read.
func processFacebookClicks(filePath string) {
	defer wg.Done()
	records, ok := readCSV(filePath, -1)
	if !ok {
		return
	}
	ingestFacebook(filePath, records)
}

func ingestFacebook(filePath string, records [][]string) {
	for i, record := range records {
		if i == 0 {
			continue
		}
		if len(record) < 9 {
			skipRow(filePath, "columnas insuficientes")
			continue
		}
		ts, sessionKey, _, hash, ok := prepareRow(filePath, record)
		if !ok {
			continue
		}
		upsertTrackedUser(hash, record[1], sessionKey, record[7], ts)

		meta := map[string]string{
			"ad_set": record[4],
			"fbclid": record[8],
		}
		appendEvent(Event{
			TrackingHash: hash,
			EventTime:    ts,
			EventType:    "click",
			Source:       "Facebook Ads",
			Campaign:     record[4],
			Cost:         parseCost(record[6]),
			Revenue:      0,
			Metadata:     marshalJSON(meta),
		})
	}
}

// processSiteEvents reads website events. action_details sometimes splits on
// commas, so extra fields are joined back and the last field stays utm_channel.
func processSiteEvents(filePath string) {
	defer wg.Done()
	records, ok := readCSV(filePath, -1)
	if !ok {
		return
	}
	ingestSite(filePath, records)
}

func ingestSite(filePath string, records [][]string) {
	for i, record := range records {
		if i == 0 {
			continue
		}
		rec, ok := normalizeSiteRecord(filePath, record)
		if !ok {
			continue
		}
		ts, sessionKey, _, hash, ok := prepareRow(filePath, rec)
		if !ok {
			continue
		}
		upsertTrackedUser(hash, rec[1], sessionKey, rec[6], ts)

		actionDetails := map[string]any{}
		if err := json.Unmarshal([]byte(rec[5]), &actionDetails); err != nil {
			actionDetails = map[string]any{}
		}
		actionType := rec[4]
		eventType := actionType
		if actionType == "purchase" || actionType == "signup" {
			eventType = "conversion"
		}
		appendEvent(Event{
			TrackingHash: hash,
			EventTime:    ts,
			EventType:    eventType,
			Source:       "Website",
			Campaign:     "",
			Cost:         0,
			Revenue:      websiteRevenue(actionType, actionDetails),
			Metadata:     marshalJSON(actionDetails),
		})
	}
}

// normalizeSiteRecord returns seven columns. When action_details was split,
// columns 5 through n-2 are joined and the last column is utm_channel.
func normalizeSiteRecord(filePath string, record []string) ([]string, bool) {
	if len(record) < 7 {
		skipRow(filePath, "número insuficiente de campos")
		return nil, false
	}
	if len(record) == 7 {
		return record, true
	}
	rec := make([]string, 7)
	rec[0] = record[0]
	rec[1] = record[1]
	rec[2] = record[2]
	rec[3] = record[3]
	rec[4] = record[4]
	rec[5] = strings.Join(record[5:len(record)-1], ",")
	rec[6] = record[len(record)-1]
	return rec, true
}

// websiteRevenue reads action_details.value for purchase and signup only.
// JSON numbers arrive as float64. Unparseable strings contribute 0.
func websiteRevenue(actionType string, actionDetails map[string]any) float64 {
	if actionType != "purchase" && actionType != "signup" {
		return 0
	}
	val, found := actionDetails["value"]
	if !found {
		return 0
	}
	switch typed := val.(type) {
	case float64:
		return typed
	case string:
		revenue, _ := strconv.ParseFloat(typed, 64)
		return revenue
	default:
		return 0
	}
}
