//go:build !js

package main

import (
	"database/sql"
	"log"

	_ "modernc.org/sqlite" // Pure-Go SQLite driver; no CGO.
)

// defaultDBPath is created on the first schema write when it does not exist.
const defaultDBPath = "./marketing_data.db"

// loadToDatabase creates dbPath if needed and inserts the in-memory rows
// in one transaction. A second call appends; nothing is deleted or updated.
func loadToDatabase(dbPath string) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Error al abrir base de datos: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS tracked_user (
		tracked_user_id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id TEXT,
		session_key TEXT,
		tracking_hash TEXT,
		utm_channel TEXT,
		attributes TEXT,
		earliest_time TEXT,
		latest_time TEXT
	);`)
	if err != nil {
		log.Fatalf("Error al crear tabla tracked_user: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS event (
		event_id INTEGER PRIMARY KEY AUTOINCREMENT,
		tracked_user_id INTEGER,
		event_time TEXT,
		event_type TEXT,
		source TEXT,
		campaign TEXT,
		cost REAL,
		revenue REAL,
		metadata TEXT
	);`)
	if err != nil {
		log.Fatalf("Error al crear tabla event: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Error iniciando transacción: %v", err)
	}

	userStmt, err := tx.Prepare(`INSERT INTO tracked_user (
		user_id, session_key, tracking_hash, utm_channel, attributes, earliest_time, latest_time
	) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatalf("Error preparando statement tracked_user: %v", err)
	}

	trackingHashToID := make(map[string]int64)
	for hash, user := range trackedUsers {
		res, err := userStmt.Exec(user.UserID, user.SessionKey, user.TrackingHash, user.UTMChannel, user.Attributes, user.EarliestTime, user.LatestTime)
		if err != nil {
			log.Printf("Error insertando usuario %s: %v", hash, err)
			continue
		}
		id, err := res.LastInsertId()
		if err != nil {
			log.Printf("Error obteniendo ID para usuario %s: %v", hash, err)
			continue
		}
		trackingHashToID[hash] = id
	}
	_ = userStmt.Close()

	eventStmt, err := tx.Prepare(`INSERT INTO event (
		tracked_user_id, event_time, event_type, source, campaign, cost, revenue, metadata
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatalf("Error preparando statement event: %v", err)
	}
	for _, event := range events {
		userID, found := trackingHashToID[event.TrackingHash]
		if !found {
			continue
		}
		_, err = eventStmt.Exec(userID, event.EventTime, event.EventType, event.Source, event.Campaign, event.Cost, event.Revenue, event.Metadata)
		if err != nil {
			log.Printf("Error insertando evento para usuario %s: %v", event.TrackingHash, err)
			continue
		}
	}
	_ = eventStmt.Close()

	if err := tx.Commit(); err != nil {
		log.Fatalf("Error haciendo commit en transacción: %v", err)
	}
}
