//go:build !js

package main

import "fmt"

func main() {
	wg.Add(3)
	go processGoogleClicks("google_clicks.csv")
	go processFacebookClicks("facebook_clicks.csv")
	go processSiteEvents("site_events.csv")
	wg.Wait()
	loadToDatabase(defaultDBPath)
	fmt.Println("ETL completado y datos cargados en marketing_data.db")
}
