//go:build js && wasm

package main

import (
	"encoding/json"
	"fmt"
	"syscall/js"
)

// main registers runMarketingETL and stays alive. The browser calls it with
// the three CSV texts. SQLite is not linked into this build.
func main() {
	js.Global().Set("runMarketingETL", js.FuncOf(func(this js.Value, args []js.Value) (out any) {
		defer func() {
			if r := recover(); r != nil {
				body, err := json.Marshal(map[string]string{"error": fmt.Sprint(r)})
				if err != nil {
					out = `{"error":"panic"}`
					return
				}
				out = string(body)
			}
		}()
		if len(args) < 3 {
			return `{"error":"se necesitan tres CSV"}`
		}
		result := runPipeline(args[0].String(), args[1].String(), args[2].String())
		body, err := json.Marshal(result)
		if err != nil {
			return `{"error":"no se pudo serializar el resultado"}`
		}
		return string(body)
	}))
	select {}
}
