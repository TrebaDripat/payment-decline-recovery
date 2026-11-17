// Package apidocs embeds the checked-in contracts served by both demo services.
package apidocs

import (
	_ "embed"
	"net/http"
)

//go:embed engine.json
var engine []byte

//go:embed provider.json
var provider []byte

func Engine(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(engine)
}

func Provider(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(provider)
}
