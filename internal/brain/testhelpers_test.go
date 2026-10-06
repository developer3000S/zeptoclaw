package brain

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
)

// jsonWrite emits a JSON document to a test response writer.
func jsonWrite(w http.ResponseWriter, v any) error {
	return json.NewEncoder(w).Encode(v)
}

// serverHost returns the 127.0.0.1 host of an httptest server, to stand in for
// a candidate address a search engine "found".
func serverHost(srv *httptest.Server) string {
	u, err := url.Parse(srv.URL)
	if err != nil {
		return srv.URL
	}
	return u.Hostname()
}

// serverPort returns the port of an httptest server, for a search document's
// "port" field.
func serverPort(srv *httptest.Server) string {
	u, err := url.Parse(srv.URL)
	if err != nil {
		return "11434"
	}
	return u.Port()
}
