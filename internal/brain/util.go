package brain

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
)

// urlQuery percent-encodes a value for a URL path segment or query parameter.
// Search APIs are strict about characters like ':' and '/' in a query string.
func urlQuery(s string) string {
	return url.QueryEscape(s)
}

// basicAuth builds an RFC 7617 Authorization header value for the APIs that
// still take an id:secret pair.
func basicAuth(id, secret string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(id+":"+secret))
}

// MarshalJSON serialises a value as JSON. It is a package-local alias so that
// the gossip and catalog packages need not each import encoding/json.
func MarshalJSON(v any) ([]byte, error) { return json.Marshal(v) }

// UnmarshalJSON deserialises JSON into v. It is a package-local alias.
func UnmarshalJSON(data []byte, v any) error { return json.Unmarshal(data, v) }
