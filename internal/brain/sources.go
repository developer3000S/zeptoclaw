package brain

import (
	"bytes"
	"context"
	"fmt"
)

// rawItem is a search engine's report about one host, before it is folded into
// a Candidate. Its fields mirror the TS reference implementation
// (fetchFromCensys/Shodan/...) one for one, so the two catalogs agree on what
// an engine reported even when the engine's document shape drifts.
type rawItem struct {
	IP          string
	Port        int
	Protocol    string
	DNSNames    []string
	Country     string
	ASN         string
	Source      string
	ServiceHint string
	Banner      string
}

const ollamaPort = 11434

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// --- Censys ---------------------------------------------------------------

// fetchCensys queries the Censys Platform v3 search API, falling back to the v2
// Hosts API when only an id/secret pair is available.
func (c *Catalog) fetchCensys(ctx context.Context) ([]rawItem, error) {
	if c.keys.CensysToken == "" {
		return nil, nil
	}
	var items []rawItem
	var lastErr error

	// v3 Platform Search.
	data, err := c.postJSON(ctx,
		c.endpoints.Censys,
		map[string]any{"query": c.cfg.Query, "per_page": c.cfg.MaxPerSource},
		map[string]string{"Authorization": "Bearer " + c.keys.CensysToken, "Accept": "application/json"})
	if err == nil {
		for _, h := range hits(data) {
			ip := firstNonEmpty(asStr(h["ip"]), asStr(h["host_id"]), asStr(h["query_target"]))
			if ip == "" {
				continue
			}
			items = append(items, rawItem{
				IP:       ip,
				Port:     ollamaPort,
				Protocol: "tcp",
				DNSNames: concat(asStrSlice(asObj(h["dns"])["names"]), asStrSlice(h["names"])),
				Country:  orDefault(asStr(asObj(h["location"])["country_code"]), "US"),
				ASN:      censysASN(h),
				Source:   "censys",
			})
		}
	} else {
		lastErr = err
	}

	// v2 Hosts fallback.
	if c.keys.CensysID != "" && c.keys.CensysSecret != "" {
		data, err = c.postJSON(ctx,
			c.endpoints.CensysV2+"?q="+urlQuery(c.cfg.Query)+"&per_page="+fmt.Sprint(c.cfg.MaxPerSource),
			nil,
			map[string]string{"Authorization": basicAuth(c.keys.CensysID, c.keys.CensysSecret), "Accept": "application/json"})
		if err == nil {
			for _, h := range hits(data) {
				ip := asStr(h["ip"])
				if ip == "" {
					continue
				}
				items = append(items, rawItem{
					IP:       ip,
					Port:     ollamaPort,
					Protocol: "tcp",
					DNSNames: asStrSlice(asObj(h["dns"])["names"]),
					Country:  orDefault(asStr(asObj(h["location"])["country_code"]), "US"),
					ASN:      censysASN(h),
					Source:   "censys",
				})
			}
		} else {
			lastErr = err
		}
	}

	if len(items) > 0 {
		return items, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return items, nil
}

func censysASN(h map[string]any) string {
	n := asInt(asObj(h["autonomous_system"])["asn"])
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("AS%d", n)
}

// hits returns the result list of a search document across the shapes the
// engines have used: result.hits, top-level hits, or results.
func hits(data map[string]any) []map[string]any {
	if data == nil {
		return nil
	}
	if r := asObj(data["result"]); r != nil {
		if hs := objSlice(r["hits"]); len(hs) > 0 {
			return hs
		}
	}
	if hs := objSlice(data["hits"]); len(hs) > 0 {
		return hs
	}
	return objSlice(data["results"])
}

func objSlice(v any) []map[string]any {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(arr))
	for _, e := range arr {
		if m := asObj(e); m != nil {
			out = append(out, m)
		}
	}
	return out
}

// --- Shodan ---------------------------------------------------------------

func (c *Catalog) fetchShodan(ctx context.Context) ([]rawItem, error) {
	data, err := c.getJSON(ctx,
		c.endpoints.Shodan+"?key="+urlQuery(c.keys.Shodan)+"&query="+urlQuery(c.cfg.Query),
		map[string]string{"Accept": "application/json"})
	if err != nil {
		return nil, err
	}
	var items []rawItem
	for _, m := range objSlice(data["matches"]) {
		ip := firstNonEmpty(asStr(m["ip_str"]), asStr(m["ip"]))
		if ip == "" {
			continue
		}
		port := asInt(m["port"])
		if port == 0 {
			port = ollamaPort
		}
		items = append(items, rawItem{
			IP:          ip,
			Port:        port,
			Protocol:    orDefault(asStr(m["transport"]), "tcp"),
			DNSNames:    asStrSlice(m["hostnames"]),
			Country:     orDefault(asStr(asObj(m["location"])["country_code"]), "US"),
			ASN:         asStr(m["asn"]),
			Source:      "shodan",
			ServiceHint: "ollama",
			Banner:      truncateStr(asStr(m["data"]), 200),
		})
	}
	return items, nil
}

// --- GreyNoise ------------------------------------------------------------

func (c *Catalog) fetchGreyNoise(ctx context.Context) ([]rawItem, error) {
	data, err := c.getJSON(ctx,
		c.endpoints.GreyNoise+"?query="+urlQuery(c.cfg.Query)+"&size="+fmt.Sprint(c.cfg.MaxPerSource),
		map[string]string{"key": c.keys.GreyNoise, "Accept": "application/json"})
	if err != nil {
		return nil, err
	}
	var items []rawItem
	for _, r := range objSlice(data["data"]) {
		ip := asStr(r["ip"])
		if ip == "" {
			continue
		}
		meta := asObj(r["metadata"])
		items = append(items, rawItem{
			IP:       ip,
			Port:     ollamaPort,
			Protocol: "tcp",
			DNSNames: prependIf(meta["rdns"]),
			Country:  orDefault(asStr(meta["country_code"]), "US"),
			ASN:      asStr(meta["asn"]),
			Source:   "greynoise",
		})
	}
	return items, nil
}

// --- ZoomEye --------------------------------------------------------------

func (c *Catalog) fetchZoomEye(ctx context.Context) ([]rawItem, error) {
	data, err := c.getJSON(ctx,
		c.endpoints.ZoomEye+"?query="+urlQuery(c.cfg.Query)+"&page=1",
		map[string]string{"API-KEY": c.keys.ZoomEye, "Accept": "application/json"})
	if err != nil {
		return nil, err
	}
	var items []rawItem
	for _, m := range objSlice(data["matches"]) {
		ip := asStr(m["ip"])
		if ip == "" {
			continue
		}
		portinfo := asObj(m["portinfo"])
		port := asInt(portinfo["port"])
		if port == 0 {
			port = ollamaPort
		}
		geo := asObj(asObj(m["geoinfo"])["country"])
		items = append(items, rawItem{
			IP:       ip,
			Port:     port,
			Protocol: orDefault(asStr(portinfo["service"]), "tcp"),
			DNSNames: prependIf(m["rdns"]),
			Country:  orDefault(asStr(geo["code"]), "US"),
			ASN:      zoomEyeASN(m),
			Source:   "zoomeye",
		})
	}
	return items, nil
}

func zoomEyeASN(m map[string]any) string {
	n := asInt(asObj(asObj(m["geoinfo"])["asn"])["asn"])
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("AS%d", n)
}

// --- Criminal IP ----------------------------------------------------------

func (c *Catalog) fetchCriminalIP(ctx context.Context) ([]rawItem, error) {
	data, err := c.getJSON(ctx,
		c.endpoints.CriminalIP+"?query="+urlQuery(c.cfg.Query)+"&offset=0",
		map[string]string{"x-api-key": c.keys.CriminalIP, "Accept": "application/json"})
	if err != nil {
		return nil, err
	}
	// data["data"] is the outer map; ["result"] is the result array itself,
	// which must not be wrapped in asObj (it only accepts JSON objects, not arrays).
	list := objSlice(asObj(data["data"])["result"])
	var items []rawItem
	for _, r := range list {
		ip := asStr(r["ip_address"])
		if ip == "" {
			continue
		}
		port := asInt(r["open_port_no"])
		if port == 0 {
			port = ollamaPort
		}
		items = append(items, rawItem{
			IP:       ip,
			Port:     port,
			Protocol: "tcp",
			DNSNames: prependIf(r["hostname"]),
			Country:  orDefault(asStr(r["country"]), "US"),
			ASN:      asStr(r["as_name"]),
			Source:   "criminal_ip",
		})
	}
	return items, nil
}

// --- Netlas ---------------------------------------------------------------

func (c *Catalog) fetchNetlas(ctx context.Context) ([]rawItem, error) {
	base := c.keys.NetlasEndpoint
	if base == "" {
		base = c.endpoints.Netlas
	}
	// Trim a trailing slash so the join below does not double it.
	for len(base) > 0 && base[len(base)-1] == '/' {
		base = base[:len(base)-1]
	}
	data, err := c.getJSON(ctx,
		base+"/responses/?q="+urlQuery(c.cfg.Query)+"&start=0",
		map[string]string{"X-Api-Key": c.keys.Netlas, "Accept": "application/json"})
	if err != nil {
		return nil, err
	}
	var items []rawItem
	for _, item := range objSlice(data["items"]) {
		d := asObj(item["data"])
		ip := asStr(d["ip"])
		if ip == "" {
			continue
		}
		port := asInt(d["port"])
		if port == 0 {
			port = ollamaPort
		}
		items = append(items, rawItem{
			IP:          ip,
			Port:        port,
			Protocol:    orDefault(firstNonEmpty(asStr(d["prot4"]), asStr(d["protocol"])), "tcp"),
			DNSNames:    netlasDNS(d),
			Country:     orDefault(firstNonEmpty(asStr(asObj(d["geo"])["country"]), asStr(d["country"])), "US"),
			ASN:         netlasASN(d),
			Source:      "netlas",
			ServiceHint: "ollama",
			Banner:      truncateStr(asStr(asObj(d["http"])["body"]), 200),
		})
	}
	return items, nil
}

func netlasDNS(d map[string]any) []string {
	var out []string
	for _, k := range []string{"host", "domain", "ptr"} {
		if s := asStr(d[k]); s != "" {
			out = union(out, s)
		}
	}
	return out
}

func netlasASN(d map[string]any) string {
	nums := asStrSlice(asObj(asObj(d["whois"])["asn"])["number"])
	if len(nums) > 0 {
		return "AS" + nums[0]
	}
	if s := asStr(d["asn"]); s != "" {
		return "AS" + s
	}
	return ""
}

// --- small helpers --------------------------------------------------------

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// prependIf turns a single optional name into the one-element DNS list the
// candidate shape expects.
func prependIf(v any) []string {
	s := asStr(v)
	if s == "" {
		return nil
	}
	return []string{s}
}

func concat(a, b []string) []string {
	return union(a, b...)
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
