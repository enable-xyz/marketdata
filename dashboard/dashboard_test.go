package dashboard

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func newHandler(t *testing.T, basePath string) *Handler {
	t.Helper()
	handler, err := New(Config{BasePath: basePath})
	if err != nil {
		t.Fatalf("New(%q): %v", basePath, err)
	}
	return handler
}

func do(t *testing.T, handler *Handler, method, target string, header http.Header) *http.Response {
	t.Helper()
	request := httptest.NewRequest(method, target, nil)
	for name, values := range header {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Result()
}

func readBody(t *testing.T, response *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("closing body: %v", err)
	}
	return string(body)
}

func embedded(t *testing.T, name string) string {
	t.Helper()
	body, err := assets.ReadFile("assets/" + name)
	if err != nil {
		t.Fatalf("reading embedded %s: %v", name, err)
	}
	return string(body)
}

func TestNewRejectsUnusableBasePath(t *testing.T) {
	for _, basePath := range []string{"", "ui", "/ui/", "/ui/.", "/ui//panel", "/ui?x", "/ui#x", "/ui\\x", "/u i"} {
		if _, err := New(Config{BasePath: basePath}); !errors.Is(err, ErrConfiguration) {
			t.Fatalf("New(%q) error = %v, want ErrConfiguration", basePath, err)
		}
	}
}

func TestIndexServedAtCanonicalPath(t *testing.T) {
	handler := newHandler(t, "/ui")
	if handler.IndexPath() != "/ui/" || handler.BasePath() != "/ui" {
		t.Fatalf("paths = %q, %q", handler.BasePath(), handler.IndexPath())
	}
	response := do(t, handler, http.MethodGet, "/ui/", nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if got := response.Header.Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	body := readBody(t, response)
	if body != embedded(t, "index.html") {
		t.Fatal("served index does not match the embedded asset")
	}
	declared, err := strconv.Atoi(response.Header.Get("Content-Length"))
	if err != nil || declared != len(body) {
		t.Fatalf("content length = %q for %d bytes", response.Header.Get("Content-Length"), len(body))
	}
}

func TestBareBasePathRedirectsToIndex(t *testing.T) {
	handler := newHandler(t, "/ui")
	response := do(t, handler, http.MethodGet, "/ui", nil)
	if response.StatusCode != http.StatusPermanentRedirect {
		t.Fatalf("status = %d, want 308", response.StatusCode)
	}
	if got := response.Header.Get("Location"); got != "/ui/" {
		t.Fatalf("location = %q", got)
	}
	if got := response.Header.Get("Content-Security-Policy"); got != ContentSecurityPolicy {
		t.Fatalf("redirect lost the policy header: %q", got)
	}
}

func TestRootBasePathServesOrigin(t *testing.T) {
	handler := newHandler(t, "/")
	if handler.IndexPath() != "/" {
		t.Fatalf("index path = %q", handler.IndexPath())
	}
	if response := do(t, handler, http.MethodGet, "/", nil); response.StatusCode != http.StatusOK {
		t.Fatalf("index status = %d", response.StatusCode)
	}
	if response := do(t, handler, http.MethodGet, "/"+FileScript, nil); response.StatusCode != http.StatusOK {
		t.Fatalf("script status = %d", response.StatusCode)
	}
}

func TestAssetContentTypesAndPaths(t *testing.T) {
	handler := newHandler(t, "/ui")
	want := map[string]string{
		"/ui/" + FileStyle:  "text/css; charset=utf-8",
		"/ui/" + FileScript: "text/javascript; charset=utf-8",
		"/ui/" + FileIcon:   "image/svg+xml",
	}
	for target, contentType := range want {
		response := do(t, handler, http.MethodGet, target, nil)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", target, response.StatusCode)
		}
		if got := response.Header.Get("Content-Type"); got != contentType {
			t.Fatalf("%s content type = %q, want %q", target, got, contentType)
		}
		if response.Header.Get("ETag") == "" {
			t.Fatalf("%s has no ETag", target)
		}
		readBody(t, response)
	}
	paths := handler.Paths()
	for _, target := range []string{"/ui/", "/ui/" + FileStyle, "/ui/" + FileScript, "/ui/" + FileIcon} {
		if !strings.Contains(strings.Join(paths, " "), target) {
			t.Fatalf("Paths() = %v, missing %q", paths, target)
		}
	}
	if len(paths) != 4 {
		t.Fatalf("Paths() = %v, want exactly the four asset routes", paths)
	}
}

func TestSecurityHeadersOnEveryResponse(t *testing.T) {
	handler := newHandler(t, "/ui")
	targets := []string{"/ui/", "/ui/" + FileStyle, "/ui/" + FileScript, "/ui/missing", "/ui"}
	for _, target := range targets {
		response := do(t, handler, http.MethodGet, target, nil)
		for name, want := range map[string]string{
			"Content-Security-Policy":      ContentSecurityPolicy,
			"Permissions-Policy":           PermissionsPolicy,
			"X-Content-Type-Options":       "nosniff",
			"X-Frame-Options":              "DENY",
			"Referrer-Policy":              "no-referrer",
			"Cross-Origin-Opener-Policy":   "same-origin",
			"Cross-Origin-Resource-Policy": "same-origin",
			"Cross-Origin-Embedder-Policy": "require-corp",
		} {
			if got := response.Header.Get(name); got != want {
				t.Fatalf("%s %s = %q, want %q", target, name, got, want)
			}
		}
		readBody(t, response)
	}
	if strings.Contains(ContentSecurityPolicy, "unsafe-inline") || strings.Contains(ContentSecurityPolicy, "unsafe-eval") ||
		!strings.Contains(ContentSecurityPolicy, "default-src 'none'") ||
		!strings.Contains(ContentSecurityPolicy, "connect-src 'self'") ||
		!strings.Contains(ContentSecurityPolicy, "frame-ancestors 'none'") {
		t.Fatalf("policy is not restrictive enough: %q", ContentSecurityPolicy)
	}
}

func TestCacheRevalidationAndConditionalRequest(t *testing.T) {
	handler := newHandler(t, "/ui")
	first := do(t, handler, http.MethodGet, "/ui/"+FileScript, nil)
	control := first.Header.Get("Cache-Control")
	if control != CacheControl || !strings.Contains(control, "no-cache") {
		t.Fatalf("cache control = %q, want revalidating %q", control, CacheControl)
	}
	etag := first.Header.Get("ETag")
	readBody(t, first)

	header := http.Header{"If-None-Match": []string{etag}}
	second := do(t, handler, http.MethodGet, "/ui/"+FileScript, header)
	if second.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional status = %d, want 304", second.StatusCode)
	}
	if second.Header.Get("ETag") != etag {
		t.Fatalf("304 dropped the ETag")
	}
	if body := readBody(t, second); body != "" {
		t.Fatalf("304 carried %d body bytes", len(body))
	}

	weak := http.Header{"If-None-Match": []string{"W/" + etag}}
	if response := do(t, handler, http.MethodGet, "/ui/"+FileScript, weak); response.StatusCode != http.StatusNotModified {
		t.Fatalf("weak validator status = %d, want 304", response.StatusCode)
	}
	stale := http.Header{"If-None-Match": []string{`"stale"`}}
	if response := do(t, handler, http.MethodGet, "/ui/"+FileScript, stale); response.StatusCode != http.StatusOK {
		t.Fatalf("stale validator status = %d, want 200", response.StatusCode)
	}
}

func TestETagIsContentDerived(t *testing.T) {
	first := newHandler(t, "/ui")
	second := newHandler(t, "/other")
	firstResponse := do(t, first, http.MethodGet, "/ui/"+FileStyle, nil)
	secondResponse := do(t, second, http.MethodGet, "/other/"+FileStyle, nil)
	if firstResponse.Header.Get("ETag") != secondResponse.Header.Get("ETag") {
		t.Fatal("ETag varies with mount point; it must derive from content alone")
	}
	indexResponse := do(t, first, http.MethodGet, "/ui/", nil)
	if indexResponse.Header.Get("ETag") == firstResponse.Header.Get("ETag") {
		t.Fatal("distinct assets share an ETag")
	}
	readBody(t, firstResponse)
	readBody(t, secondResponse)
	readBody(t, indexResponse)
}

func TestHeadOmitsBody(t *testing.T) {
	handler := newHandler(t, "/ui")
	response := do(t, handler, http.MethodHead, "/ui/", nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if response.Header.Get("Content-Length") == "" {
		t.Fatal("HEAD lost Content-Length")
	}
	if body := readBody(t, response); body != "" {
		t.Fatalf("HEAD returned %d body bytes", len(body))
	}
}

func TestRejectedRequests(t *testing.T) {
	handler := newHandler(t, "/ui")
	post := do(t, handler, http.MethodPost, "/ui/", nil)
	if post.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d", post.StatusCode)
	}
	if got := post.Header.Get("Allow"); got != "GET, HEAD" {
		t.Fatalf("Allow = %q", got)
	}
	if body := readBody(t, post); body != `{"error":"method_not_allowed"}`+"\n" {
		t.Fatalf("POST body = %q", body)
	}

	query := do(t, handler, http.MethodGet, "/ui/?token=leak", nil)
	if query.StatusCode != http.StatusBadRequest {
		t.Fatalf("query status = %d", query.StatusCode)
	}
	if body := readBody(t, query); body != `{"error":"invalid_request"}`+"\n" {
		t.Fatalf("query body = %q", body)
	}

	missing := do(t, handler, http.MethodGet, "/ui/nope.js", nil)
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing status = %d", missing.StatusCode)
	}
	if got := missing.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("missing content type = %q", got)
	}
	if body := readBody(t, missing); body != `{"error":"not_found"}`+"\n" {
		t.Fatalf("missing body = %q", body)
	}

	outside := do(t, handler, http.MethodGet, "/v1/catalog/sources", nil)
	if outside.StatusCode != http.StatusNotFound {
		t.Fatalf("outside status = %d", outside.StatusCode)
	}
	readBody(t, outside)
}

func TestShellCarriesStableLandmarks(t *testing.T) {
	shell := embedded(t, "index.html")
	for _, landmark := range []string{
		"app-shell", "banner", "nav", "tape-strip", "tape-rail", "auth-panel", "token-form", "token-input",
		"token-remember", "token-submit", "token-clear", "auth-state", "session-chip", "refresh", "auto-refresh",
		"refresh-interval", "view-overview", "view-catalog", "view-coverage", "view-datasets", "view-query",
		"view-telemetry", "nav-overview", "nav-catalog", "nav-coverage", "nav-datasets", "nav-query", "nav-telemetry",
		"verdict-freshness", "verdict-continuity", "verdict-projection", "verdict-freshness-word",
		"verdict-freshness-evidence", "table-sources", "table-instruments", "table-coverage", "table-incidents",
		"table-datasets", "table-query", "table-query-refs", "table-telemetry", "status-sources", "status-coverage",
		"status-datasets", "status-query", "status-telemetry", "count-sources", "query-form", "query-run",
		"query-next", "query-reset", "query-family", "query-dataset", "query-sources", "query-start", "query-end",
		"query-limit", "query-family-field", "query-dataset-field", "panel-query-result", "panel-query-form",
		"instrument-filter", "metric-filter", "shortcuts", "tape-note", "readiness-kv", "estate-kv",
	} {
		if !strings.Contains(shell, `data-testid="`+landmark+`"`) {
			t.Fatalf("shell is missing stable landmark %q", landmark)
		}
	}
	for _, semantic := range []string{
		"<header class=\"banner\"", "<nav ", "<main id=\"main\"", "<footer", "<noscript>", "<caption",
		`scope="col"`, `role="status"`, `<label for="token"`, "<fieldset", "<legend", "class=\"skip\"",
		`aria-labelledby=`, `<html lang="en">`,
	} {
		if !strings.Contains(shell, semantic) {
			t.Fatalf("shell is missing required semantic markup %q", semantic)
		}
	}
	if strings.Count(shell, "<script") != 1 || !strings.Contains(shell, `<script src="app.js" defer></script>`) {
		t.Fatal("shell must load exactly one same-origin script and carry no inline script")
	}
	if strings.Contains(shell, "<style") {
		t.Fatal("shell must not carry an inline style block")
	}
	if inline := regexp.MustCompile(`(?i)\son[a-z]+\s*=`); inline.MatchString(shell) {
		t.Fatal("shell carries an inline event handler attribute")
	}
}

func TestBearerGuidanceUsesCSPRNGBoundary(t *testing.T) {
	shell := embedded(t, "index.html")
	if !strings.Contains(shell, `minlength="32"`) ||
		!strings.Contains(shell, "lossless text encoding of at least 32 CSPRNG bytes") {
		t.Fatal("shell must require bearer material generated from at least 32 CSPRNG bytes")
	}
	script := embedded(t, FileScript)
	if !strings.Contains(script, "tokenMin: 32,") {
		t.Fatal("client bearer boundary must match the 32-byte server minimum")
	}
}

func TestShellDisclosesNoDeploymentState(t *testing.T) {
	for _, name := range []string{"index.html", FileStyle, FileScript, FileIcon} {
		// The SVG namespace is a required XML identifier, not a fetched URL.
		content := strings.ReplaceAll(embedded(t, name), `xmlns="http://www.w3.org/2000/svg"`, "")
		for _, banned := range []string{
			"http://", "https://www", "0.0.0.0", ".amazonaws.", ".internal",
			"cdn.", "fonts.", "Bearer ey", "/Users/", "/home/", "postgres://", "clickhouse://", "s3://",
		} {
			if strings.Contains(content, banned) {
				t.Fatalf("%s contains %q, which can disclose deployment or remote state", name, banned)
			}
		}
		if strings.Contains(content, "@import") || strings.Contains(content, "integrity=") {
			t.Fatalf("%s references an external asset", name)
		}
		if address := regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`); address.MatchString(content) {
			t.Fatalf("%s embeds what looks like an address or account identity", name)
		}
	}
	// The only absolute URL literal permitted anywhere is the SVG namespace.
	icon := embedded(t, FileIcon)
	if strings.Count(icon, "http") != 1 || !strings.Contains(icon, `xmlns="http://www.w3.org/2000/svg"`) {
		t.Fatal("icon must reference only the SVG namespace")
	}
}

func TestScriptTokenHandlingIsSafe(t *testing.T) {
	script := embedded(t, FileScript)
	for _, banned := range []string{
		"localStorage", "document.cookie", "console.", "eval(", "innerHTML", "outerHTML", "insertAdjacentHTML",
		"document.write", "new Function", "location.search", "URLSearchParams", "?token", "token=",
		"XMLHttpRequest", "navigator.sendBeacon", "importScripts",
	} {
		if strings.Contains(script, banned) {
			t.Fatalf("script contains %q", banned)
		}
	}
	for _, required := range []string{
		`Authorization: "Bearer " + token`, "sessionStorage", `credentials: "omit"`, `mode: "same-origin"`,
		`redirect: "error"`, `cache: "no-store"`, `referrerPolicy: "no-referrer"`, "textContent",
		"function clearToken", "sessionDrop(SESSION_TOKEN_KEY)", `window.location.protocol === "https:"`, "window.isSecureContext !== true",
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("script is missing required behaviour %q", required)
		}
	}
	if strings.Count(script, "Bearer ") != 1 {
		t.Fatal("the token must be attached in exactly one place")
	}
	if strings.Count(script, "fetch(") != 1 || !strings.Contains(script, "new URL(pathname, window.location.origin)") {
		t.Fatal("every request must flow through the single audited fetch call against this origin")
	}
	// The only host literals permitted are the loopback names the secure-context
	// gate recognises; nothing may name a deployment host.
	if strings.Count(script, "127.0.0.1") != 1 || strings.Count(script, "localhost") != 1 {
		t.Fatal("script carries an unexpected host literal")
	}
}

func TestScriptRenderingIsBounded(t *testing.T) {
	script := embedded(t, FileScript)
	for _, bound := range []string{
		"sourceRows:", "instrumentRows:", "coverageRows:", "incidentRows:", "datasetRows:", "queryRows:",
		"metricRows:", "tapeCells:", "responseBytes:", "metricChars:",
	} {
		if !strings.Contains(script, bound) {
			t.Fatalf("script is missing render bound %q", bound)
		}
	}
	if !strings.Contains(script, "function boundNote") || !strings.Contains(script, "client bound") {
		t.Fatal("script must report when it truncates a result set")
	}
	if strings.Count(script, ".slice(0, BOUNDS.") < 7 {
		t.Fatal("render paths must slice against the declared bounds")
	}
}

func TestStyleSupportsAccessibilityAndLayout(t *testing.T) {
	style := embedded(t, FileStyle)
	for _, required := range []string{
		"prefers-reduced-motion: reduce", "prefers-color-scheme: light", "focus-visible", ".visually-hidden",
		"@media (max-width: 900px)", "overflow-x: auto", "color-scheme: dark light",
		// Views and conditional fields are toggled with the hidden attribute, so
		// a layout display rule must never be able to override it.
		"[hidden] {\n  display: none !important;\n}",
	} {
		if !strings.Contains(style, required) {
			t.Fatalf("stylesheet is missing %q", required)
		}
	}
	for _, banned := range []string{"backdrop-filter", "url(", "@font-face", "cubic-bezier(0.68"} {
		if strings.Contains(style, banned) {
			t.Fatalf("stylesheet contains %q", banned)
		}
	}
	if strings.Contains(style, "#000000") || strings.Contains(style, "#ffffff") ||
		strings.Contains(style, "#fff;") || strings.Contains(style, "#000;") {
		t.Fatal("stylesheet uses an untinted pure black or white")
	}
}

func TestNilHandlerFailsClosed(t *testing.T) {
	var handler *Handler
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ui/", nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}
}
