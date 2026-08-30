// Package dashboard serves the embedded, same-origin operator dashboard for
// market-data operations. The package holds inert assets only: it never opens a
// listener, reads configuration, resolves a secret, or contacts a datastore.
// Every byte it returns is compiled into the binary, so an unauthenticated
// fetch of the shell can never disclose deployment state. All data the operator
// sees is fetched by the shell from the already-authenticated read-only
// boundary using a bearer token the operator types into the page.
package dashboard

import (
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
)

//go:embed assets/index.html assets/app.css assets/app.js assets/icon.svg
var assets embed.FS

// ErrConfiguration reports a caller binding the handler cannot honour. The
// handler fails closed: no default mount point is invented.
var ErrConfiguration = errors.New("dashboard: invalid configuration")

// ContentSecurityPolicy is the single policy served with every asset. It denies
// every default fetch, permits only same-origin script, style, image, and XHR
// targets, and forbids framing, base rewriting, and form navigation. The shell
// carries no inline script or style, so no hash or nonce is required. Form
// submission is handled in script and cancelled, so form-action 'none' also
// guarantees a token can never leave the page as a URL parameter.
const ContentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; " +
	"connect-src 'self'; font-src 'none'; object-src 'none'; media-src 'none'; child-src 'none'; " +
	"form-action 'none'; base-uri 'none'; frame-ancestors 'none'"

// PermissionsPolicy denies every powerful browser feature. An operator console
// needs none of them.
const PermissionsPolicy = "accelerometer=(), ambient-light-sensor=(), autoplay=(), battery=(), camera=(), " +
	"display-capture=(), document-domain=(), encrypted-media=(), fullscreen=(), geolocation=(), gyroscope=(), " +
	"magnetometer=(), microphone=(), midi=(), payment=(), picture-in-picture=(), publickey-credentials-get=(), " +
	"screen-wake-lock=(), serial=(), usb=(), xr-spatial-tracking=()"

// CacheControl revalidates on every navigation. Assets are immutable per build
// but their URLs are not build-stamped, so a stored copy must never be reused
// without an ETag check; a redeployed binary can therefore never be shadowed by
// a stale script in an operator's browser cache.
const CacheControl = "no-cache, must-revalidate, private"

// Asset file names relative to the mounted base path. The index is served only
// at the base path with a trailing slash so that every relative asset URL in
// the document resolves under the same prefix.
const (
	FileStyle  = "app.css"
	FileScript = "app.js"
	FileIcon   = "icon.svg"
)

// Config binds the handler to one explicit mount point. There is no default:
// an empty BasePath is rejected rather than silently mounted at the origin
// root, because the surrounding router owns the origin's namespace.
type Config struct {
	// BasePath is the absolute, already-cleaned path the handler is mounted
	// at, such as "/ui" or "/". It must not end in a slash unless it is the
	// origin root.
	BasePath string
}

type asset struct {
	contentType string
	body        []byte
	etag        string
	length      string
}

// Handler answers a fixed, closed set of asset routes under its base path.
// Anything else is a JSON 404 in the same shape the read-only boundary uses.
type Handler struct {
	basePath  string
	indexPath string
	routes    map[string]asset
	paths     []string
}

// New reads the embedded assets, derives a content ETag for each, and returns a
// handler bound to config.BasePath.
func New(config Config) (*Handler, error) {
	base, err := normalizeBasePath(config.BasePath)
	if err != nil {
		return nil, err
	}
	index, err := loadAsset("assets/index.html", "text/html; charset=utf-8")
	if err != nil {
		return nil, err
	}
	style, err := loadAsset("assets/"+FileStyle, "text/css; charset=utf-8")
	if err != nil {
		return nil, err
	}
	script, err := loadAsset("assets/"+FileScript, "text/javascript; charset=utf-8")
	if err != nil {
		return nil, err
	}
	icon, err := loadAsset("assets/"+FileIcon, "image/svg+xml")
	if err != nil {
		return nil, err
	}
	indexPath := base
	if indexPath != "/" {
		indexPath += "/"
	}
	handler := &Handler{basePath: base, indexPath: indexPath, routes: map[string]asset{
		indexPath:                        index,
		path.Join(indexPath, FileStyle):  style,
		path.Join(indexPath, FileScript): script,
		path.Join(indexPath, FileIcon):   icon,
	}}
	handler.paths = make([]string, 0, len(handler.routes))
	for route := range handler.routes {
		handler.paths = append(handler.paths, route)
	}
	slices.Sort(handler.paths)
	return handler, nil
}

func normalizeBasePath(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("%w: base path is required", ErrConfiguration)
	}
	if !strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("%w: base path must be absolute", ErrConfiguration)
	}
	if value != "/" && strings.HasSuffix(value, "/") {
		return "", fmt.Errorf("%w: base path must not end in a slash", ErrConfiguration)
	}
	if path.Clean(value) != value {
		return "", fmt.Errorf("%w: base path must be clean", ErrConfiguration)
	}
	if strings.ContainsAny(value, "?#\\ ") {
		return "", fmt.Errorf("%w: base path contains a reserved character", ErrConfiguration)
	}
	return value, nil
}

func loadAsset(name, contentType string) (asset, error) {
	body, err := assets.ReadFile(name)
	if err != nil {
		return asset{}, fmt.Errorf("%w: %s: %w", ErrConfiguration, name, err)
	}
	if len(body) == 0 {
		return asset{}, fmt.Errorf("%w: %s is empty", ErrConfiguration, name)
	}
	digest := sha256.Sum256(body)
	return asset{contentType: contentType, body: body,
		etag:   `"` + base64.RawURLEncoding.EncodeToString(digest[:16]) + `"`,
		length: strconv.Itoa(len(body))}, nil
}

// BasePath returns the mount point the handler was bound to.
func (h *Handler) BasePath() string { return h.basePath }

// IndexPath returns the canonical shell URL. A request to the bare base path is
// redirected here so relative asset URLs resolve under the mount point.
func (h *Handler) IndexPath() string { return h.indexPath }

// Paths returns every absolute path the handler answers, sorted. A router that
// prefers exact registration over prefix matching can register these directly.
func (h *Handler) Paths() []string { return slices.Clone(h.paths) }

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h == nil || len(h.routes) == 0 {
		writeProblem(writer, http.StatusInternalServerError, "dashboard_unavailable")
		return
	}
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writeProblem(writer, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if request.URL.RawQuery != "" {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	requestPath := request.URL.Path
	if requestPath == h.basePath && requestPath != h.indexPath {
		writeSecurityHeaders(writer)
		writer.Header().Set("Cache-Control", CacheControl)
		writer.Header().Set("Location", h.indexPath)
		writer.WriteHeader(http.StatusPermanentRedirect)
		return
	}
	selected, ok := h.routes[requestPath]
	if !ok {
		writeProblem(writer, http.StatusNotFound, "not_found")
		return
	}
	writeSecurityHeaders(writer)
	writer.Header().Set("Cache-Control", CacheControl)
	writer.Header().Set("ETag", selected.etag)
	writer.Header().Set("Content-Type", selected.contentType)
	if matchesETag(request.Header.Values("If-None-Match"), selected.etag) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}
	writer.Header().Set("Content-Length", selected.length)
	writer.WriteHeader(http.StatusOK)
	if request.Method == http.MethodHead {
		return
	}
	_, _ = writer.Write(selected.body)
}

func matchesETag(headers []string, etag string) bool {
	for _, header := range headers {
		for candidate := range strings.SplitSeq(header, ",") {
			candidate = strings.TrimSpace(candidate)
			if candidate == "*" || candidate == etag || strings.TrimPrefix(candidate, "W/") == etag {
				return true
			}
		}
	}
	return false
}

func writeSecurityHeaders(writer http.ResponseWriter) {
	header := writer.Header()
	header.Set("Content-Security-Policy", ContentSecurityPolicy)
	header.Set("Permissions-Policy", PermissionsPolicy)
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("X-Frame-Options", "DENY")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("Cross-Origin-Opener-Policy", "same-origin")
	header.Set("Cross-Origin-Resource-Policy", "same-origin")
	header.Set("Cross-Origin-Embedder-Policy", "require-corp")
}

func writeProblem(writer http.ResponseWriter, status int, code string) {
	body := []byte(`{"error":"` + code + `"}` + "\n")
	writeSecurityHeaders(writer)
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}
