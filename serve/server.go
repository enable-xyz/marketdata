package serve

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/enable-xyz/marketdata/warehouse"
)

type requestScopeKey struct{}

type Server struct {
	config           resolvedConfig
	deps             Dependencies
	http             *http.Server
	catalog          *requestGate
	query            *requestGate
	native           *requestGate
	normalized       *requestGate
	rejecting        atomic.Bool
	serveMu          sync.Mutex
	serving          bool
	handlerMu        sync.Mutex
	handlers         sync.WaitGroup
	acceptHandlers   bool
	wipeScheduleOnce sync.Once
	wipeOnce         sync.Once
	wipeDone         chan struct{}
}

// New resolves every certificate, key, paging key, and bearer token before a
// listener can be passed to Serve. Resolved material is retained only in
// process memory and is never formatted, logged, or returned by an endpoint.
func New(ctx context.Context, config Config, resolver SecretResolver, dependencies Dependencies) (*Server, error) {
	resolved, err := resolveConfig(ctx, config, resolver)
	if err != nil {
		return nil, err
	}
	if dependencies.Metadata == nil || dependencies.Datasets == nil || dependencies.Query == nil ||
		dependencies.Native == nil || dependencies.Normalized == nil || dependencies.Metrics == nil {
		wipeResolvedConfig(&resolved)
		return nil, fmt.Errorf("%w: every read-only dependency is required", ErrConfiguration)
	}
	server := &Server{config: resolved, deps: dependencies, catalog: newRequestGate(resolved.Catalog),
		query: newRequestGate(resolved.Query), native: newRequestGate(resolved.NativeReplay),
		normalized: newRequestGate(resolved.NormalizedReplay), acceptHandlers: true, wipeDone: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", server.handleLive)
	mux.HandleFunc("GET /health/ready", server.handleReady)
	mux.Handle("GET /v1/catalog/sources", server.scoped(ScopeCatalogRead, server.catalog, server.handleSources))
	mux.Handle("GET /v1/catalog/instruments", server.scoped(ScopeCatalogRead, server.catalog, server.handleInstruments))
	mux.Handle("GET /v1/coverage", server.scoped(ScopeCoverageRead, server.catalog, server.handleCoverage))
	mux.Handle("GET /v1/incidents", server.scoped(ScopeCoverageRead, server.catalog, server.handleIncidents))
	mux.Handle("GET /v1/datasets", server.scoped(ScopeCatalogRead, server.catalog, server.handleDatasets))
	mux.Handle("POST /v1/query", server.scoped(ScopeQueryRead, server.query, server.handleQuery))
	mux.Handle("POST /v1/replay/native", server.scoped(ScopeReplayNative, server.native, server.handleNativeReplay))
	mux.Handle("POST /v1/replay/normalized", server.scoped(ScopeReplayNormalized, server.normalized, server.handleNormalizedReplay))
	mux.Handle("GET /metrics", server.scoped(ScopeMetricsRead, server.catalog, server.handleMetrics))
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	server.http = &http.Server{
		Handler: server.trackHandlers(server.boundary(mux)), ReadHeaderTimeout: resolved.ReadHeaderTimeout, ReadTimeout: resolved.ReadTimeout,
		WriteTimeout: resolved.WriteTimeout, IdleTimeout: resolved.IdleTimeout, Protocols: protocols,
		ErrorLog: log.New(io.Discard, "", 0),
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{resolved.certificate},
			NextProtos: []string{"h2", "http/1.1"}},
	}
	return server, nil
}

func (s *Server) Serve(listener net.Listener) error {
	if s == nil || listener == nil || s.http == nil {
		return ErrConfiguration
	}
	s.serveMu.Lock()
	if s.serving || s.rejecting.Load() {
		s.serveMu.Unlock()
		return ErrShuttingDown
	}
	s.serving = true
	s.serveMu.Unlock()
	err := s.http.Serve(tls.NewListener(listener, s.http.TLSConfig))
	s.serveMu.Lock()
	s.serving = false
	s.serveMu.Unlock()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown first rejects new work, then drains within the caller's context. If
// the bound expires, Close cancels remaining handlers before this method
// returns. A stopped Server is intentionally not reusable; restart creates a
// newly resolved instance.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.http == nil || ctx == nil {
		return ErrConfiguration
	}
	s.rejecting.Store(true)
	s.handlerMu.Lock()
	s.acceptHandlers = false
	s.handlerMu.Unlock()
	err := s.http.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, s.http.Close())
	}
	s.scheduleSecretWipe()
	select {
	case <-s.wipeDone:
		return err
	case <-ctx.Done():
		return errors.Join(err, context.Cause(ctx))
	}
}

func (s *Server) scheduleSecretWipe() {
	s.wipeScheduleOnce.Do(func() {
		go func() {
			s.handlers.Wait()
			s.wipeSecrets()
			close(s.wipeDone)
		}()
	})
}

func (s *Server) wipeSecrets() {
	s.wipeOnce.Do(func() {
		wipeResolvedConfig(&s.config)
		if s.http != nil && s.http.TLSConfig != nil {
			for index := range s.http.TLSConfig.Certificates {
				wipeTLSCertificate(&s.http.TLSConfig.Certificates[index])
			}
			s.http.TLSConfig.Certificates = nil
		}
	})
}

func wipeResolvedConfig(config *resolvedConfig) {
	clear(config.pagingKey)
	config.pagingKey = nil
	for index := range config.auth.principals {
		clear(config.auth.principals[index].tokenDigest[:])
	}
	config.auth.principals = nil
	wipeTLSCertificate(&config.certificate)
}

func wipeTLSCertificate(certificate *tls.Certificate) {
	for index := range certificate.Certificate {
		clear(certificate.Certificate[index])
	}
	certificate.Certificate = nil
	clear(certificate.OCSPStaple)
	certificate.OCSPStaple = nil
	for index := range certificate.SignedCertificateTimestamps {
		clear(certificate.SignedCertificateTimestamps[index])
	}
	certificate.SignedCertificateTimestamps = nil
	wipePrivateKey(certificate.PrivateKey)
	*certificate = tls.Certificate{}
}

func wipePrivateKey(key any) {
	switch value := key.(type) {
	case ed25519.PrivateKey:
		clear(value)
	case []byte:
		clear(value)
	case *ecdsa.PrivateKey:
		if value != nil {
			wipeBigInt(value.D)
			value.D = nil
		}
	case *rsa.PrivateKey:
		if value == nil {
			return
		}
		wipeBigInt(value.D)
		value.D = nil
		for _, prime := range value.Primes {
			wipeBigInt(prime)
		}
		value.Primes = nil
		for _, integer := range []*big.Int{value.Precomputed.Dp, value.Precomputed.Dq, value.Precomputed.Qinv} {
			wipeBigInt(integer)
		}
		for index := range value.Precomputed.CRTValues {
			for _, integer := range []*big.Int{value.Precomputed.CRTValues[index].Exp, value.Precomputed.CRTValues[index].Coeff, value.Precomputed.CRTValues[index].R} {
				wipeBigInt(integer)
			}
		}
		value.Precomputed = rsa.PrecomputedValues{}
	}
}

func wipeBigInt(integer *big.Int) {
	if integer == nil {
		return
	}
	clear(integer.Bits())
	integer.SetInt64(0)
}

func (s *Server) trackHandlers(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		s.handlerMu.Lock()
		if !s.acceptHandlers {
			s.handlerMu.Unlock()
			writeProblem(writer, http.StatusServiceUnavailable, "shutting_down")
			return
		}
		s.handlers.Add(1)
		s.handlerMu.Unlock()
		defer s.handlers.Done()
		next.ServeHTTP(writer, request)
	})
}

func (s *Server) boundary(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.TLS == nil {
			writeProblem(writer, http.StatusUpgradeRequired, "tls_required")
			return
		}
		health := request.Method == http.MethodGet && (request.URL.Path == "/health/live" || request.URL.Path == "/health/ready")
		if health {
			next.ServeHTTP(writer, request)
			return
		}
		if s.rejecting.Load() {
			writeProblem(writer, http.StatusServiceUnavailable, "shutting_down")
			return
		}
		scopes, err := s.config.auth.authenticate(bearerHeaders(request))
		if err != nil {
			writer.Header().Set("WWW-Authenticate", `Bearer realm="marketdata"`)
			writeProblem(writer, http.StatusUnauthorized, "authentication_required")
			return
		}
		request = request.WithContext(context.WithValue(request.Context(), requestScopeKey{}, scopes))
		next.ServeHTTP(writer, request)
	})
}

func (s *Server) scoped(scope Scope, gate *requestGate, handler http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		scopes, ok := request.Context().Value(requestScopeKey{}).(scopeSet)
		if !ok || authorize(scopes, scope) != nil {
			writeProblem(writer, http.StatusForbidden, "scope_denied")
			return
		}
		deadline := gate.limits.Deadline
		if gate.limits.MaxDuration > 0 && gate.limits.MaxDuration < deadline {
			deadline = gate.limits.MaxDuration
		}
		ctx, cancel := context.WithTimeout(request.Context(), deadline)
		defer cancel()
		if deadlineAt, ok := ctx.Deadline(); ok {
			controller := http.NewResponseController(writer)
			_ = controller.SetReadDeadline(deadlineAt)
			_ = controller.SetWriteDeadline(deadlineAt)
		}
		release, err := gate.enter(ctx)
		if err != nil {
			writeProblem(writer, statusForError(err), codeForError(err))
			return
		}
		defer release()
		handler(writer, request.WithContext(ctx))
	})
}

func (s *Server) handleLive(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	writeJSON(writer, http.StatusOK, struct {
		Live bool `json:"live"`
	}{Live: true}, 128)
}

func (s *Server) handleReady(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	ready := !s.rejecting.Load() && s.deps.Metadata.Ready()
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(writer, status, struct {
		Ready bool `json:"ready"`
	}{Ready: ready}, 128)
}

func (s *Server) handleSources(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	values, err := s.deps.Metadata.Sources(request.Context())
	s.writeMetadata(writer, values, err)
}

func (s *Server) handleInstruments(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	values, err := s.deps.Metadata.Instruments(request.Context())
	s.writeMetadata(writer, values, err)
}

func (s *Server) handleCoverage(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	values, err := s.deps.Metadata.Coverage(request.Context())
	s.writeMetadata(writer, values, err)
}

func (s *Server) handleIncidents(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	values, err := s.deps.Metadata.Incidents(request.Context())
	s.writeMetadata(writer, values, err)
}

func (s *Server) handleDatasets(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	values, err := s.deps.Metadata.Datasets(request.Context())
	s.writeMetadata(writer, values, err)
}

func (s *Server) writeMetadata(writer http.ResponseWriter, value any, err error) {
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	writeJSON(writer, http.StatusOK, value, min(s.config.Catalog.MaxBytes, s.config.MaxResponseBytes))
}

func (s *Server) handleQuery(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	var input QueryRequest
	if err := decodeClosedJSON(writer, request, &input); err != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	query, err := normalizeQueryRequest(input, s.config.Config)
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	digest, err := query.digest()
	if err != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	now := s.config.Now().UTC()
	var parsed *pageTokenPayload
	if query.PageToken != "" {
		token, tokenErr := parsePageToken(query.PageToken, s.config.pagingKey, digest, now)
		if tokenErr != nil {
			writeProblem(writer, http.StatusBadRequest, "page_token_denied")
			return
		}
		parsed = &token
	}
	dataset, err := pinQueryDataset(request.Context(), s.deps.Datasets, query, parsed)
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	var after *warehouse.SortKey
	if parsed != nil {
		after = &parsed.LastKey
	}
	page, err := s.deps.Query.Page(request.Context(), query.spec(dataset, after))
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	if page.Dataset != dataset {
		writeProblem(writer, http.StatusServiceUnavailable, "query_identity_mismatch")
		return
	}
	body, err := s.encodeQueryPage(request.Context(), page, digest, now)
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	writeBytes(writer, http.StatusOK, "application/json", body)
}

func (s *Server) encodeQueryPage(ctx context.Context, page warehouse.Page, digest [32]byte, now time.Time) ([]byte, error) {
	maximum := min(s.config.MaxResponseBytes, s.config.Query.MaxBytes)
	if page.Coverage == nil {
		page.Coverage = []warehouse.CoverageReference{}
	}
	if page.Gaps == nil {
		page.Gaps = []warehouse.GapReference{}
	}
	coverage, err := json.Marshal(page.Coverage)
	if err != nil {
		return nil, err
	}
	gaps, err := json.Marshal(page.Gaps)
	if err != nil {
		return nil, err
	}
	appendString := func(destination []byte, value string) []byte {
		encoded, _ := json.Marshal(value)
		return append(destination, encoded...)
	}
	body := make([]byte, 0, min(int(maximum), s.config.Query.BufferBytes))
	body = append(body, `{"dataset_id":`...)
	body = appendString(body, page.Dataset.IDString())
	body = append(body, `,"catalog_snapshot_id":`...)
	body = appendString(body, page.Dataset.CatalogSnapshotIDString())
	body = append(body, `,"schema_name":`...)
	body = appendString(body, page.Dataset.SchemaName)
	body = append(body, `,"schema_version":`...)
	body = strconv.AppendUint(body, uint64(page.Dataset.SchemaVersion), 10)
	body = append(body, `,"records":[`...)
	tail := func(token string) []byte {
		result := make([]byte, 0, len(coverage)+len(gaps)+len(token)+64)
		result = append(result, `],"coverage":`...)
		result = append(result, coverage...)
		result = append(result, `,"gaps":`...)
		result = append(result, gaps...)
		if token != "" {
			result = append(result, `,"next_page_token":`...)
			result = strconv.AppendQuote(result, token)
		}
		result = append(result, '}', '\n')
		return result
	}
	if len(page.Rows) == 0 {
		if page.HasMore {
			return nil, ErrResponseTooLarge
		}
		suffix := tail("")
		if int64(len(body)+len(suffix)) > maximum {
			return nil, ErrResponseTooLarge
		}
		return append(body, suffix...), nil
	}
	var suffix []byte
	selected := 0
	for index, row := range page.Rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		encodedRow, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		continuation := page.HasMore || index+1 < len(page.Rows)
		token := ""
		if continuation {
			last, err := row.SortKey()
			if err != nil {
				return nil, err
			}
			token, err = mintPageToken(s.config.pagingKey, digest, page.Dataset.IDString(), last, now.Add(s.config.PageTokenTTL))
			if err != nil {
				return nil, err
			}
		}
		candidateSuffix := tail(token)
		separator := 0
		if selected != 0 {
			separator = 1
		}
		if int64(len(body)+separator+len(encodedRow)+len(candidateSuffix)) > maximum {
			if selected == 0 {
				return nil, ErrResponseTooLarge
			}
			break
		}
		if separator != 0 {
			body = append(body, ',')
		}
		body = append(body, encodedRow...)
		suffix = candidateSuffix
		selected++
	}
	if selected == 0 || suffix == nil {
		return nil, ErrResponseTooLarge
	}
	body = append(body, suffix...)
	return body, nil
}

func (s *Server) handleNativeReplay(writer http.ResponseWriter, request *http.Request) {
	_, dataset, serviceRequest, ok := s.prepareReplay(writer, request)
	if !ok {
		return
	}
	stream, err := s.deps.Native.OpenNative(request.Context(), serviceRequest)
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	defer stream.Close()
	first, err := stream.Next(request.Context())
	if err != nil && !errors.Is(err, io.EOF) {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	if err == nil {
		if len(first) > segment.MaxRecordBytes || int64(len(first)) > s.config.NativeReplay.MaxBytes {
			writeProblem(writer, http.StatusRequestEntityTooLarge, "oversized_replay_row")
			return
		}
		if validateErr := validateNativeFrame(first, serviceRequest); validateErr != nil {
			writeProblem(writer, http.StatusServiceUnavailable, "replay_identity_mismatch")
			return
		}
	}
	setReplayHeaders(writer, replay.NativeStreamContentTypeV1, dataset)
	declareReplayTrailers(writer)
	buffer := bufio.NewWriterSize(writer, s.config.NativeReplay.BufferBytes)
	var written int64
	if err == nil {
		if writeErr := writeBoundedChunk(request.Context(), buffer, first, &written, s.config.NativeReplay.MaxBytes); writeErr != nil {
			finishReplay(writer, buffer, replayWriteTerminalCode(writeErr))
			return
		}
	}
	for {
		frame, nextErr := stream.Next(request.Context())
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			finishReplay(writer, buffer, replayTerminalCode(nextErr))
			return
		}
		if validateNativeFrame(frame, serviceRequest) != nil {
			finishReplay(writer, buffer, "invalid_frame")
			return
		}
		if int64(len(frame)) > s.config.NativeReplay.MaxBytes-written {
			writer.Header().Set(ReplayTrailerTruncated, "bytes")
			finishReplay(writer, buffer, "byte_limit")
			return
		}
		if writeErr := writeBoundedChunk(request.Context(), buffer, frame, &written, s.config.NativeReplay.MaxBytes); writeErr != nil {
			finishReplay(writer, buffer, replayWriteTerminalCode(writeErr))
			return
		}
	}
	finishReplay(writer, buffer, "")
}

func (s *Server) handleNormalizedReplay(writer http.ResponseWriter, request *http.Request) {
	_, dataset, serviceRequest, ok := s.prepareReplay(writer, request)
	if !ok {
		return
	}
	stream, err := s.deps.Normalized.OpenNormalized(request.Context(), serviceRequest)
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	defer stream.Close()
	first, err := stream.Next(request.Context())
	var firstLine []byte
	if err == nil {
		firstLine, err = first.AppendNDJSON(nil, serviceRequest)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	if int64(len(firstLine)) > s.config.NormalizedReplay.MaxBytes {
		writeProblem(writer, http.StatusRequestEntityTooLarge, "oversized_replay_row")
		return
	}
	setReplayHeaders(writer, replay.NormalizedContentTypeV1, dataset)
	declareReplayTrailers(writer)
	buffer := bufio.NewWriterSize(writer, s.config.NormalizedReplay.BufferBytes)
	var written int64
	if len(firstLine) != 0 {
		if writeErr := writeBoundedChunk(request.Context(), buffer, firstLine, &written, s.config.NormalizedReplay.MaxBytes); writeErr != nil {
			finishReplay(writer, buffer, replayWriteTerminalCode(writeErr))
			return
		}
	}
	for {
		item, nextErr := stream.Next(request.Context())
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			finishReplay(writer, buffer, replayTerminalCode(nextErr))
			return
		}
		line, encodeErr := item.AppendNDJSON(nil, serviceRequest)
		if encodeErr != nil {
			finishReplay(writer, buffer, "invalid_item")
			return
		}
		if int64(len(line)) > s.config.NormalizedReplay.MaxBytes-written {
			writer.Header().Set(ReplayTrailerTruncated, "bytes")
			finishReplay(writer, buffer, "byte_limit")
			return
		}
		if writeErr := writeBoundedChunk(request.Context(), buffer, line, &written, s.config.NormalizedReplay.MaxBytes); writeErr != nil {
			finishReplay(writer, buffer, replayWriteTerminalCode(writeErr))
			return
		}
	}
	finishReplay(writer, buffer, "")
}

func (s *Server) prepareReplay(writer http.ResponseWriter, request *http.Request) (ReplayRequest, warehouse.Dataset, replay.ServiceRequest, bool) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return ReplayRequest{}, warehouse.Dataset{}, replay.ServiceRequest{}, false
	}
	var input ReplayRequest
	if err := decodeClosedJSON(writer, request, &input); err != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return ReplayRequest{}, warehouse.Dataset{}, replay.ServiceRequest{}, false
	}
	normalized, err := normalizeReplayRequest(input, s.config.Config)
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return ReplayRequest{}, warehouse.Dataset{}, replay.ServiceRequest{}, false
	}
	dataset, err := pinReplayDataset(request.Context(), s.deps.Datasets, normalized)
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return ReplayRequest{}, warehouse.Dataset{}, replay.ServiceRequest{}, false
	}
	serviceRequest := normalized.serviceRequest(dataset)
	if err := serviceRequest.Validate(); err != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return ReplayRequest{}, warehouse.Dataset{}, replay.ServiceRequest{}, false
	}
	return input, dataset, serviceRequest, true
}

func (s *Server) handleMetrics(writer http.ResponseWriter, request *http.Request) {
	if rejectQueryParameters(request) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	metrics, err := s.deps.Metrics.Metrics(request.Context())
	if err != nil {
		writeProblem(writer, statusForError(err), codeForError(err))
		return
	}
	if len(metrics) > MaximumMetrics {
		writeProblem(writer, http.StatusRequestEntityTooLarge, "response_too_large")
		return
	}
	metrics = slices.Clone(metrics)
	slices.SortFunc(metrics, func(left, right Metric) int { return strings.Compare(left.Name, right.Name) })
	maximum := min(s.config.Catalog.MaxBytes, s.config.MaxResponseBytes)
	body := make([]byte, 0, min(s.config.Catalog.BufferBytes, 4096))
	for index, metric := range metrics {
		if !validMetric(metric) || (index > 0 && metric.Name == metrics[index-1].Name) {
			writeProblem(writer, http.StatusServiceUnavailable, "metrics_unavailable")
			return
		}
		body = append(body, "# HELP "...)
		body = append(body, metric.Name...)
		body = append(body, ' ')
		body = append(body, strings.ReplaceAll(strings.ReplaceAll(metric.Help, "\\", "\\\\"), "\n", "\\n")...)
		body = append(body, '\n')
		body = append(body, "# TYPE "...)
		body = append(body, metric.Name...)
		body = append(body, ' ')
		body = append(body, string(metric.Type)...)
		body = append(body, '\n')
		body = append(body, metric.Name...)
		body = append(body, ' ')
		body = strconv.AppendFloat(body, metric.Value, 'g', -1, 64)
		body = append(body, '\n')
		if int64(len(body)) > maximum {
			writeProblem(writer, http.StatusRequestEntityTooLarge, "response_too_large")
			return
		}
	}
	writeBytes(writer, http.StatusOK, "text/plain; version=0.0.4", body)
}

func validMetric(metric Metric) bool {
	if metric.Name == "" || len(metric.Name) > 128 || metric.Help == "" || len(metric.Help) > 4096 ||
		(metric.Type != MetricCounter && metric.Type != MetricGauge) || math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
		return false
	}
	for index, value := range metric.Name {
		if !((value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || value == '_' || value == ':' ||
			(index > 0 && value >= '0' && value <= '9')) {
			return false
		}
	}
	return strings.IndexByte(metric.Help, 0) < 0
}

func validateNativeFrame(frame []byte, request replay.ServiceRequest) error {
	if len(frame) == 0 || len(frame) > segment.MaxRecordBytes {
		return ErrResponseTooLarge
	}
	record, err := segment.UnmarshalEnvelope(frame)
	if err != nil {
		return err
	}
	_, sourceSelected := slices.BinarySearch(request.SourceIDs, record.SourceID)
	if !sourceSelected || record.ReceivedWallTimeNS < request.StartReceivedTimeNS ||
		record.ReceivedWallTimeNS >= request.EndReceivedTimeNS {
		return replay.ErrInvalidServiceRequest
	}
	if record.Kind == segment.RecordKindControl && record.ChannelOrEndpoint == "replay.discontinuity" {
		return nil
	}
	_, channelSelected := slices.BinarySearch(request.ChannelIDs, record.ChannelOrEndpoint)
	instrument := ""
	if record.InstrumentUID.Valid {
		instrument = record.InstrumentUID.Value
	}
	_, instrumentSelected := slices.BinarySearch(request.InstrumentUIDs, instrument)
	if (len(request.ChannelIDs) != 0 && !channelSelected) ||
		(len(request.InstrumentUIDs) != 0 && !instrumentSelected) {
		return replay.ErrInvalidServiceRequest
	}
	return nil
}

func setReplayHeaders(writer http.ResponseWriter, contentType string, dataset warehouse.Dataset) {
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("X-Dataset-ID", dataset.IDString())
	writer.Header().Set("X-Catalog-Snapshot-ID", dataset.CatalogSnapshotIDString())
	writer.Header().Set("X-Schema-Name", dataset.SchemaName)
	writer.Header().Set("X-Schema-Version", strconv.FormatUint(uint64(dataset.SchemaVersion), 10))
	writer.Header().Set("X-Content-Type-Options", "nosniff")
}
func declareReplayTrailers(writer http.ResponseWriter) {
	writer.Header().Add("Trailer", ReplayTrailerTruncated+", "+ReplayTrailerTerminalError)
}

func finishReplay(writer http.ResponseWriter, buffer *bufio.Writer, code string) {
	if err := buffer.Flush(); err != nil {
		code = "write_error"
	}
	if code != "" {
		writer.Header().Set(ReplayTrailerTerminalError, code)
	}
}

func replayTerminalCode(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return "source_error"
	}
}

func replayWriteTerminalCode(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return replayTerminalCode(err)
	}
	return "write_error"
}

func writeBoundedChunk(ctx context.Context, writer io.Writer, chunk []byte, written *int64, maximum int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if int64(len(chunk)) > maximum-*written {
		return ErrResponseTooLarge
	}
	for len(chunk) != 0 {
		count, err := writer.Write(chunk)
		*written += int64(count)
		chunk = chunk[count:]
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

type requestGate struct {
	limits  RouteLimits
	slots   chan struct{}
	workers chan struct{}
}

func newRequestGate(limits RouteLimits) *requestGate {
	return &requestGate{limits: limits, slots: make(chan struct{}, limits.QueueDepth+limits.Concurrency),
		workers: make(chan struct{}, limits.Concurrency)}
}

func (g *requestGate) enter(ctx context.Context) (func(), error) {
	select {
	case g.slots <- struct{}{}:
	default:
		return nil, ErrQueueFull
	}
	select {
	case g.workers <- struct{}{}:
		return func() {
			<-g.workers
			<-g.slots
		}, nil
	case <-ctx.Done():
		<-g.slots
		return nil, context.Cause(ctx)
	}
}

func decodeClosedJSON(writer http.ResponseWriter, request *http.Request, target any) error {
	contentType := request.Header.Get("Content-Type")
	if contentType != "application/json" && !strings.HasPrefix(contentType, "application/json;") {
		return ErrQueryRequest
	}
	request.Body = http.MaxBytesReader(writer, request.Body, MaximumRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrQueryRequest
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrQueryRequest
	}
	return nil
}

func rejectQueryParameters(request *http.Request) error {
	if request.URL.RawQuery != "" {
		return ErrQueryRequest
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, value any, maximum int64) {
	encoded, err := json.Marshal(value)
	if err != nil {
		writeProblem(writer, http.StatusInternalServerError, "encoding_failed")
		return
	}
	encoded = append(encoded, '\n')
	if int64(len(encoded)) > maximum {
		writeProblem(writer, http.StatusRequestEntityTooLarge, "response_too_large")
		return
	}
	writeBytes(writer, status, "application/json", encoded)
}

func writeBytes(writer http.ResponseWriter, status int, contentType string, body []byte) {
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}

func writeProblem(writer http.ResponseWriter, status int, code string) {
	body := []byte(`{"error":"` + code + `"}` + "\n")
	writeBytes(writer, status, "application/json", body)
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, ErrAuthentication):
		return http.StatusUnauthorized
	case errors.Is(err, ErrAuthorization):
		return http.StatusForbidden
	case errors.Is(err, ErrQueryRequest), errors.Is(err, ErrPageToken), errors.Is(err, warehouse.ErrInvalidQuery), errors.Is(err, replay.ErrInvalidServiceRequest):
		return http.StatusBadRequest
	case errors.Is(err, ErrResponseTooLarge):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, ErrQueueFull), errors.Is(err, ErrShuttingDown):
		return http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		return http.StatusRequestTimeout
	default:
		return http.StatusServiceUnavailable
	}
}

func codeForError(err error) string {
	switch {
	case errors.Is(err, ErrPageToken):
		return "page_token_denied"
	case errors.Is(err, ErrQueryRequest), errors.Is(err, warehouse.ErrInvalidQuery), errors.Is(err, replay.ErrInvalidServiceRequest):
		return "invalid_request"
	case errors.Is(err, ErrResponseTooLarge):
		return "response_too_large"
	case errors.Is(err, ErrQueueFull):
		return "queue_full"
	case errors.Is(err, ErrShuttingDown):
		return "shutting_down"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "request_cancelled"
	default:
		return "unavailable"
	}
}
