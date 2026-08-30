package serve

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
)

type scopeSet uint8

const (
	bitCatalog scopeSet = 1 << iota
	bitCoverage
	bitQuery
	bitNative
	bitNormalized
	bitMetrics
)

type resolvedPrincipal struct {
	tokenDigest [sha256.Size]byte
	scopes      scopeSet
}

type authenticator struct {
	principals []resolvedPrincipal
}

func resolveConfig(ctx context.Context, config Config, resolver SecretResolver) (resolvedConfig, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return resolvedConfig{}, err
	}
	if resolver == nil {
		return resolvedConfig{}, fmt.Errorf("%w: runtime secret resolver is required", ErrConfiguration)
	}
	certificatePEM, err := resolver.Resolve(ctx, normalized.TLSCertRef)
	if err != nil || len(certificatePEM) == 0 || len(certificatePEM) > MaximumSecretBytes {
		clear(certificatePEM)
		return resolvedConfig{}, fmt.Errorf("%w: resolving TLS certificate", ErrConfiguration)
	}
	defer clear(certificatePEM)
	keyPEM, err := resolver.Resolve(ctx, normalized.TLSKeyRef)
	if err != nil || len(keyPEM) == 0 || len(keyPEM) > MaximumSecretBytes {
		clear(keyPEM)
		return resolvedConfig{}, fmt.Errorf("%w: resolving TLS private key", ErrConfiguration)
	}
	defer clear(keyPEM)
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return resolvedConfig{}, fmt.Errorf("%w: parsing TLS certificate pair", ErrConfiguration)
	}
	return resolveParsedConfig(ctx, normalized, resolver, &certificate)
}

func resolveParsedConfig(ctx context.Context, normalized Config, resolver SecretResolver, certificate *tls.Certificate) (resolvedConfig, error) {
	defer wipeTLSCertificate(certificate)
	pagingMaterial, err := resolver.Resolve(ctx, normalized.PagingKeyRef)
	if err != nil || len(pagingMaterial) < sha256.Size || len(pagingMaterial) > MaximumSecretBytes {
		clear(pagingMaterial)
		return resolvedConfig{}, fmt.Errorf("%w: resolving paging key", ErrConfiguration)
	}
	pagingKey := append([]byte(nil), pagingMaterial...)
	clear(pagingMaterial)

	principals := make([]resolvedPrincipal, 0, len(normalized.Principals))
	for _, principal := range normalized.Principals {
		token, resolveErr := resolver.Resolve(ctx, principal.TokenRef)
		if resolveErr != nil || len(token) < MinimumBearerBytes || len(token) > MaximumBearerBytes {
			clear(token)
			clear(pagingKey)
			return resolvedConfig{}, fmt.Errorf("%w: resolving principal bearer token", ErrConfiguration)
		}
		digest := sha256.Sum256(token)
		clear(token)
		for _, existing := range principals {
			if subtle.ConstantTimeCompare(digest[:], existing.tokenDigest[:]) == 1 {
				clear(pagingKey)
				return resolvedConfig{}, fmt.Errorf("%w: bearer token identities must be unique", ErrConfiguration)
			}
		}
		var scopes scopeSet
		for _, scope := range principal.Scopes {
			mask, _ := scopeMask(scope)
			scopes |= mask
		}
		principals = append(principals, resolvedPrincipal{tokenDigest: digest, scopes: scopes})
	}
	resolved := resolvedConfig{Config: normalized, certificate: *certificate, pagingKey: pagingKey,
		auth: authenticator{principals: principals}}
	*certificate = tls.Certificate{}
	return resolved, nil
}

func scopeMask(scope Scope) (scopeSet, bool) {
	switch scope {
	case ScopeCatalogRead:
		return bitCatalog, true
	case ScopeCoverageRead:
		return bitCoverage, true
	case ScopeQueryRead:
		return bitQuery, true
	case ScopeReplayNative:
		return bitNative, true
	case ScopeReplayNormalized:
		return bitNormalized, true
	case ScopeMetricsRead:
		return bitMetrics, true
	default:
		return 0, false
	}
}

func (a authenticator) authenticate(headerValues []string) (scopeSet, error) {
	if len(headerValues) != 1 || !strings.HasPrefix(headerValues[0], "Bearer ") {
		return 0, ErrAuthentication
	}
	token := strings.TrimPrefix(headerValues[0], "Bearer ")
	if len(token) < MinimumBearerBytes || len(token) > MaximumBearerBytes {
		return 0, ErrAuthentication
	}
	digest := sha256.Sum256([]byte(token))
	var matched int
	var granted scopeSet
	for _, principal := range a.principals {
		equal := subtle.ConstantTimeCompare(digest[:], principal.tokenDigest[:])
		matched |= equal
		mask := scopeSet(0 - uint8(equal))
		granted |= principal.scopes & mask
	}
	if subtle.ConstantTimeEq(int32(matched), 1) != 1 {
		return 0, ErrAuthentication
	}
	return granted, nil
}

func authorize(scopes scopeSet, required Scope) error {
	mask, ok := scopeMask(required)
	if !ok || scopes&mask == 0 {
		return ErrAuthorization
	}
	return nil
}

func bearerHeaders(request *http.Request) []string {
	return request.Header.Values("Authorization")
}
