package serve

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type resolvedConfig struct {
	Config
	certificate tls.Certificate
	pagingKey   []byte
	auth        authenticator
}

func normalizeConfig(config Config) (Config, error) {
	defaults := DefaultConfig()
	if config.MaxQueryInterval == 0 {
		config.MaxQueryInterval = defaults.MaxQueryInterval
	}
	if config.DefaultPageRows == 0 {
		config.DefaultPageRows = defaults.DefaultPageRows
	}
	if config.MaxPageRows == 0 {
		config.MaxPageRows = defaults.MaxPageRows
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaults.MaxResponseBytes
	}
	if config.PageTokenTTL == 0 {
		config.PageTokenTTL = defaults.PageTokenTTL
	}
	if config.ReadHeaderTimeout == 0 {
		config.ReadHeaderTimeout = defaults.ReadHeaderTimeout
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = defaults.ReadTimeout
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = defaults.WriteTimeout
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = defaults.IdleTimeout
	}
	config.Catalog = normalizeRouteLimits(config.Catalog, defaults.Catalog)
	config.Query = normalizeRouteLimits(config.Query, defaults.Query)
	config.NativeReplay = normalizeRouteLimits(config.NativeReplay, defaults.NativeReplay)
	config.NormalizedReplay = normalizeRouteLimits(config.NormalizedReplay, defaults.NormalizedReplay)
	if config.Now == nil {
		config.Now = defaults.Now
	}

	if !validConfigText(config.TLSCertRef) || !validConfigText(config.TLSKeyRef) || !validConfigText(config.PagingKeyRef) {
		return Config{}, fmt.Errorf("%w: TLS certificate, TLS key, and paging key references are required", ErrConfiguration)
	}
	if len(config.Principals) < 1 || len(config.Principals) > MaximumPrincipals {
		return Config{}, fmt.Errorf("%w: principals must contain 1..%d entries", ErrConfiguration, MaximumPrincipals)
	}
	if config.MaxQueryInterval <= 0 || config.MaxQueryInterval > MaximumQueryInterval {
		return Config{}, fmt.Errorf("%w: maximum query interval may only reduce 24 hours", ErrConfiguration)
	}
	if config.DefaultPageRows < 1 || config.DefaultPageRows > DefaultPageRows || config.MaxPageRows < config.DefaultPageRows || config.MaxPageRows > MaximumPageRows {
		return Config{}, fmt.Errorf("%w: page rows exceed 1,000 default or 10,000 maximum", ErrConfiguration)
	}
	if config.MaxResponseBytes < 1 || config.MaxResponseBytes > MaximumResponseBytes {
		return Config{}, fmt.Errorf("%w: JSON response limit exceeds 16 MiB", ErrConfiguration)
	}
	if config.PageTokenTTL < time.Second || config.PageTokenTTL > 24*time.Hour || config.ReadHeaderTimeout <= 0 || config.ReadTimeout <= 0 ||
		config.WriteTimeout <= 0 || config.IdleTimeout <= 0 {
		return Config{}, fmt.Errorf("%w: timeouts and token lifetime must be positive and bounded", ErrConfiguration)
	}
	for name, limits := range map[string]RouteLimits{
		"catalog": config.Catalog, "query": config.Query, "native replay": config.NativeReplay, "normalized replay": config.NormalizedReplay,
	} {
		if err := validateRouteLimits(name, limits); err != nil {
			return Config{}, err
		}
	}
	seenIDs := make(map[string]struct{}, len(config.Principals))
	for _, principal := range config.Principals {
		if !validConfigText(principal.ID) || !validConfigText(principal.TokenRef) {
			return Config{}, fmt.Errorf("%w: principal ID and token reference are required", ErrConfiguration)
		}
		if _, exists := seenIDs[principal.ID]; exists {
			return Config{}, fmt.Errorf("%w: duplicate principal ID", ErrConfiguration)
		}
		seenIDs[principal.ID] = struct{}{}
		seenScopes := make(map[Scope]struct{}, len(principal.Scopes))
		for _, scope := range principal.Scopes {
			if _, ok := scopeMask(scope); !ok {
				return Config{}, fmt.Errorf("%w: unknown authorization scope", ErrConfiguration)
			}
			if _, exists := seenScopes[scope]; exists {
				return Config{}, fmt.Errorf("%w: duplicate authorization scope", ErrConfiguration)
			}
			seenScopes[scope] = struct{}{}
		}
	}
	return config, nil
}

func normalizeRouteLimits(value, defaults RouteLimits) RouteLimits {
	if value.QueueDepth == 0 {
		value.QueueDepth = defaults.QueueDepth
	}
	if value.Concurrency == 0 {
		value.Concurrency = defaults.Concurrency
	}
	if value.Deadline == 0 {
		value.Deadline = defaults.Deadline
	}
	if value.MaxDuration == 0 {
		value.MaxDuration = defaults.MaxDuration
	}
	if value.MaxBytes == 0 {
		value.MaxBytes = defaults.MaxBytes
	}
	if value.BufferBytes == 0 {
		value.BufferBytes = defaults.BufferBytes
	}
	return value
}

func validateRouteLimits(name string, limits RouteLimits) error {
	if limits.QueueDepth < 1 || limits.QueueDepth > 4096 || limits.Concurrency < 1 || limits.Concurrency > 256 ||
		limits.Deadline <= 0 || limits.MaxDuration <= 0 || limits.MaxBytes < 1 ||
		limits.BufferBytes < 1 || limits.BufferBytes > 1<<20 {
		return fmt.Errorf("%w: %s queue, concurrency, deadline, duration, byte, or buffer bound", ErrConfiguration, name)
	}
	return nil
}

func validConfigText(value string) bool {
	return value != "" && len(value) <= 4096 && utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}
