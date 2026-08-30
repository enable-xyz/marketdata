package serve

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestResolveConfigRequiresThirtyTwoByteBearer(t *testing.T) {
	if MinimumBearerBytes != 32 {
		t.Fatalf("MinimumBearerBytes = %d, want 32", MinimumBearerBytes)
	}
	material, _, _, err := newX6Material()
	if err != nil {
		t.Fatal(err)
	}
	config := x6Config()
	config.Principals = []Principal{{ID: "boundary", TokenRef: "boundary-token", Scopes: []Scope{ScopeCatalogRead}}}
	for size := range MinimumBearerBytes {
		t.Run("reject-"+strconv.Itoa(size), func(t *testing.T) {
			token := bytes.Repeat([]byte{0xa5}, size)
			material["boundary-token"] = token
			resolved, resolveErr := resolveConfig(t.Context(), config, material)
			if !errors.Is(resolveErr, ErrConfiguration) {
				wipeResolvedConfig(&resolved)
				t.Fatalf("resolveConfig() error = %v, want ErrConfiguration for %d-byte bearer", resolveErr, size)
			}
			if resolved.certificate.PrivateKey != nil || resolved.certificate.Certificate != nil ||
				resolved.pagingKey != nil || resolved.auth.principals != nil {
				wipeResolvedConfig(&resolved)
				t.Fatal("resolveConfig returned secret state after rejecting a short bearer")
			}
			if size > 0 && strings.Contains(resolveErr.Error(), string(token)) {
				t.Fatal("resolveConfig error disclosed rejected bearer material")
			}
		})
	}

	token := strings.Repeat("t", MinimumBearerBytes)
	material["boundary-token"] = []byte(token)
	resolved, err := resolveConfig(t.Context(), config, material)
	if err != nil {
		t.Fatalf("resolveConfig() rejected %d-byte bearer: %v", MinimumBearerBytes, err)
	}
	defer wipeResolvedConfig(&resolved)
	if resolved.certificate.PrivateKey == nil || len(resolved.certificate.Certificate) == 0 {
		t.Fatal("resolveConfig wiped parsed TLS ownership on success")
	}
	if _, err := resolved.auth.authenticate([]string{"Bearer " + token}); err != nil {
		t.Fatalf("authenticate() rejected %d-byte configured bearer: %v", MinimumBearerBytes, err)
	}
	if _, err := resolved.auth.authenticate([]string{"Bearer " + token[:MinimumBearerBytes-1]}); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("authenticate() short bearer error = %v, want ErrAuthentication", err)
	}
}

func TestResolveParsedConfigWipesTLSOnFailure(t *testing.T) {
	config := x6Config()
	config.Principals = []Principal{{ID: "wipe", TokenRef: "token", Scopes: []Scope{ScopeCatalogRead}}}
	tests := []struct {
		name     string
		material x6SecretResolver
	}{
		{name: "paging resolution", material: x6SecretResolver{}},
		{name: "bearer resolution", material: x6SecretResolver{
			"paging": bytes.Repeat([]byte{'p'}, 32),
			"token":  bytes.Repeat([]byte{'t'}, MinimumBearerBytes-1),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			certificateDER := bytes.Repeat([]byte{0xa5}, 32)
			privateKey := ed25519.PrivateKey(bytes.Repeat([]byte{0x5a}, ed25519.PrivateKeySize))
			certificate := tls.Certificate{
				Certificate: [][]byte{certificateDER},
				PrivateKey:  privateKey,
			}
			resolved, err := resolveParsedConfig(t.Context(), config, test.material, &certificate)
			if !errors.Is(err, ErrConfiguration) {
				wipeResolvedConfig(&resolved)
				t.Fatalf("resolveParsedConfig() error = %v, want ErrConfiguration", err)
			}
			if resolved.certificate.PrivateKey != nil || resolved.certificate.Certificate != nil ||
				resolved.pagingKey != nil || resolved.auth.principals != nil {
				wipeResolvedConfig(&resolved)
				t.Fatal("resolveParsedConfig returned secret state after failure")
			}
			if !bytes.Equal(certificateDER, make([]byte, len(certificateDER))) ||
				!bytes.Equal(privateKey, make([]byte, len(privateKey))) {
				t.Fatal("parsed TLS certificate or private key retained bytes after failure")
			}
			if certificate.PrivateKey != nil || certificate.Certificate != nil {
				t.Fatal("failed parsed TLS certificate retained ownership")
			}
		})
	}
}
