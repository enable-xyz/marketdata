package quality

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type syntheticFixtureManifest struct {
	Version            uint16                       `json:"version"`
	Provenance         string                       `json:"provenance"`
	PrimarySourceClaim bool                         `json:"primary_source_claim"`
	Fixtures           []syntheticFixtureDescriptor `json:"fixtures"`
}

type syntheticFixtureDescriptor struct {
	ID     string `json:"id"`
	Path   string `json:"path"`
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func TestFaultHarnessSyntheticFixtures(t *testing.T) {
	t.Parallel()
	root := filepath.Join("..", "testdata", "synthetic")
	manifestBytes, err := os.ReadFile(filepath.Join(root, "fixtures.json"))
	if err != nil {
		t.Fatalf("ReadFile(fixtures.json) error = %v", err)
	}
	var manifest syntheticFixtureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("Unmarshal(fixtures.json) error = %v", err)
	}
	if manifest.Version != 1 || manifest.Provenance != "synthetic" || manifest.PrimarySourceClaim {
		t.Fatalf("unsafe fixture provenance descriptor = %#v", manifest)
	}
	if len(manifest.Fixtures) == 0 || len(manifest.Fixtures) > MaxFaultScriptEvents {
		t.Fatalf("fixture count = %d", len(manifest.Fixtures))
	}
	seen := make(map[string]struct{}, len(manifest.Fixtures))
	for _, fixture := range manifest.Fixtures {
		fixture := fixture
		t.Run(fixture.ID, func(t *testing.T) {
			t.Parallel()
			if !strings.HasPrefix(fixture.ID, "synthetic.") {
				t.Fatalf("fixture ID %q claims no synthetic namespace", fixture.ID)
			}
			if filepath.Base(fixture.Path) != fixture.Path {
				t.Fatalf("fixture path %q escapes synthetic fixture root", fixture.Path)
			}
			data, err := os.ReadFile(filepath.Join(root, fixture.Path))
			if err != nil {
				t.Fatalf("ReadFile(%s) error = %v", fixture.Path, err)
			}
			if len(data) != fixture.Bytes {
				t.Fatalf("fixture bytes = %d, descriptor says %d", len(data), fixture.Bytes)
			}
			want, err := hex.DecodeString(fixture.SHA256)
			if err != nil || len(want) != sha256.Size {
				t.Fatalf("fixture SHA-256 %q is invalid", fixture.SHA256)
			}
			got := sha256.Sum256(data)
			if !strings.EqualFold(hex.EncodeToString(got[:]), fixture.SHA256) {
				t.Fatalf("fixture SHA-256 = %x, want %x", got, want)
			}
		})
		if _, exists := seen[fixture.ID]; exists {
			t.Fatalf("duplicate fixture identity %q", fixture.ID)
		}
		seen[fixture.ID] = struct{}{}
	}
}
