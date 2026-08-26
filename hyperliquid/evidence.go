package hyperliquid

import (
	"crypto/sha256"
	"encoding/json"
	"slices"
)

// RawEvidence is immutable inside the adapter. Multiple observations parsed
// from one source message share one evidence allocation; Bytes returns a clone
// so callers cannot alter the stored source bytes.
type RawEvidence struct {
	payload []byte
	digest  [sha256.Size]byte
}

func newRawEvidence(payload []byte) (*RawEvidence, error) {
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) {
		return nil, ErrInvalidPayload
	}
	clone := slices.Clone(payload)
	return &RawEvidence{payload: clone, digest: sha256.Sum256(clone)}, nil
}

func (e *RawEvidence) Bytes() []byte {
	if e == nil {
		return nil
	}
	return slices.Clone(e.payload)
}

func (e *RawEvidence) SHA256() [sha256.Size]byte {
	if e == nil {
		return [sha256.Size]byte{}
	}
	return e.digest
}

func (e *RawEvidence) ByteLength() int {
	if e == nil {
		return 0
	}
	return len(e.payload)
}

func (e *RawEvidence) Valid() bool {
	return e != nil && len(e.payload) > 0 && len(e.payload) <= MaxRawPayloadBytes && json.Valid(e.payload) && sha256.Sum256(e.payload) == e.digest
}
