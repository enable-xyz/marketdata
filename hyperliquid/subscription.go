package hyperliquid

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

type SubscriptionType string

const (
	SubscriptionTrades         SubscriptionType = "trades"
	SubscriptionL2Book         SubscriptionType = "l2Book"
	SubscriptionBBO            SubscriptionType = "bbo"
	SubscriptionActiveAssetCtx SubscriptionType = "activeAssetCtx"
)

type BookDepthContract struct {
	Fast     bool
	NSigFigs uint8
	Mantissa uint8
}

func (c BookDepthContract) Validate() error {
	if c.NSigFigs != 0 && (c.NSigFigs < 2 || c.NSigFigs > 5) {
		return ErrBookDepthContract
	}
	if c.Mantissa != 0 && (c.NSigFigs != 5 || (c.Mantissa != 1 && c.Mantissa != 2 && c.Mantissa != 5)) {
		return ErrBookDepthContract
	}
	return nil
}

func (c BookDepthContract) MaximumLevels() int {
	if c.Fast {
		return MaximumLevelsFast
	}
	return MaximumLevelsSlow
}

func (c BookDepthContract) Name() string {
	speed := "slow_20"
	if c.Fast {
		speed = "fast_5"
	}
	return fmt.Sprintf("hyperliquid_l2_full_snapshot_%s_nsig_%d_mantissa_%d", speed, c.NSigFigs, c.Mantissa)
}

type Subscription struct {
	Type SubscriptionType
	Coin string
	DEX  string
	Book BookDepthContract
}

func (s Subscription) Validate(family Family, dexName string) error {
	if err := validateFamily(family); err != nil {
		return err
	}
	if err := validateDEXName(family, dexName); err != nil {
		return err
	}
	if !validCoin(s.Coin) {
		return fmt.Errorf("%w: invalid coin", ErrInvalidSubscription)
	}
	switch family {
	case MainPerpetual:
		if s.DEX != "" || strings.Contains(s.Coin, ":") || strings.HasPrefix(s.Coin, "@") {
			return fmt.Errorf("%w: main-perpetual subscription must use a bare coin and no DEX", ErrInvalidSubscription)
		}
	case Spot:
		if s.DEX != "" || (!strings.HasPrefix(s.Coin, "@") && !strings.Contains(s.Coin, "/")) {
			return fmt.Errorf("%w: spot subscription must use @index or a documented pair name and no DEX", ErrInvalidSubscription)
		}
	case HIP3:
		if s.DEX != dexName || strings.Contains(s.Coin, ":") || strings.HasPrefix(s.Coin, "@") {
			return fmt.Errorf("%w: HIP-3 subscription requires the exact DEX and a bare coin", ErrInvalidSubscription)
		}
	}
	switch s.Type {
	case SubscriptionTrades, SubscriptionBBO, SubscriptionActiveAssetCtx:
		if s.Book != (BookDepthContract{}) {
			return fmt.Errorf("%w: non-book subscription has depth fields", ErrInvalidSubscription)
		}
	case SubscriptionL2Book:
		if err := s.Book.Validate(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unknown subscription type", ErrInvalidSubscription)
	}
	return nil
}

func (s Subscription) StreamIdentity() string {
	if s.Type == SubscriptionL2Book {
		return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d\x00%t", s.Type, s.DEX, s.Coin, s.Book.NSigFigs, s.Book.Mantissa, s.Book.Fast)
	}
	return string(s.Type) + "\x00" + s.DEX + "\x00" + s.Coin
}

// BookCaptureIdentity is constructed from the exact capture-side l2Book
// subscription. Its fields are opaque so a decoded snapshot cannot infer
// slow/fast semantics from the returned number of levels.
type BookCaptureIdentity struct {
	family       Family
	dexName      string
	subscription Subscription
}

func bookCaptureIdentity(family Family, dexName string, subscription Subscription) (BookCaptureIdentity, error) {
	if subscription.Type != SubscriptionL2Book || subscription.Validate(family, dexName) != nil {
		return BookCaptureIdentity{}, ErrBookDepthContract
	}
	return BookCaptureIdentity{family: family, dexName: dexName, subscription: subscription}, nil
}

func (i BookCaptureIdentity) Validate() error {
	if i.subscription.Type != SubscriptionL2Book || i.subscription.Validate(i.family, i.dexName) != nil {
		return ErrBookDepthContract
	}
	return nil
}

func (i BookCaptureIdentity) Family() Family             { return i.family }
func (i BookCaptureIdentity) DEXName() string            { return i.dexName }
func (i BookCaptureIdentity) Subscription() Subscription { return i.subscription }
func (i BookCaptureIdentity) StreamIdentity() string     { return i.subscription.StreamIdentity() }

func SubscriptionMessages(family Family, dexName string, subscriptions []Subscription) ([][]byte, error) {
	if len(subscriptions) == 0 || len(subscriptions) > MaxSubscriptions {
		return nil, fmt.Errorf("%w: subscription count outside 1..%d", ErrInvalidSubscription, MaxSubscriptions)
	}
	ordered := slices.Clone(subscriptions)
	for _, subscription := range ordered {
		if err := subscription.Validate(family, dexName); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(ordered, func(a, b Subscription) int { return strings.Compare(a.StreamIdentity(), b.StreamIdentity()) })
	messages := make([][]byte, len(ordered))
	for index, subscription := range ordered {
		if index > 0 && subscription.StreamIdentity() == ordered[index-1].StreamIdentity() {
			return nil, fmt.Errorf("%w: duplicate stream", ErrInvalidSubscription)
		}
		payload, err := encodeSubscription(family, dexName, subscription)
		if err != nil {
			return nil, err
		}
		messages[index] = payload
	}
	return messages, nil
}

func encodeSubscription(family Family, dexName string, subscription Subscription) ([]byte, error) {
	return encodeSubscriptionOperation("subscribe", family, dexName, subscription)
}

func encodeSubscriptionOperation(method string, family Family, dexName string, subscription Subscription) ([]byte, error) {
	if method != "subscribe" && method != "unsubscribe" {
		return nil, ErrInvalidSubscription
	}
	wireCoin, err := subscriptionWireCoin(family, dexName, subscription)
	if err != nil {
		return nil, err
	}
	wire := struct {
		Method       string `json:"method"`
		Subscription any    `json:"subscription"`
	}{Method: method}
	if subscription.Type == SubscriptionL2Book {
		book := struct {
			Type     SubscriptionType `json:"type"`
			Coin     string           `json:"coin"`
			NSigFigs *uint8           `json:"nSigFigs,omitempty"`
			Mantissa *uint8           `json:"mantissa,omitempty"`
			Fast     bool             `json:"fast"`
		}{Type: subscription.Type, Coin: wireCoin, Fast: subscription.Book.Fast}
		if subscription.Book.NSigFigs != 0 {
			value := subscription.Book.NSigFigs
			book.NSigFigs = &value
		}
		if subscription.Book.Mantissa != 0 {
			value := subscription.Book.Mantissa
			book.Mantissa = &value
		}
		wire.Subscription = book
	} else {
		wire.Subscription = struct {
			Type SubscriptionType `json:"type"`
			Coin string           `json:"coin"`
		}{Type: subscription.Type, Coin: wireCoin}
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%w: encode: %v", ErrInvalidSubscription, err)
	}
	return payload, nil
}

type SubscriptionACK struct {
	Subscription Subscription
	Evidence     *RawEvidence
	Method       string
}

func (a SubscriptionACK) ValidateEvidence(family Family, dexName string) error {
	if !a.Evidence.Valid() {
		return ErrInvalidPayload
	}
	reparsed, err := ParseSubscriptionACK(family, dexName, a.Evidence.Bytes())
	if err != nil || reparsed.Method != a.Method || reparsed.Subscription != a.Subscription || reparsed.Evidence.SHA256() != a.Evidence.SHA256() {
		return ErrInvalidPayload
	}
	return nil
}

func ParseSubscriptionACK(family Family, dexName string, payload []byte) (SubscriptionACK, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return SubscriptionACK{}, err
	}
	var message struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &message); err != nil || message.Channel != "subscriptionResponse" || len(message.Data) == 0 {
		return SubscriptionACK{}, ErrInvalidPayload
	}
	var envelope struct {
		Method       string          `json:"method"`
		Subscription json.RawMessage `json:"subscription"`
	}
	subscriptionRaw := message.Data
	method := "subscribe"
	if json.Unmarshal(message.Data, &envelope) == nil && envelope.Method != "" {
		if (envelope.Method != "subscribe" && envelope.Method != "unsubscribe") || len(envelope.Subscription) == 0 {
			return SubscriptionACK{}, ErrInvalidPayload
		}
		method = envelope.Method
		subscriptionRaw = envelope.Subscription
	}
	subscription, err := decodeSubscription(family, dexName, subscriptionRaw)
	if err != nil {
		return SubscriptionACK{}, ErrInvalidPayload
	}
	return SubscriptionACK{Subscription: subscription, Method: method, Evidence: evidence}, nil
}

func decodeSubscription(family Family, dexName string, raw json.RawMessage) (Subscription, error) {
	var native struct {
		Type     SubscriptionType `json:"type"`
		Coin     string           `json:"coin"`
		DEX      json.RawMessage  `json:"dex"`
		NSigFigs *uint8           `json:"nSigFigs"`
		Mantissa *uint8           `json:"mantissa"`
		Fast     *bool            `json:"fast"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &native) != nil || len(native.DEX) != 0 {
		return Subscription{}, ErrInvalidPayload
	}
	coin, dex, err := decodeSubscriptionWireCoin(family, dexName, native.Coin)
	if err != nil {
		return Subscription{}, ErrInvalidPayload
	}
	subscription := Subscription{Type: native.Type, Coin: coin, DEX: dex}
	if native.Type == SubscriptionL2Book {
		if native.NSigFigs != nil {
			subscription.Book.NSigFigs = *native.NSigFigs
		}
		if native.Mantissa != nil {
			subscription.Book.Mantissa = *native.Mantissa
		}
		if native.Fast != nil {
			subscription.Book.Fast = *native.Fast
		}
	} else if native.NSigFigs != nil || native.Mantissa != nil || native.Fast != nil {
		return Subscription{}, ErrInvalidPayload
	}
	if subscription.Validate(family, dexName) != nil {
		return Subscription{}, ErrInvalidPayload
	}
	return subscription, nil
}

func subscriptionWireCoin(family Family, dexName string, subscription Subscription) (string, error) {
	if err := subscription.Validate(family, dexName); err != nil {
		return "", err
	}
	if family == HIP3 {
		return dexName + ":" + subscription.Coin, nil
	}
	return subscription.Coin, nil
}

func decodeSubscriptionWireCoin(family Family, dexName, wireCoin string) (string, string, error) {
	if validateFamily(family) != nil || validateDEXName(family, dexName) != nil || !validCoin(wireCoin) {
		return "", "", ErrInvalidPayload
	}
	if family != HIP3 {
		return wireCoin, "", nil
	}
	prefix := dexName + ":"
	if !strings.HasPrefix(wireCoin, prefix) {
		return "", "", ErrInvalidPayload
	}
	coin := strings.TrimPrefix(wireCoin, prefix)
	if !validCoin(coin) || strings.Contains(coin, ":") || strings.HasPrefix(coin, "@") {
		return "", "", ErrInvalidPayload
	}
	return coin, dexName, nil
}

func ParsePong(payload []byte) (*RawEvidence, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return nil, err
	}
	var message struct {
		Channel string `json:"channel"`
	}
	if json.Unmarshal(payload, &message) != nil || message.Channel != "pong" {
		return nil, ErrInvalidPayload
	}
	return evidence, nil
}

func validCoin(coin string) bool {
	if coin == "" || len(coin) > 128 || strings.IndexByte(coin, 0) >= 0 {
		return false
	}
	for _, r := range coin {
		if r < 0x21 || r > 0x7e || r == '\\' || r == '"' {
			return false
		}
	}
	return true
}
