package hyperliquid

import (
	"crypto/sha256"
	"encoding/json"
	"slices"
)

// ReceiveEnvelope immutably binds one received payload to the active capture
// subscription at receive time. Its construction is adapter-private.
type ReceiveEnvelope struct {
	payload         []byte
	receivedAtNS    uint64
	family          Family
	dexName         string
	channel         string
	coin            string
	subscription    Subscription
	hasSubscription bool
	binding         [sha256.Size]byte
}

func (e ReceiveEnvelope) Bytes() []byte                 { return slices.Clone(e.payload) }
func (e ReceiveEnvelope) ReceivedAtMonotonicNS() uint64 { return e.receivedAtNS }
func (e ReceiveEnvelope) Channel() string               { return e.channel }
func (e ReceiveEnvelope) Coin() string                  { return e.coin }
func (e ReceiveEnvelope) DEXName() string               { return e.dexName }
func (e ReceiveEnvelope) Subscription() (Subscription, bool) {
	return e.subscription, e.hasSubscription
}

func newReceiveEnvelope(payload []byte, receivedAtNS uint64, family Family, dexName string, subscription Subscription, hasSubscription bool) (ReceiveEnvelope, error) {
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) || validateFamily(family) != nil || validateDEXName(family, dexName) != nil {
		return ReceiveEnvelope{}, ErrInvalidPayload
	}
	channel, payloadDEX, coin, subscriptionType, requiresSubscription, err := receivePayloadIdentity(payload, family, dexName)
	if err != nil {
		return ReceiveEnvelope{}, err
	}
	if requiresSubscription {
		if !hasSubscription || subscription.Type != subscriptionType || subscription.DEX != payloadDEX ||
			subscription.Coin != coin || subscription.Validate(family, dexName) != nil {
			return ReceiveEnvelope{}, ErrBookStreamMismatch
		}
	} else if hasSubscription {
		return ReceiveEnvelope{}, ErrInvalidPayload
	}
	envelope := ReceiveEnvelope{
		payload: slices.Clone(payload), receivedAtNS: receivedAtNS, family: family, dexName: dexName,
		channel: channel, coin: coin, subscription: subscription, hasSubscription: hasSubscription,
	}
	envelope.binding = receiveEnvelopeBinding(envelope)
	return envelope, nil
}

func (e ReceiveEnvelope) valid() bool {
	return len(e.payload) > 0 && json.Valid(e.payload) && e.binding != ([sha256.Size]byte{}) && e.binding == receiveEnvelopeBinding(e)
}

func (e ReceiveEnvelope) bookCaptureIdentity() (BookCaptureIdentity, error) {
	if !e.valid() || !e.hasSubscription || e.subscription.Type != SubscriptionL2Book || e.channel != "l2Book" ||
		e.dexName != e.subscription.DEX || e.coin != e.subscription.Coin {
		return BookCaptureIdentity{}, ErrBookStreamMismatch
	}
	return bookCaptureIdentity(e.family, e.dexName, e.subscription)
}

func receiveEnvelopeBinding(envelope ReceiveEnvelope) [sha256.Size]byte {
	material, err := json.Marshal(struct {
		PayloadSHA256   [sha256.Size]byte `json:"payload_sha256"`
		ReceivedAtNS    uint64            `json:"received_at_ns"`
		Family          Family            `json:"family"`
		DEXName         string            `json:"dex_name"`
		Channel         string            `json:"channel"`
		Coin            string            `json:"coin"`
		Subscription    Subscription      `json:"subscription"`
		HasSubscription bool              `json:"has_subscription"`
	}{
		PayloadSHA256: sha256.Sum256(envelope.payload), ReceivedAtNS: envelope.receivedAtNS,
		Family: envelope.family, DEXName: envelope.dexName, Channel: envelope.channel, Coin: envelope.coin,
		Subscription: envelope.subscription, HasSubscription: envelope.hasSubscription,
	})
	if err != nil {
		return [sha256.Size]byte{}
	}
	return sha256.Sum256(material)
}

func receivePayloadIdentity(payload []byte, family Family, dexName string) (string, string, string, SubscriptionType, bool, error) {
	var message struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if json.Unmarshal(payload, &message) != nil || message.Channel == "" {
		return "", "", "", "", false, ErrInvalidPayload
	}
	switch message.Channel {
	case "l2Book", "bbo", "activeAssetCtx":
		var data struct {
			Coin string          `json:"coin"`
			DEX  json.RawMessage `json:"dex"`
		}
		if json.Unmarshal(message.Data, &data) != nil || len(data.DEX) != 0 {
			return "", "", "", "", false, ErrInvalidPayload
		}
		coin, dex, err := decodeSubscriptionWireCoin(family, dexName, data.Coin)
		if err != nil {
			return "", "", "", "", false, err
		}
		var subscriptionType SubscriptionType
		switch message.Channel {
		case "l2Book":
			subscriptionType = SubscriptionL2Book
		case "bbo":
			subscriptionType = SubscriptionBBO
		default:
			subscriptionType = SubscriptionActiveAssetCtx
		}
		return message.Channel, dex, coin, subscriptionType, true, nil
	case "trades":
		var rows []struct {
			Coin string          `json:"coin"`
			DEX  json.RawMessage `json:"dex"`
		}
		if json.Unmarshal(message.Data, &rows) != nil || len(rows) == 0 || len(rows[0].DEX) != 0 {
			return "", "", "", "", false, ErrInvalidPayload
		}
		coin, dex, err := decodeSubscriptionWireCoin(family, dexName, rows[0].Coin)
		if err != nil {
			return "", "", "", "", false, err
		}
		for _, row := range rows {
			if len(row.DEX) != 0 || row.Coin != rows[0].Coin {
				return "", "", "", "", false, ErrInvalidPayload
			}
		}
		return message.Channel, dex, coin, SubscriptionTrades, true, nil
	case "subscriptionResponse", "pong":
		return message.Channel, "", "", "", false, nil
	default:
		return "", "", "", "", false, ErrInvalidPayload
	}
}
