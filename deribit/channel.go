package deribit

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type ChannelRequest struct {
	Role       SourceRole
	Instrument string
	IndexName  string
	Kind       string
	Currency   string
	Group      string
	Depth      int
}

func (r ChannelRequest) Channel(policy CadencePolicy) (string, error) {
	if err := policy.Validate(); err != nil {
		return "", err
	}
	cadence := string(policy.Requested)
	switch r.Role {
	case RoleBook, RoleTrade, RoleTicker, RoleFunding:
		if !validSegment(r.Instrument, false) || r.IndexName != "" || r.Kind != "" || r.Currency != "" || r.Group != "" || r.Depth != 0 {
			return "", ErrInvalidChannel
		}
		prefix := map[SourceRole]string{RoleBook: "book", RoleTrade: "trades", RoleTicker: "ticker", RoleFunding: "perpetual"}[r.Role]
		return prefix + "." + r.Instrument + "." + cadence, nil
	case RoleQuote:
		if !validSegment(r.Instrument, false) || r.IndexName != "" || r.Kind != "" || r.Currency != "" || r.Group != "" || r.Depth != 0 {
			return "", ErrInvalidChannel
		}
		return "quote." + r.Instrument, nil
	case RoleIndex:
		if !validSegment(r.IndexName, true) || r.Instrument != "" || r.Kind != "" || r.Currency != "" || r.Group != "" || r.Depth != 0 {
			return "", ErrInvalidChannel
		}
		return "deribit_price_index." + r.IndexName, nil
	case RoleInstrumentCreation, RoleInstrumentState:
		if !validKind(r.Kind) || !validCurrency(r.Currency) || r.Instrument != "" || r.IndexName != "" || r.Group != "" || r.Depth != 0 {
			return "", ErrInvalidChannel
		}
		middle := "state"
		if r.Role == RoleInstrumentCreation {
			middle = "creation"
		}
		return "instrument." + middle + "." + r.Kind + "." + r.Currency, nil
	case RoleGroupedBookView:
		if !validSegment(r.Instrument, false) || !validGroup(r.Group) || (r.Depth != 1 && r.Depth != 10 && r.Depth != 20) ||
			r.IndexName != "" || r.Kind != "" || r.Currency != "" {
			return "", ErrInvalidChannel
		}
		return "book." + r.Instrument + "." + r.Group + "." + strconv.Itoa(r.Depth) + "." + cadence, nil
	default:
		return "", fmt.Errorf("%w: unsupported role %s", ErrInvalidChannel, r.Role)
	}
}

func Channels(policy CadencePolicy, requests []ChannelRequest) ([]string, error) {
	if len(requests) == 0 || len(requests) > MaxSubscriptions {
		return nil, fmt.Errorf("%w: subscription count outside 1..%d", ErrInvalidChannel, MaxSubscriptions)
	}
	channels := make([]string, 0, len(requests))
	seen := make(map[string]struct{}, len(requests))
	for _, request := range requests {
		channel, err := request.Channel(policy)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[channel]; exists {
			return nil, fmt.Errorf("%w: duplicate channel", ErrInvalidChannel)
		}
		seen[channel] = struct{}{}
		channels = append(channels, channel)
	}
	slices.Sort(channels)
	return channels, nil
}

func validSegment(value string, lower bool) bool {
	if value == "" || len(value) > 128 || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, r := range value {
		if r >= '0' && r <= '9' || r == '-' || r == '_' {
			continue
		}
		if lower && r >= 'a' && r <= 'z' {
			continue
		}
		if !lower && (r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z') {
			continue
		}
		return false
	}
	return true
}

func validKind(kind string) bool {
	switch kind {
	case "future", "option", "spot", "future_combo", "option_combo", "any":
		return true
	default:
		return false
	}
}

func validCurrency(currency string) bool {
	if currency == "any" {
		return true
	}
	if currency == "" || len(currency) > 16 {
		return false
	}
	for _, r := range currency {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

func validGroup(group string) bool {
	if group == "none" {
		return true
	}
	if group == "" || len(group) > 16 || strings.Count(group, ".") > 1 {
		return false
	}
	for _, r := range group {
		if r != '.' && (r < '0' || r > '9') {
			return false
		}
	}
	return group[0] != '.' && group[len(group)-1] != '.'
}
