package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/bybit"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/deribit"
	"github.com/enable-xyz/marketdata/hyperliquid"
	"github.com/enable-xyz/marketdata/okx"
	"github.com/enable-xyz/marketdata/verify"
	"github.com/spf13/cobra"
)

const (
	defaultCanaryDuration       = 26 * time.Hour
	maximumCanaryDuration       = 31 * 24 * time.Hour
	defaultReconnectAttempts    = uint32(256)
	maximumReconnectAttempts    = uint32(4096)
	defaultReconnectBackoff     = 5 * time.Second
	maximumReconnectBackoff     = 10 * time.Minute
	maximumHeartbeatBound       = 10 * time.Minute
	maximumCanaryMessageBytes   = uint32(4 << 20)
	maximumReceiptBytes         = 1 << 20
	maximumOutputPathBytes      = 4096
	maximumCanaryIdentifierSize = 256
)

var errCanaryUnsuccessful = errors.New("public canary did not complete its planned duration")

type canaryRunner func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error)

type commandDependencies struct {
	run           canaryRunner
	newClock      func(string) (*verify.SystemClock, error)
	newEpochID    func() (string, error)
	newHTTPClient func() *http.Client
	newRateBudget func(*verify.SystemClock) (verify.CanaryRateBudgets, error)
	writeReceipt  func(string, []byte) error
}

type commandOptions struct {
	selector             string
	instrument           string
	dex                  string
	duration             time.Duration
	output               string
	reconnectAttempts    uint32
	reconnectBackoff     time.Duration
	maxMessageBytes      uint32
	heartbeatInterval    time.Duration
	heartbeatTimeout     time.Duration
	subscriptionACKBound time.Duration
}

type venueBounds struct {
	maxMessageBytes      uint32
	heartbeatInterval    time.Duration
	heartbeatTimeout     time.Duration
	subscriptionACKBound time.Duration
}

func productionDependencies() commandDependencies {
	return commandDependencies{
		run:           verify.RunCanary,
		newClock:      verify.NewSystemClock,
		newEpochID:    generateEpochID,
		newHTTPClient: newBoundedHTTPClient,
		newRateBudget: newCanaryRateBudgets,
		writeReceipt:  writeReceiptExclusive,
	}
}

func newCanaryCommand(dependencies commandDependencies) *cobra.Command {
	options := commandOptions{
		duration:          defaultCanaryDuration,
		reconnectAttempts: defaultReconnectAttempts,
		reconnectBackoff:  defaultReconnectBackoff,
	}
	command := &cobra.Command{
		Use:           "public-canary",
		Short:         "Run one bounded public market-data canary",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(command *cobra.Command, _ []string) error {
			return runCanaryCommand(command.Context(), command, dependencies, options)
		},
	}
	flags := command.Flags()
	flags.StringVar(&options.selector, "selector", "", "public canary selector")
	flags.StringVar(&options.instrument, "instrument", "", "one explicit venue-native instrument")
	flags.StringVar(&options.dex, "dex", "", "HIP-3 DEX identity (valid only with hyperliquid-hip3)")
	flags.DurationVar(&options.duration, "duration", defaultCanaryDuration, "bounded canary duration")
	flags.StringVarP(&options.output, "output", "o", "", "create-exclusive JSON receipt path")
	flags.Uint32Var(&options.reconnectAttempts, "reconnect-max-attempts", defaultReconnectAttempts, "maximum reconnect attempts")
	flags.DurationVar(&options.reconnectBackoff, "reconnect-backoff", defaultReconnectBackoff, "bounded delay before each reconnect")
	flags.Uint32Var(&options.maxMessageBytes, "max-message-bytes", 0, "message byte cap (default: selected venue contract)")
	flags.DurationVar(&options.heartbeatInterval, "heartbeat-interval", 0, "heartbeat interval (default: selected venue contract)")
	flags.DurationVar(&options.heartbeatTimeout, "heartbeat-timeout", 0, "heartbeat response bound (default: selected venue contract)")
	flags.DurationVar(&options.subscriptionACKBound, "subscription-ack-timeout", 0, "subscription acknowledgement bound (default: selected venue contract)")
	_ = command.MarkFlagRequired("selector")
	_ = command.MarkFlagRequired("instrument")
	_ = command.MarkFlagRequired("output")
	return command
}

func runCanaryCommand(ctx context.Context, command *cobra.Command, dependencies commandDependencies, options commandOptions) error {
	if err := validateDependencies(dependencies); err != nil {
		return err
	}
	if err := validateIdentity("selector", options.selector, false); err != nil {
		return err
	}
	if err := validateIdentity("instrument", options.instrument, false); err != nil {
		return err
	}
	if err := validateIdentity("DEX", options.dex, true); err != nil {
		return err
	}
	if options.selector == verify.CanarySelectorHyperliquidHIP3 {
		if options.dex == "" {
			return errors.New("hyperliquid-hip3 requires --dex")
		}
	} else if options.dex != "" {
		return errors.New("--dex is valid only with hyperliquid-hip3")
	}
	if _, err := verify.CanaryDriver(options.selector); err != nil {
		return err
	}
	if err := applyVenueDefaults(command, &options); err != nil {
		return err
	}
	if err := validateBounds(options); err != nil {
		return err
	}
	outputPath, err := validateOutputPath(options.output)
	if err != nil {
		return fmt.Errorf("invalid receipt output: %w", err)
	}
	epochID, err := dependencies.newEpochID()
	if err != nil {
		return fmt.Errorf("generate clock epoch identifier: %w", err)
	}
	if err := validateIdentity("clock epoch", epochID, false); err != nil {
		return err
	}
	clock, err := dependencies.newClock(epochID)
	if err != nil {
		return fmt.Errorf("create system clock: %w", err)
	}
	reading := clock.Read()
	durationNS := uint64(options.duration)
	if durationNS > math.MaxUint64-reading.MonotonicNS ||
		reading.WallTimeNS > math.MaxInt64-int64(options.duration) {
		return errors.New("canary duration overflows the system clock interval")
	}
	client := dependencies.newHTTPClient()
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return errors.New("explicit bounded HTTP client is required")
	}
	budgets, err := dependencies.newRateBudget(clock)
	if err != nil {
		return fmt.Errorf("create public rate budgets: %w", err)
	}
	config := verify.CanaryConfig{
		Selector:        options.selector,
		Instrument:      options.instrument,
		DEX:             options.dex,
		DurationNS:      durationNS,
		Reconnect:       verify.CanaryReconnectPolicy{MaxAttempts: options.reconnectAttempts, BackoffNS: uint64(options.reconnectBackoff)},
		MaxMessageBytes: options.maxMessageBytes,
		Heartbeat: verify.CanaryHeartbeatSchedule{
			IntervalNS:   uint64(options.heartbeatInterval),
			TimeoutNS:    uint64(options.heartbeatTimeout),
			ACKTimeoutNS: uint64(options.subscriptionACKBound),
		},
		Clock:       clock,
		HTTPClient:  client,
		RateBudgets: budgets,
	}
	receipt, runErr := dependencies.run(ctx, config)
	validationErr := validateInvocationReceipt(receipt, options.selector)
	if runErr != nil {
		if validationErr == nil {
			if err := persistReceipt(dependencies.writeReceipt, outputPath, receipt); err != nil {
				return errors.Join(runErr, err)
			}
		}
		return runErr
	}
	if validationErr != nil {
		return validationErr
	}
	if err := persistReceipt(dependencies.writeReceipt, outputPath, receipt); err != nil {
		return err
	}
	if err := validateSuccessfulReceipt(receipt, durationNS); err != nil {
		return err
	}
	return nil
}

func validateDependencies(dependencies commandDependencies) error {
	if dependencies.run == nil || dependencies.newClock == nil || dependencies.newEpochID == nil ||
		dependencies.newHTTPClient == nil || dependencies.newRateBudget == nil || dependencies.writeReceipt == nil {
		return errors.New("canary command dependencies are incomplete")
	}
	return nil
}

func applyVenueDefaults(command *cobra.Command, options *commandOptions) error {
	defaults, ok := defaultBoundsForSelector(options.selector)
	if !ok {
		for _, name := range []string{"max-message-bytes", "heartbeat-interval", "heartbeat-timeout", "subscription-ack-timeout"} {
			if !command.Flags().Changed(name) {
				return fmt.Errorf("--%s is required for selector %q because it has no command-owned default", name, options.selector)
			}
		}
		return nil
	}
	if !command.Flags().Changed("max-message-bytes") {
		options.maxMessageBytes = defaults.maxMessageBytes
	}
	if !command.Flags().Changed("heartbeat-interval") {
		options.heartbeatInterval = defaults.heartbeatInterval
	}
	if !command.Flags().Changed("heartbeat-timeout") {
		options.heartbeatTimeout = defaults.heartbeatTimeout
	}
	if !command.Flags().Changed("subscription-ack-timeout") {
		options.subscriptionACKBound = defaults.subscriptionACKBound
	}
	return nil
}

func defaultBoundsForSelector(selector string) (venueBounds, bool) {
	switch selector {
	case verify.CanarySelectorBinanceSpot:
		return venueBounds{
			maxMessageBytes:      binance.SpotMaxRawPayloadBytes,
			heartbeatInterval:    time.Duration(binance.SpotPingIntervalNS),
			heartbeatTimeout:     time.Duration(binance.SpotPongDeadlineNS),
			subscriptionACKBound: time.Duration(binance.SpotACKDeadlineNS),
		}, true
	case verify.CanarySelectorBinanceUSDM, verify.CanarySelectorBinanceUSDMPublic, verify.CanarySelectorBinanceUSDMMarket:
		contract := binance.USDMPublicSourceContract()
		return venueBounds{
			maxMessageBytes:      binance.USDMMaxRawPayloadBytes,
			heartbeatInterval:    time.Duration(contract.Heartbeat.IntervalNS),
			heartbeatTimeout:     time.Duration(contract.Heartbeat.TimeoutNS),
			subscriptionACKBound: 10 * time.Second,
		}, true
	case verify.CanarySelectorBinanceCoinM:
		contract := binance.CoinMSourceContract()
		return venueBounds{
			maxMessageBytes:      binance.CoinMMaxRawPayloadBytes,
			heartbeatInterval:    time.Duration(contract.Heartbeat.IntervalNS),
			heartbeatTimeout:     time.Duration(contract.Heartbeat.TimeoutNS),
			subscriptionACKBound: 10 * time.Second,
		}, true
	case verify.CanarySelectorBybitSpot, verify.CanarySelectorBybitLinear, verify.CanarySelectorBybitInverse, verify.CanarySelectorBybitOption:
		return venueBounds{
			maxMessageBytes:      bybit.MaxRawPayloadBytes,
			heartbeatInterval:    time.Duration(bybit.HeartbeatIntervalNS),
			heartbeatTimeout:     time.Duration(bybit.HeartbeatTimeoutNS),
			subscriptionACKBound: time.Duration(bybit.SubscriptionACKTimeoutNS),
		}, true
	case verify.CanarySelectorOKXSpot, verify.CanarySelectorOKXSwap, verify.CanarySelectorOKXFutures, verify.CanarySelectorOKXOption:
		return venueBounds{
			maxMessageBytes:      okx.MaxRawPayloadBytes,
			heartbeatInterval:    time.Duration(okx.HeartbeatIntervalNS),
			heartbeatTimeout:     time.Duration(okx.HeartbeatTimeoutNS),
			subscriptionACKBound: time.Duration(okx.SubscriptionACKTimeoutNS),
		}, true
	case verify.CanarySelectorDeribitPublic100MS:
		return venueBounds{
			maxMessageBytes:      deribit.MaxRawPayloadBytes,
			heartbeatInterval:    time.Duration(deribit.HeartbeatIntervalNS),
			heartbeatTimeout:     time.Duration(deribit.HeartbeatTimeoutNS),
			subscriptionACKBound: time.Duration(deribit.SubscriptionTimeoutNS),
		}, true
	case verify.CanarySelectorHyperliquidMain, verify.CanarySelectorHyperliquidSpot, verify.CanarySelectorHyperliquidHIP3:
		return venueBounds{
			maxMessageBytes:      hyperliquid.MaxRawPayloadBytes,
			heartbeatInterval:    time.Duration(hyperliquid.HeartbeatIntervalNS),
			heartbeatTimeout:     time.Duration(hyperliquid.HeartbeatTimeoutNS),
			subscriptionACKBound: time.Duration(hyperliquid.SubscriptionACKTimeoutNS),
		}, true
	default:
		return venueBounds{}, false
	}
}

func validateBounds(options commandOptions) error {
	if options.duration <= 0 || options.duration > maximumCanaryDuration {
		return fmt.Errorf("duration must be positive and no greater than %s", maximumCanaryDuration)
	}
	if options.reconnectAttempts == 0 || options.reconnectAttempts > maximumReconnectAttempts {
		return fmt.Errorf("reconnect maximum must be between 1 and %d", maximumReconnectAttempts)
	}
	if options.reconnectBackoff <= 0 || options.reconnectBackoff > maximumReconnectBackoff {
		return fmt.Errorf("reconnect backoff must be positive and no greater than %s", maximumReconnectBackoff)
	}
	if options.maxMessageBytes == 0 || options.maxMessageBytes > maximumCanaryMessageBytes {
		return fmt.Errorf("message byte cap must be between 1 and %d", maximumCanaryMessageBytes)
	}
	for name, value := range map[string]time.Duration{
		"heartbeat interval":       options.heartbeatInterval,
		"heartbeat timeout":        options.heartbeatTimeout,
		"subscription ACK timeout": options.subscriptionACKBound,
	} {
		if value <= 0 || value > maximumHeartbeatBound {
			return fmt.Errorf("%s must be positive and no greater than %s", name, maximumHeartbeatBound)
		}
	}
	return nil
}

func validateIdentity(name, value string, optional bool) error {
	if value == "" && optional {
		return nil
	}
	if strings.TrimSpace(value) == "" || len(value) > maximumCanaryIdentifierSize || !utf8.ValidString(value) {
		return fmt.Errorf("%s must be nonempty, valid UTF-8, and at most %d bytes", name, maximumCanaryIdentifierSize)
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return fmt.Errorf("%s contains a control character", name)
		}
	}
	return nil
}

func validateInvocationReceipt(receipt verify.CanaryReceipt, selector string) error {
	if receipt.Selector != selector {
		return errors.New("canary runtime returned a receipt for a different selector")
	}
	if err := verify.ValidateCanaryReceipt(receipt); err != nil {
		return fmt.Errorf("canary runtime returned an invalid receipt: %w", err)
	}
	return nil
}

func validateSuccessfulReceipt(receipt verify.CanaryReceipt, requestedDurationNS uint64) error {
	if receipt.TerminalReason != verify.CanaryTerminalPlannedDuration ||
		len(receipt.UnexplainedIntervals) != 0 ||
		receipt.SubscriptionsRequested != receipt.SubscriptionsACKed ||
		receipt.HeartbeatsSent != receipt.HeartbeatsACKed ||
		receipt.DurationNS < requestedDurationNS {
		return fmt.Errorf("%w: terminal reason %q, unexplained intervals %d", errCanaryUnsuccessful, receipt.TerminalReason, len(receipt.UnexplainedIntervals))
	}
	return nil
}

func persistReceipt(writer func(string, []byte) error, path string, receipt verify.CanaryReceipt) error {
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("marshal canary receipt: %w", err)
	}
	if len(encoded)+1 > maximumReceiptBytes {
		return fmt.Errorf("canary receipt exceeds %d bytes", maximumReceiptBytes)
	}
	encoded = append(encoded, '\n')
	if err := writer(path, encoded); err != nil {
		return fmt.Errorf("write canary receipt: %w", err)
	}
	return nil
}

func generateEpochID() (string, error) {
	var random [16]byte
	if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
		return "", err
	}
	return "public-canary-" + hex.EncodeToString(random[:]), nil
}

func newBoundedHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.ExpectContinueTimeout = time.Second
	transport.IdleConnTimeout = 90 * time.Second
	transport.MaxIdleConns = 16
	transport.MaxIdleConnsPerHost = 4
	transport.MaxConnsPerHost = 8
	transport.MaxResponseHeaderBytes = 1 << 20
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}
}

func newCanaryRateBudgets(clock *verify.SystemClock) (verify.CanaryRateBudgets, error) {
	if clock == nil {
		return verify.CanaryRateBudgets{}, errors.New("system clock is required")
	}
	now := clock.Read().MonotonicNS
	binanceDerivatives, err := capture.NewTokenRateBudget(binance.USDMPublicSourceContract().Rate, now)
	if err != nil {
		return verify.CanaryRateBudgets{}, err
	}
	okxHandshake, err := okx.NewHandshakeBudget(now)
	if err != nil {
		return verify.CanaryRateBudgets{}, err
	}
	hyperliquidMessages, err := hyperliquid.NewWeightedLimiter(hyperliquid.MaxOutboundMessagesPerMinute, time.Minute, clock)
	if err != nil {
		return verify.CanaryRateBudgets{}, err
	}
	hyperliquidConnections, err := hyperliquid.NewWeightedLimiter(hyperliquid.MaxConnectionAttemptsPerMinute, time.Minute, clock)
	if err != nil {
		return verify.CanaryRateBudgets{}, err
	}
	return verify.CanaryRateBudgets{
		BinanceDerivatives:     binanceDerivatives,
		OKXHandshake:           okxHandshake,
		HyperliquidMessages:    hyperliquidMessages,
		HyperliquidConnections: hyperliquidConnections,
	}, nil
}

func validateOutputPath(path string) (string, error) {
	if path == "" || len(path) > maximumOutputPathBytes || strings.IndexByte(path, 0) >= 0 {
		return "", errors.New("output path is empty or oversized")
	}
	if filepath.Clean(path) != path || filepath.Base(path) == "." || filepath.Base(path) == string(filepath.Separator) {
		return "", errors.New("output path must be canonical and name one file")
	}
	for _, component := range strings.Split(filepath.ToSlash(path), "/") {
		if component == ".." {
			return "", errors.New("output path must not traverse a parent directory")
		}
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if info, statErr := os.Lstat(absolute); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("output path is a symlink")
		}
		return "", errors.New("output path already exists")
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", statErr
	}
	if err := validateRealDirectoryChain(filepath.Dir(absolute)); err != nil {
		return "", err
	}
	return absolute, nil
}

func validateRealDirectoryChain(directory string) error {
	for current := directory; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("output directory component %q is not a real directory", current)
		}
		next := filepath.Dir(current)
		if next == current {
			return nil
		}
	}
}

func writeReceiptExclusive(path string, data []byte) (err error) {
	validated, err := validateOutputPath(path)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(validated, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	created, statErr := file.Stat()
	if statErr != nil || !created.Mode().IsRegular() {
		_ = file.Close()
		removeCreatedFile(validated, created)
		if statErr != nil {
			return statErr
		}
		return errors.New("created receipt is not a regular file")
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = file.Close()
		removeCreatedFile(validated, created)
	}()
	if _, err = file.Write(data); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	listed, err := os.Lstat(validated)
	if err != nil || listed.Mode()&os.ModeSymlink != 0 || !listed.Mode().IsRegular() || !os.SameFile(created, listed) {
		if err != nil {
			return err
		}
		return errors.New("receipt path changed while writing")
	}
	if err = validateRealDirectoryChain(filepath.Dir(validated)); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(validated))
	if err != nil {
		return err
	}
	if err = directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	if err = directory.Close(); err != nil {
		return err
	}
	committed = true
	return nil
}

func removeCreatedFile(path string, created os.FileInfo) {
	if created == nil {
		return
	}
	listed, err := os.Lstat(path)
	if err == nil && listed.Mode()&os.ModeSymlink == 0 && os.SameFile(created, listed) {
		_ = os.Remove(path)
	}
}
