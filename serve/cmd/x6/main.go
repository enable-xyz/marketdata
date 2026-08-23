package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/enable-xyz/marketdata/serve"
)

func main() {
	check := flag.String("check", "", "compare the measured result with a frozen X6 JSON fixture")
	flag.Parse()
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signalContext, 2*time.Minute)
	defer cancel()
	measured, err := serve.RunX6(ctx)
	if err == nil && *check != "" {
		var file *os.File
		file, err = os.Open(*check)
		if err == nil {
			var expected serve.X6Fixture
			expected, err = serve.DecodeX6Fixture(file)
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
			if err == nil {
				err = serve.CompareX6Fixture(expected, measured)
			}
		}
	}
	if err == nil && *check == "" {
		err = serve.EncodeX6Fixture(os.Stdout, measured)
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "x6 release gate failed")
		os.Exit(1)
	}
}
