package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// WithHandledSignal returns a context that will be canceled when
// SIGINT or SIGTERM signals received.
func WithHandledSignal() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, syscall.SIGINT, syscall.SIGTERM)

	go func(channel <-chan os.Signal, cancel context.CancelFunc) {
		sig := <-channel
		log.Printf("signal %q received\n", sig)
		cancel()
	}(channel, cancel)

	return ctx
}
