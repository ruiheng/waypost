//go:build windows

package main

import (
	"os"
	"os/signal"
)

func suppressParentInterrupt() func() {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	return func() {
		signal.Stop(interrupts)
	}
}
