//go:build !windows

package main

func suppressParentInterrupt() func() {
	return func() {}
}
