//go:build !debug

package main

import "log"

// StartPprof does nothing in builds without debug, since the profiler is
// excluded by the build tag.
func StartPprof(addr string) {
	if addr != "" {
		log.Println("agente: pprof disabled — compiled out for release build")
	}
}
