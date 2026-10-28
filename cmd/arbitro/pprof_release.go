//go:build !debug

package main

import "log"

func StartPprof(addr string) {
	if addr != "" {
		log.Println("arbitro: pprof disabled — compiled out for release build")
	}
}
