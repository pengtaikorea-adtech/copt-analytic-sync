package main

import (
	"testing"
	"time"
)

// TestStartAndStopTheService windows service start/stop
func TestStartAndStopTheService(t *testing.T) {
	srv := NewService()
	srv.Start()

	timer := time.NewTimer(5 * time.Second)
	//wait for the timer then stop
	<-timer.C
	srv.Stop()
}
