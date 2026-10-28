package nodeapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type lateReportMsg struct {
	RequestID         string `json:"request_id"`
	AttemptN          int    `json:"attempt_n"`
	Function          string `json:"function"`
	NodeID            string `json:"node_id"`
	Outcome           string `json:"outcome"`
	ObservedLatencyMS int64  `json:"observed_latency_ms"`
	StartedAt         string `json:"started_at"`
	// ColdStart carries the signal of the pool, which provisioned during
	// this call, for the late report. It is omitted when empty, which
	// keeps the wire compatible: an older arbiter ignores the unknown
	// key and the warm case does not even send it, so this is an additive
	// extension of the schema of the late report.
	ColdStart bool `json:"cold_start,omitempty"`
}

const lateQueueCap = 1024
const lateSendTimeout = 2 * time.Second

// ConfigureLateReports enables the sender of the late reports in the
// continuum mode, called by the agent when the address of the arbiter is
// configured.
//
// A standalone node never calls it: the late queue stays empty and the
// behavior of the invocation is unchanged, with no detachment and no
// sending. A non-positive queue capacity selects the default, and a
// non-positive late capacity means no extra limit beyond the timeout of
// the node.
func (s *Server) ConfigureLateReports(url, token, nodeID string, queueCap int, lateCap time.Duration) {
	s.configureLateSender(url, token, nodeID, queueCap, lateCap)
}

func (s *Server) configureLateSender(url, token, nodeID string, queueCap int, lateCap time.Duration) {
	if queueCap <= 0 {
		queueCap = lateQueueCap
	}
	s.lateURL = url
	s.lateToken = token
	s.nodeID = nodeID
	s.lateCap = lateCap
	s.lateClient = &http.Client{Timeout: lateSendTimeout}
	s.lateQueue = make(chan lateReportMsg, queueCap)
	go s.lateSenderLoop()
}

// detachAllowed is the rule of admission of the orphans: the request
// detaches with an active sender and with room in the late report queue.
// A standalone node never detaches, which preserves the legacy behavior.
//
// The shedding based on the depth of the queue was removed, because it
// was self-contradictory: the pool fills exactly when the attempts are
// being cut, which is when the late report is the only sample of the real
// latency that the selector can receive, since a cut attempt does not
// measure the node. Under load the learning was therefore the first thing
// discarded, and a large number of attempts were left without any sample
// at all.
//
// The limits that remain are the ceiling of the late queue, the minimum
// of the timeout of the node and the late capacity in the detached
// context, and the admission of the pool itself, since the orphan waits
// and expires like any other invocation.
func (s *Server) detachAllowed() bool {
	q := s.lateQueue
	if q == nil {
		return false
	}
	return len(q) < cap(q)
}

func (s *Server) enqueueLateReport(msg lateReportMsg) {
	if s.lateQueue == nil {
		return
	}
	select {
	case s.lateQueue <- msg:
	default:
		atomic.AddInt64(&s.lateDropped, 1)
		log.Printf("nodeapi: late-report queue full, dropped report for fn=%s request_id=%s", msg.Function, msg.RequestID)
	}
}

func (s *Server) lateSenderLoop() {
	for msg := range s.lateQueue {
		s.sendLateReport(msg)
	}
}

func (s *Server) sendLateReport(msg lateReportMsg) {
	body, err := json.Marshal(msg)
	if err != nil {
		log.Printf("nodeapi: falha ao serializar late-report: %v", err)
		return
	}
	if s.postLateReport(body) == nil {
		return
	}
	// The sending is best-effort with a single immediate retry. Beyond
	// that, only the drop is counted, with no retry queue and no
	// reliable delivery.
	if s.postLateReport(body) != nil {
		atomic.AddInt64(&s.lateDropped, 1)
		log.Printf("nodeapi: LATE_DROPPED fn=%s request_id=%s attempt_n=%d late_dropped_total=%d", msg.Function, msg.RequestID, msg.AttemptN, atomic.LoadInt64(&s.lateDropped))
	}
}

func (s *Server) postLateReport(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, s.lateURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("nodeapi: falha ao criar request late-report: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.lateToken)
	resp, err := s.lateClient.Do(req)
	if err != nil {
		log.Printf("nodeapi: falha ao enviar late-report para %s: %v", s.lateURL, err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("nodeapi: late-report recebeu status %d", resp.StatusCode)
		return fmt.Errorf("nodeapi: late-report status %d", resp.StatusCode)
	}
	return nil
}
