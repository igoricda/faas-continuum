// Command analyze-runs reads invocation logs in JSON lines and computes the
// metrics of the analysis, with the same computation for the runs of the
// historical selector and of the canonical risk selector.
//
// Usage: go run ./tools/analyze-runs <file1.jsonl> [<file2.jsonl> ...]
// Output: JSON on the standard output, with one object per file plus a total.
//
// Grouping: by request identifier, where a line without one becomes a unitary
// invocation. Within a pair of request and attempt with more than one line,
// which is an arming and a settlement, the truth is the last line, so a
// superseded exceeded budget does not count twice.
//
// Metrics:
//   - invocations, attempts, which are the lines, and hedged invocations, with
//     at least two distinct attempts;
//   - hedge rate, which is the hedged invocations over the invocations;
//   - hedge win rate, which is the hedged invocations delivered by an attempt
//     other than the first, over the hedged invocations;
//   - first choice rate, which is the fraction of invocations whose first
//     attempt met the objective, where without an objective the classification
//     counts as met, and the refinement over the best bound enters with the risk
//     selector;
//   - wasted work, which is the sum of the latency of the attempts that were
//     not delivered, one per pair of invocation and attempt, excluding the
//     superseded line;
//   - the delivered percentiles over the latency of the delivered line;
//   - the edge share, which is the delivered at the edge over all delivered;
//   - the cloud lines, which are the lines of the cloud fallback, outside the
//     hedge.
//
// It has no external dependency beyond the standard library.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
)

type line struct {
	RequestID         string `json:"request_id"`
	AttemptN          int    `json:"attempt_n"`
	Armed             bool   `json:"armed"`
	Delivered         bool   `json:"delivered"`
	NodeID            string `json:"node_id"`
	Layer             string `json:"layer"`
	Outcome           string `json:"outcome"`
	ObservedLatencyMS int64  `json:"observed_latency_ms"`
	SloMet            bool   `json:"slo_met"`
}

type report struct {
	File              string  `json:"file"`
	Invokes           int     `json:"invokes"`
	Attempts          int     `json:"attempts"`
	Hedged            int     `json:"hedged"`
	HedgeRate         float64 `json:"hedge_rate"`
	HedgeWinRate      float64 `json:"hedge_win_rate"`
	FirstChoiceOKRate float64 `json:"first_choice_ok_rate"`
	WastedWorkMS      int64   `json:"wasted_work_ms"`
	DeliveredP50MS    int64   `json:"delivered_p50_ms"`
	DeliveredP95MS    int64   `json:"delivered_p95_ms"`
	DeliveredP99MS    int64   `json:"delivered_p99_ms"`
	EdgeShare         float64 `json:"edge_share"`
	CloudLines        int     `json:"cloud_lines"`
	// Diagnosis: invocations without exactly one delivered line, from an old log or from an error.
	BadDelivered int `json:"bad_delivered"`
}

func quantile(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return int64(math.Round(float64(sorted[lo])*(1-frac) + float64(sorted[hi])*frac))
}

func analyze(path string) (report, error) {
	f, err := os.Open(path)
	if err != nil {
		return report{}, err
	}
	defer f.Close()
	var lines []line
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var ln line
		if err := json.Unmarshal(b, &ln); err != nil {
			return report{}, fmt.Errorf("%s: linha inválida: %w", path, err)
		}
		lines = append(lines, ln)
	}
	if err := sc.Err(); err != nil {
		return report{}, err
	}
	rep := report{File: path}
	// Group by invocation, and a line without a request identifier is its own invocation.
	groups := map[string][]int{}
	anon := 0
	for i, ln := range lines {
		key := ln.RequestID
		if key == "" {
			key = fmt.Sprintf("\x00anon%d", anon)
			anon++
		}
		groups[key] = append(groups[key], i)
		if ln.NodeID == "cloud-fallback" {
			rep.CloudLines++
		}
	}
	rep.Invokes = len(groups)
	rep.Attempts = len(lines)
	var deliveredLat []int64
	var edgeDelivered, totalDelivered int
	firstOK := 0
	firstKnown := 0
	for _, idxs := range groups {
		// The last line per attempt is the truth, which is the supersede.
		truth := map[int]line{}
		for _, idx := range idxs {
			ln := lines[idx]
			// An attempt number of zero, from old lines, from the cloud or from the absence of a node, counts as one attempt.
			n := ln.AttemptN
			if n <= 0 {
				n = 1
			}
			truth[n] = ln // JSONL é append-only: última ocorrência vence
		}
		if len(truth) >= 2 {
			rep.Hedged++
		}
		deliveredN := -1
		deliveredCount := 0
		for n, ln := range truth {
			if ln.Delivered {
				deliveredCount++
				deliveredN = n
			}
		}
		if deliveredCount != 1 {
			// An old log, without the flags, or an error stays out of the hedge
			// win and of the first choice but counts in the diagnosis.
			rep.BadDelivered++
			// The latency falls back to the last truth as the delivered one.
			if deliveredCount == 0 && len(truth) == 1 {
				for _, ln := range truth {
					deliveredLat = append(deliveredLat, ln.ObservedLatencyMS)
					totalDelivered++
					if ln.Layer == "borda" {
						edgeDelivered++
					}
				}
			}
			continue
		}
		if deliveredN != 1 {
			// The hedge win counts a delivered attempt other than the first, and
			// it only applies when there was a hedge, since with one attempt the
			// delivered number is always one.
			if len(truth) >= 2 {
				rep.HedgeWinRate++ // acumulador temporário (contagem)
			}
		}
		if first, ok := truth[1]; ok {
			firstKnown++
			if first.SloMet {
				firstOK++
			}
		}
		for n, ln := range truth {
			if n == deliveredN {
				deliveredLat = append(deliveredLat, ln.ObservedLatencyMS)
				totalDelivered++
				if ln.Layer == "borda" {
					edgeDelivered++
				}
			} else {
				rep.WastedWorkMS += ln.ObservedLatencyMS
			}
		}
	}
	if rep.Invokes > 0 {
		rep.HedgeRate = float64(rep.Hedged) / float64(rep.Invokes)
	}
	if rep.Hedged > 0 {
		rep.HedgeWinRate = rep.HedgeWinRate / float64(rep.Hedged)
	}
	if firstKnown > 0 {
		rep.FirstChoiceOKRate = float64(firstOK) / float64(firstKnown)
	}
	sort.Slice(deliveredLat, func(i, j int) bool { return deliveredLat[i] < deliveredLat[j] })
	rep.DeliveredP50MS = quantile(deliveredLat, 0.50)
	rep.DeliveredP95MS = quantile(deliveredLat, 0.95)
	rep.DeliveredP99MS = quantile(deliveredLat, 0.99)
	if totalDelivered > 0 {
		rep.EdgeShare = float64(edgeDelivered) / float64(totalDelivered)
	}
	return rep, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "uso: analyze-runs <arq1.jsonl> [arq2.jsonl ...]")
		os.Exit(2)
	}
	var reps []report
	for _, path := range os.Args[1:] {
		rep, err := analyze(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		reps = append(reps, rep)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if len(reps) == 1 {
		if err := enc.Encode(reps[0]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := enc.Encode(reps); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
