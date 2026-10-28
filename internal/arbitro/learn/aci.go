// Online adaptive conformal calibration (Gibbs and Candes, 2021).
//
// One parameter per model: alpha_t starts at risk and is updated after each
// observation. It targets long-run coverage close to 1 - risk over the
// observations actually seen, without requiring a correct model. Selective
// feedback (only the chosen node is observed) and the fact that coverage is
// not sharpness remain caveats.

package learn

// ACI holds the adaptive miscoverage level.
type ACI struct {
	alpha float64
	set   bool
}

// Alpha returns alpha_t, initializing it to risk on first use.
func (a *ACI) Alpha(risk float64) float64 {
	if !a.set {
		return risk
	}
	return a.alpha
}

// Update applies alpha_{t+1} = clamp(alpha_t + gamma*(risk - err), 0.001, 0.5),
// where err is 1 if the latency exceeded the bound and 0 otherwise.
func (a *ACI) Update(miss bool, risk, gamma float64) {
	al := a.Alpha(risk)
	err := 0.0
	if miss {
		err = 1.0
	}
	al += gamma * (risk - err)
	if al < 0.001 {
		al = 0.001
	}
	if al > 0.5 {
		al = 0.5
	}
	a.alpha = al
	a.set = true
}
