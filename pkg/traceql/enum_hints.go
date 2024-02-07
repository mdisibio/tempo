package traceql

import "time"

const HintSample = "sample"

type Hint struct {
	Name  string
	Value Static
}

func newHint(k string, v Static) *Hint {
	return &Hint{k, v}
}

type Hints struct {
	Hints []*Hint
}

func newHints(h []*Hint) *Hints {
	return &Hints{h}
}

func (h *Hints) GetFloat(k string) (bool, float64) {
	if h == nil {
		return false, 0
	}

	for _, hh := range h.Hints {
		if hh.Name == k && hh.Value.Type == TypeFloat {
			return true, hh.Value.F
		}
	}

	return false, 0
}

func (h *Hints) GetInt(k string) (bool, int) {
	if h == nil {
		return false, 0
	}

	for _, hh := range h.Hints {
		if hh.Name == k && hh.Value.Type == TypeInt {
			return true, hh.Value.N
		}
	}

	return false, 0
}

func (h *Hints) GetDuration(k string) (bool, time.Duration) {
	if h == nil {
		return false, 0
	}

	for _, hh := range h.Hints {
		if hh.Name == k && hh.Value.Type == TypeDuration {
			return true, hh.Value.D
		}
	}

	return false, 0
}

func (h *Hints) GetBool(k string) (ok bool, v bool) {
	if h == nil {
		return false, false
	}

	for _, hh := range h.Hints {
		if hh.Name == k && hh.Value.Type == TypeBoolean {
			return true, hh.Value.B
		}
	}

	return false, false
}
