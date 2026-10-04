package auth

import (
	"context"
	"fmt"
	"os"
	"strings"
)

type statusEntry struct {
	label  string
	state  probeState
	detail string
}

type Status struct {
	mode        string
	entries     []statusEntry
	Active      Method
	fallback    bool
	remediation string
}

func Inspect(ctx context.Context, opts Options) *Status {
	token := probeToken(opts)
	if token.state == stateValid {
		return &Status{
			mode: opts.normalizedMode(),
			entries: []statusEntry{
				{"Explicit token (--token)", token.state, token.detail},
				{methodLabel(MethodADC), stateSkipped, "Skipped (--token overrides)"},
				{methodLabel(MethodGcloud), stateSkipped, "Skipped (--token overrides)"},
			},
			Active: MethodToken,
		}
	}

	adc := probeADC(ctx)
	gcloud := probeGcloud(ctx)
	if gcloud.state == stateValid {
		if account := gcloudAccount(ctx); account != "" && account != "(unset)" {
			gcloud.detail = "Valid (account: " + account + ")"
		}
	}

	if adc.err != nil {
		opts.logf("auth: %s probe failed: %v", methodLabel(MethodADC), adc.err)
	}
	if gcloud.err != nil {
		opts.logf("auth: %s probe failed: %v", methodLabel(MethodGcloud), gcloud.err)
	}

	st := &Status{
		mode: opts.normalizedMode(),
		entries: []statusEntry{
			{"Explicit token (--token)", token.state, token.detail},
			{methodLabel(MethodADC), adc.state, adc.detail},
			{methodLabel(MethodGcloud), gcloud.state, gcloud.detail},
		},
	}

	gcloudFound := gcloud.state != stateMissing
	switch st.mode {
	case ModeADC:
		if adc.state == stateValid {
			st.Active = MethodADC
		}
	case ModeGcloud:
		if gcloud.state == stateValid {
			st.Active = MethodGcloud
		}
	default:
		order := candidateOrder(gcloudFound, adcFileExists(), os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "")
		for i, m := range order {
			valid := (m == MethodADC && adc.state == stateValid) || (m == MethodGcloud && gcloud.state == stateValid)
			if valid {
				st.Active = m
				st.fallback = i > 0
				break
			}
		}
	}

	if st.Active == "" {
		st.remediation = remediation(st.mode, gcloudFound)
	}
	return st
}

func (s *Status) Render() string {
	labelWidth := len("AUTH METHOD")
	for _, e := range s.entries {
		if len(e.label) > labelWidth {
			labelWidth = len(e.label)
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%-*s  %-7s  %s\n", labelWidth, "AUTH METHOD", "STATUS", "DETAILS")
	for _, e := range s.entries {
		fmt.Fprintf(&b, "%-*s  %-7s  %s\n", labelWidth, e.label, glyph(e.state), e.detail)
	}

	if s.Active != "" {
		fmt.Fprintf(&b, "\nActive resolution (--auth=%s): %s", s.mode, methodLabel(s.Active))
		if s.fallback {
			b.WriteString(" (falling back)")
		}
		b.WriteString("\n")
		return b.String()
	}

	fmt.Fprintf(&b, "\nActive resolution (--auth=%s): FAILED\n", s.mode)
	if s.remediation != "" {
		fmt.Fprintf(&b, "Remediation: %s\n", s.remediation)
	}
	return b.String()
}

func glyph(state probeState) string {
	switch state {
	case stateValid:
		return "[✓]"
	case stateAbsent, stateSkipped:
		return "[-]"
	default:
		return "[✗]"
	}
}

func remediation(mode string, gcloudFound bool) string {
	switch mode {
	case ModeADC:
		return "Run `gcloud auth application-default login` or switch to `--auth=auto`."
	case ModeGcloud:
		if !gcloudFound {
			return "Install the Google Cloud SDK or use `--auth=adc`."
		}
		return "Run `gcloud auth login` or switch to `--auth=auto`."
	default:
		if !gcloudFound {
			return "Install gcloud and run `gcloud auth application-default login`, or set GOOGLE_APPLICATION_CREDENTIALS."
		}
		return "Run `gcloud auth login --update-adc` (or `gcloud auth application-default login`) to authenticate."
	}
}
