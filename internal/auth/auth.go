package auth

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	gcpauth "cloud.google.com/go/auth"
	gcpcredentials "cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/logging"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

type Method string

const (
	MethodToken  Method = "token"
	MethodADC    Method = "adc"
	MethodGcloud Method = "gcloud"
)

const (
	ModeAuto   = "auto"
	ModeADC    = "adc"
	ModeGcloud = "gcloud"
)

const probeTimeout = 10 * time.Second

type Options struct {
	Mode    string
	Token   string
	Verbose bool
	Logf    func(format string, args ...any)
}

func (o Options) logf(format string, args ...any) {
	if o.Verbose && o.Logf != nil {
		o.Logf(format, args...)
	}
}

func (o Options) normalizedMode() string {
	if o.Mode == "" {
		return ModeAuto
	}
	return o.Mode
}

type Result struct {
	Method      Method
	Credentials *gcpauth.Credentials
	TokenSource oauth2.TokenSource
}

func (r *Result) ClientOptions(quotaProject string) []option.ClientOption {
	if r.Credentials != nil {
		// Let ADC supply its own quota project (creds file or GOOGLE_CLOUD_QUOTA_PROJECT).
		return []option.ClientOption{option.WithAuthCredentials(r.Credentials)}
	}
	return []option.ClientOption{
		option.WithTokenSource(r.TokenSource),
		option.WithQuotaProject(quotaProject),
	}
}

func ValidateMode(mode string) error {
	switch mode {
	case "", ModeAuto, ModeADC, ModeGcloud:
		return nil
	}
	return fmt.Errorf("invalid --auth %q: valid values are 'auto', 'adc' or 'gcloud'", mode)
}

func methodLabel(m Method) string {
	switch m {
	case MethodADC:
		return "ADC"
	case MethodGcloud:
		return "gcloud CLI"
	default:
		return "token"
	}
}

type probeState int

const (
	stateAbsent probeState = iota
	stateMissing
	stateInvalid
	stateValid
	stateSkipped
)

type probe struct {
	state  probeState
	detail string
	err    error
	result *Result
}

func probeToken(opts Options) probe {
	if strings.TrimSpace(opts.Token) == "" {
		return probe{state: stateAbsent, detail: "Not provided"}
	}
	return probe{
		state:  stateValid,
		detail: "Provided (not validated)",
		result: &Result{
			Method: MethodToken,
			TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: opts.Token,
				TokenType:   "Bearer",
			}),
		},
	}
}

func probeADC(ctx context.Context) probe {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	creds, err := gcpcredentials.DetectDefault(&gcpcredentials.DetectOptions{
		Scopes: []string{logging.AdminScope},
	})
	if err != nil {
		return probe{state: stateMissing, detail: "Missing (" + shortErr(err) + ")", err: err}
	}
	if _, err := creds.Token(ctx); err != nil {
		return probe{state: stateInvalid, detail: "Expired (" + shortErr(err) + ")", err: err}
	}
	return probe{
		state:  stateValid,
		detail: "Valid (source: " + adcSource() + ")",
		result: &Result{Method: MethodADC, Credentials: creds},
	}
}

func probeGcloud(ctx context.Context) probe {
	if !gcloudAvailable() {
		return probe{state: stateMissing, detail: "Not found in PATH"}
	}
	ts := newGcloudTokenSource(ctx)
	if _, err := ts.Token(); err != nil {
		return probe{state: stateInvalid, detail: "Logged out or expired (" + shortErr(err) + ")", err: err}
	}
	return probe{
		state:  stateValid,
		detail: "Valid",
		result: &Result{Method: MethodGcloud, TokenSource: ts},
	}
}

func probeMethod(ctx context.Context, m Method) probe {
	switch m {
	case MethodADC:
		return probeADC(ctx)
	case MethodGcloud:
		return probeGcloud(ctx)
	default:
		return probe{state: stateMissing, detail: "Unknown method", err: errors.New("unknown auth method")}
	}
}

func Resolve(ctx context.Context, opts Options) (*Result, error) {
	if p := probeToken(opts); p.state == stateValid {
		opts.logf("auth: using %s", methodLabel(MethodToken))
		return p.result, nil
	}

	mode := opts.normalizedMode()
	gcloudFound := gcloudAvailable()

	switch mode {
	case ModeADC:
		p := probeADC(ctx)
		if p.state != stateValid {
			opts.logf("auth: %s unavailable: %v", methodLabel(MethodADC), p.err)
			return nil, resolutionError("ADC credentials missing or expired.", mode, gcloudFound)
		}
		opts.logf("auth: using %s", methodLabel(MethodADC))
		return p.result, nil
	case ModeGcloud:
		if !gcloudFound {
			opts.logf("auth: %s not found in PATH", methodLabel(MethodGcloud))
			return nil, resolutionError("`gcloud` CLI not found in PATH.", mode, false)
		}
		p := probeGcloud(ctx)
		if p.state != stateValid {
			opts.logf("auth: %s token retrieval failed: %v", methodLabel(MethodGcloud), p.err)
			return nil, resolutionError("failed to retrieve token from gcloud.", mode, true)
		}
		opts.logf("auth: using %s", methodLabel(MethodGcloud))
		return p.result, nil
	}

	for _, m := range candidateOrder(gcloudFound, adcFileExists(), os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "") {
		p := probeMethod(ctx, m)
		if p.state == stateValid {
			opts.logf("auth: using %s", methodLabel(p.result.Method))
			return p.result, nil
		}
		opts.logf("auth: %s unavailable: %v", methodLabel(m), p.err)
	}

	headline := "no valid Google Cloud credentials found."
	if !gcloudFound {
		headline = "no Google Cloud credentials found."
	}
	return nil, resolutionError(headline, mode, gcloudFound)
}

func resolutionError(headline, mode string, gcloudFound bool) error {
	return errors.New(headline + "\n" + remediation(mode, gcloudFound))
}

func UnauthenticatedError(method Method, firstPage bool, cause error) error {
	switch method {
	case MethodToken:
		return errors.New("provided access token is invalid or expired.\nVerify the token passed to --token / GRAPPLE_TOKEN.")
	case MethodGcloud:
		if localCredentialFailure(cause) {
			return errors.New("failed to obtain gcloud credentials.\nRun `gcloud auth login` and try again.")
		}
		if firstPage {
			return errors.New("authentication rejected by Cloud Logging with gcloud credentials.\nRun `gcloud auth login`; if it persists, your organization may enforce Certificate-Based Access (try `--auth=adc`).")
		}
		return errors.New("authentication failed for gcloud credentials.\nRun `gcloud auth login` and try again.")
	default:
		return errors.New("authentication failed.\nRun `gcloud auth login --update-adc` (or `gcloud auth application-default login`) and try again.")
	}
}

// localCredentialFailure reports whether err came from the token source rather than the server,
// which gRPC wraps as Unauthenticated ("transport: per-RPC creds failed ...").
func localCredentialFailure(err error) bool {
	return err != nil && strings.Contains(err.Error(), "per-RPC creds failed")
}

func PermissionDeniedError(project string) error {
	return fmt.Errorf("permission denied for project %q.\nVerify your account has the `Logs Viewer` role (roles/logging.viewer).", project)
}

func candidateOrder(gcloudFound, adcExists, gacSet bool) []Method {
	if !gcloudFound {
		return []Method{MethodADC}
	}
	if gacSet || adcExists {
		return []Method{MethodADC, MethodGcloud}
	}
	return []Method{MethodGcloud, MethodADC}
}

func adcFileExists() bool {
	p := adcFilePath()
	if p == "" {
		return false
	}
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// Mirrors the well-known ADC path used by cloud.google.com/go/auth, which is $HOME/.config/gcloud on every
// non-Windows OS; os.UserConfigDir resolves to ~/Library/Application Support on macOS and would miss the file.
func adcFilePath() string {
	if runtime.GOOS == "windows" {
		appData := os.Getenv("APPDATA")
		if appData == "" {
			return ""
		}
		return filepath.Join(appData, "gcloud", "application_default_credentials.json")
	}
	home := os.Getenv("HOME")
	if home == "" {
		if u, err := user.Current(); err == nil {
			home = u.HomeDir
		}
	}
	if home == "" {
		return ""
	}
	return filepath.Join(home, ".config", "gcloud", "application_default_credentials.json")
}

func adcSource() string {
	if p := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); p != "" {
		return "GOOGLE_APPLICATION_CREDENTIALS=" + p
	}
	if adcFileExists() {
		return shortenHome(adcFilePath())
	}
	return "metadata server"
}

func shortenHome(p string) string {
	home, err := os.UserHomeDir()
	if err == nil && home != "" && strings.HasPrefix(p, home) {
		return "~" + strings.TrimPrefix(p, home)
	}
	return p
}

func gcloudAvailable() bool {
	_, err := exec.LookPath(gcloudBin)
	return err == nil
}

func shortErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "invalid_rapt"), strings.Contains(lower, "reauth"):
		return "invalid_rapt"
	case strings.Contains(lower, "invalid_grant"):
		return "invalid_grant"
	}
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	const max = 120
	if len(msg) > max {
		msg = msg[:max] + "..."
	}
	return msg
}
