package auth

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	gcpauth "cloud.google.com/go/auth"
	"golang.org/x/oauth2"
)

func TestValidateMode(t *testing.T) {
	for _, mode := range []string{"", ModeAuto, ModeADC, ModeGcloud} {
		if err := ValidateMode(mode); err != nil {
			t.Errorf("ValidateMode(%q) unexpected error: %v", mode, err)
		}
	}
	if err := ValidateMode("bogus"); err == nil {
		t.Error("ValidateMode(\"bogus\") expected error, got nil")
	}
}

func TestCandidateOrder(t *testing.T) {
	cases := []struct {
		name        string
		gcloud, adc bool
		gacSet      bool
		want        []Method
	}{
		{"gac set prefers adc", true, false, true, []Method{MethodADC, MethodGcloud}},
		{"adc file prefers adc", true, true, false, []Method{MethodADC, MethodGcloud}},
		{"only gcloud prefers gcloud", true, false, false, []Method{MethodGcloud, MethodADC}},
		{"no gcloud returns adc only", false, false, false, []Method{MethodADC}},
		{"no gcloud with adc returns adc only", false, true, false, []Method{MethodADC}},
	}
	for _, c := range cases {
		if got := candidateOrder(c.gcloud, c.adc, c.gacSet); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: candidateOrder() = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestAdcSource(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/creds.json")
	if got := adcSource(); got != "GOOGLE_APPLICATION_CREDENTIALS=/tmp/creds.json" {
		t.Errorf("adcSource() = %q", got)
	}

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("HOME", t.TempDir())
	if got := adcSource(); got != "metadata server" {
		t.Errorf("adcSource() = %q, want metadata server", got)
	}
}

func TestShortErr(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"oauth2: \"invalid_grant\" \"reauth related error (invalid_rapt)\"", "invalid_rapt"},
		{"oauth2: cannot fetch token: invalid_grant", "invalid_grant"},
		{"line one\nline two", "line one"},
	}
	for _, c := range cases {
		if got := shortErr(errString(c.input)); got != c.want {
			t.Errorf("shortErr(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestResolveTokenOverride(t *testing.T) {
	res, err := Resolve(context.Background(), Options{Mode: ModeGcloud, Token: "ya29.fake"})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if res.Method != MethodToken {
		t.Errorf("Method = %q, want %q", res.Method, MethodToken)
	}
	if res.TokenSource == nil {
		t.Error("TokenSource is nil")
	}
}

func TestResolveForcedGcloud(t *testing.T) {
	fakeGcloud(t, "ya29.fake-token")
	res, err := Resolve(context.Background(), Options{Mode: ModeGcloud})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if res.Method != MethodGcloud {
		t.Errorf("Method = %q, want %q", res.Method, MethodGcloud)
	}
}

func TestResolveForcedGcloudMissing(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	_, err := Resolve(context.Background(), Options{Mode: ModeGcloud})
	if err == nil || !strings.Contains(err.Error(), "not found in PATH") {
		t.Fatalf("expected missing-gcloud error, got %v", err)
	}
}

func TestResolveAutoPrefersGcloudWithoutADC(t *testing.T) {
	fakeGcloud(t, "ya29.fake-token")
	res, err := Resolve(context.Background(), Options{Mode: ModeAuto})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if res.Method != MethodGcloud {
		t.Errorf("Method = %q, want %q", res.Method, MethodGcloud)
	}
}

func TestGcloudTokenSourceEmptyOutput(t *testing.T) {
	fakeGcloud(t, "")
	ts := newGcloudTokenSource(context.Background())
	if _, err := ts.Token(); err == nil {
		t.Fatal("expected error for empty token, got nil")
	}
}

func TestGcloudTokenSourceFailureSurfacesStderr(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "gcloud")
	writeExecutable(t, script, "#!/bin/sh\necho \"please run gcloud auth login\" >&2\nexit 1\n")
	t.Setenv("PATH", dir)

	ts := newGcloudTokenSource(context.Background())
	_, err := ts.Token()
	if err == nil || !strings.Contains(err.Error(), "please run gcloud auth login") {
		t.Fatalf("expected stderr to surface, got %v", err)
	}
}

func TestGcloudTokenSourceCaches(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "calls")
	script := filepath.Join(dir, "gcloud")
	writeExecutable(t, script, "#!/bin/sh\necho x >> "+marker+"\necho ya29.fake-token\n")
	t.Setenv("PATH", dir)

	ts := newGcloudTokenSource(context.Background())
	for i := 0; i < 3; i++ {
		if _, err := ts.Token(); err != nil {
			t.Fatalf("Token() error: %v", err)
		}
	}

	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("reading marker: %v", err)
	}
	if calls := strings.Count(string(data), "x"); calls != 1 {
		t.Errorf("gcloud invoked %d times, want 1 (cached)", calls)
	}
}

func TestResolveAutoFallsBackFromBrokenADC(t *testing.T) {
	fakeGcloud(t, "ya29.fake-token")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "does-not-exist.json"))

	res, err := Resolve(context.Background(), Options{Mode: ModeAuto})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if res.Method != MethodGcloud {
		t.Errorf("Method = %q, want %q", res.Method, MethodGcloud)
	}
}

func TestInspectFallback(t *testing.T) {
	fakeGcloud(t, "ya29.fake-token")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "does-not-exist.json"))

	st := Inspect(context.Background(), Options{Mode: ModeAuto})
	if st.Active != MethodGcloud {
		t.Errorf("Active = %q, want %q", st.Active, MethodGcloud)
	}
	if !st.fallback {
		t.Error("Fallback = false, want true")
	}
	if st.remediation != "" {
		t.Errorf("unexpected remediation: %q", st.remediation)
	}
}

func TestUnauthenticatedError(t *testing.T) {
	if got := UnauthenticatedError(MethodToken, true, nil).Error(); !strings.Contains(got, "provided access token") {
		t.Errorf("token error = %q", got)
	}
	serverErr := errString("rpc error: code = Unauthenticated desc = request had invalid authentication credentials")
	if got := UnauthenticatedError(MethodGcloud, true, serverErr).Error(); !strings.Contains(got, "Certificate-Based Access") {
		t.Errorf("gcloud first-page error = %q", got)
	}
	localErr := errString("rpc error: code = Unauthenticated desc = transport: per-RPC creds failed due to error: boom")
	if got := UnauthenticatedError(MethodGcloud, true, localErr).Error(); !strings.Contains(got, "Run `gcloud auth login`") || strings.Contains(got, "Certificate-Based Access") {
		t.Errorf("gcloud local-failure error = %q", got)
	}
	if got := UnauthenticatedError(MethodADC, true, nil).Error(); !strings.Contains(got, "authentication failed") {
		t.Errorf("adc error = %q", got)
	}
	if got := UnauthenticatedError(MethodGcloud, false, nil).Error(); !strings.Contains(got, "authentication failed") {
		t.Errorf("gcloud later-page error = %q", got)
	}
}

func TestPermissionDeniedError(t *testing.T) {
	if got := PermissionDeniedError("proj").Error(); !strings.Contains(got, "roles/logging.viewer") || !strings.Contains(got, "proj") {
		t.Errorf("permission error = %q", got)
	}
}

func TestStatusRender(t *testing.T) {
	st := &Status{
		mode: ModeAuto,
		entries: []statusEntry{
			{"ADC", stateValid, "Valid"},
			{"gcloud CLI", stateInvalid, "Expired"},
		},
		Active:   MethodGcloud,
		fallback: true,
	}
	out := st.Render()
	for _, want := range []string{"AUTH METHOD", "[✓]", "[✗]", "Active resolution (--auth=auto): gcloud CLI (falling back)"} {
		if !strings.Contains(out, want) {
			t.Errorf("Render() missing %q in:\n%s", want, out)
		}
	}

	failed := &Status{mode: ModeADC, entries: []statusEntry{{"ADC", stateMissing, "Missing"}}, remediation: "do a thing"}
	out = failed.Render()
	if !strings.Contains(out, "FAILED") || !strings.Contains(out, "Remediation: do a thing") {
		t.Errorf("failed Render() = %q", out)
	}
}

func TestClientOptions(t *testing.T) {
	adc := (&Result{Method: MethodADC, Credentials: &gcpauth.Credentials{}}).ClientOptions("proj")
	if len(adc) != 1 {
		t.Errorf("ADC ClientOptions len = %d, want 1 (no forced quota project)", len(adc))
	}
	tok := (&Result{Method: MethodGcloud, TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "x"})}).ClientOptions("proj")
	if len(tok) != 2 {
		t.Errorf("token ClientOptions len = %d, want 2 (token source + quota project)", len(tok))
	}
}

func TestLastNonEmptyLine(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"  \n\n", ""},
		{"ya29.tok\n", "ya29.tok"},
		{"WARNING: updating\n\nya29.tok\n", "ya29.tok"},
		{"a\nb", "b"},
	}
	for _, c := range cases {
		if got := lastNonEmptyLine(c.in); got != c.want {
			t.Errorf("lastNonEmptyLine(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestInspectTokenSkipsProbes(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "called")
	writeExecutable(t, filepath.Join(dir, "gcloud"), "#!/bin/sh\ntouch "+marker+"\necho ya29.fake\n")
	t.Setenv("PATH", dir)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(dir, "missing.json"))

	st := Inspect(context.Background(), Options{Mode: ModeAuto, Token: "ya29.fake"})
	if st.Active != MethodToken {
		t.Errorf("Active = %q, want %q", st.Active, MethodToken)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Error("gcloud was invoked even though --token was provided")
	}
}

func fakeGcloud(t *testing.T, token string) {
	t.Helper()
	dir := t.TempDir()
	script := filepath.Join(dir, "gcloud")
	body := "#!/bin/sh\necho " + token + "\n"
	writeExecutable(t, script, body)
	t.Setenv("PATH", dir)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CONFIG_HOME", "")
}

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fake gcloud is not portable to windows")
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("writing fake gcloud: %v", err)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
