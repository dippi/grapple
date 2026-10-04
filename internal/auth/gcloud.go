package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

const (
	gcloudBin     = "gcloud"
	gcloudTimeout = 10 * time.Second

	// gcloud refreshes its cached access token when it has less than 5 minutes of life left,
	// so caching one for 5 minutes can never outlive the token gcloud handed us.
	gcloudTokenTTL = 5 * time.Minute
)

type gcloudTokenSource struct {
	ctx context.Context
	mu  sync.Mutex
	tok *oauth2.Token
}

func newGcloudTokenSource(ctx context.Context) *gcloudTokenSource {
	return &gcloudTokenSource{ctx: ctx}
}

func (g *gcloudTokenSource) Token() (*oauth2.Token, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tok != nil && g.tok.Valid() {
		return g.tok, nil
	}

	ctx, cancel := context.WithTimeout(g.ctx, gcloudTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, gcloudBin, "auth", "print-access-token", "--quiet")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			return nil, fmt.Errorf("gcloud timed out: %w", ctx.Err())
		case ctx.Err() != nil:
			return nil, ctx.Err()
		}
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, errors.New(msg)
		}
		return nil, err
	}

	token := lastNonEmptyLine(stdout.String())
	if token == "" {
		return nil, errors.New("gcloud returned an empty token")
	}

	g.tok = &oauth2.Token{
		AccessToken: token,
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(gcloudTokenTTL),
	}
	return g.tok, nil
}

func lastNonEmptyLine(s string) string {
	last := ""
	for _, line := range strings.Split(s, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			last = line
		}
	}
	return last
}

func gcloudAccount(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, gcloudTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, gcloudBin, "config", "get-value", "account", "--quiet").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
